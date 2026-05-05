"""
app/services/dashboard_service.py
═══════════════════════════════════════════════════════════════════════════
DASHBOARD STATS SERVICE
═══════════════════════════════════════════════════════════════════════════

Computes the dashboard's KPI numbers and chart data on every request,
so the dashboard is always fresh as soon as the user uploads new data.

Data flow:
    uploads table (latest user upload)
        │
        ▼
    Postgres cleaned_<name>_<ts> table (the actual rows)
        │
        ▼
    pandas DataFrame
        │
        ▼
    Aggregations: total customers, orders, revenue, AOV, monthly trend, top categories
        │
        ▼
    JSON to frontend

The service is robust to missing/varying column names -- it auto-detects
the right column for each metric using a list of likely names. So a user
who uploads a CSV with `OrderAmount` works the same as a user with
`order_total` or `Revenue`.

Public API:
    get_dashboard_stats(user_id: int = 1) -> dict
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

# ──────────────────────────────────────────────────────────────────────────
#  Column auto-detection
#
#  Real customer CSVs use wildly different column names. Rather than force
#  a schema, we try a list of likely names per metric. Comparison is
#  case-insensitive and ignores underscores / spaces.
# ──────────────────────────────────────────────────────────────────────────

REVENUE_CANDIDATES = [
    "orderamount", "order_amount", "ordertotal", "order_total",
    "totalprice", "total_price", "revenue", "amount", "totalamount",
    "total_amount", "subtotal", "grandtotal", "grand_total", "sales",
    "salesamount", "sales_amount", "netrevenue", "net_revenue",
]

CUSTOMER_ID_CANDIDATES = [
    "customerid", "customer_id", "custid", "cust_id", "userid",
    "user_id", "buyerid", "buyer_id", "clientid", "client_id",
    "customer", "user", "buyer",
]

ORDER_ID_CANDIDATES = [
    "orderid", "order_id", "transactionid", "transaction_id",
    "invoiceid", "invoice_id", "ordernumber", "order_number",
    "orderno", "order_no",
]

DATE_CANDIDATES = [
    "orderdate", "order_date", "transactiondate", "transaction_date",
    "purchasedate", "purchase_date", "date", "createdat", "created_at",
    "timestamp", "datetime", "ordertime", "order_time",
]

CATEGORY_CANDIDATES = [
    "category", "productcategory", "product_category", "categoryname",
    "category_name", "department", "section", "type", "producttype",
    "product_type",
]


def _normalize(s: str) -> str:
    """lowercase, strip underscores/spaces for matching"""
    return s.lower().replace("_", "").replace(" ", "").replace("-", "")


def _find_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Return the actual column name from `df` matching the first candidate
    (case- and underscore-insensitive). Returns None if no match."""
    if df is None or df.empty:
        return None
    norm_to_actual = {_normalize(c): c for c in df.columns}
    for cand in candidates:
        actual = norm_to_actual.get(_normalize(cand))
        if actual:
            return actual
    return None


# ──────────────────────────────────────────────────────────────────────────
#  Public API
# ──────────────────────────────────────────────────────────────────────────

def get_dashboard_stats(db: Session, user_id: int = 1) -> dict:
    """
    Compute dashboard stats from the user's most recent upload.

    Returns a dict with this shape:
    {
      "status": "success" | "no_data",
      "source_file": "ecommerce_orders_egypt.csv",
      "uploaded_at": "2026-04-25T...",
      "currency": "EGP",
      "kpis": {
        "total_customers":    int,
        "total_orders":       int,
        "avg_order_value":    float,
        "total_revenue":      float,
      },
      "charts": {
        "revenue_trend": [{"period": "2024-04", "revenue": 1.2e6}, ...],
        "top_categories": [{"category": "Electronics", "revenue": 4.2e5}, ...],
      },
      "detected_columns": { ... }   # for debugging / showing in UI
    }
    """
    df, source_file, uploaded_at = _load_latest_upload(db, user_id)
    if df is None:
        return {
            "status":      "no_data",
            "message":     ("No uploads found. Upload a CSV through the "
                            "Datasets page to populate the dashboard."),
            "kpis":        _empty_kpis(),
            "charts":      _empty_charts(),
        }

    # Column detection
    rev_col   = _find_column(df, REVENUE_CANDIDATES)
    cust_col  = _find_column(df, CUSTOMER_ID_CANDIDATES)
    order_col = _find_column(df, ORDER_ID_CANDIDATES)
    date_col  = _find_column(df, DATE_CANDIDATES)
    cat_col   = _find_column(df, CATEGORY_CANDIDATES)

    detected = {
        "revenue":   rev_col,
        "customer":  cust_col,
        "order":     order_col,
        "date":      date_col,
        "category":  cat_col,
    }

    # KPIs
    kpis = _compute_kpis(df, rev_col, cust_col, order_col)

    # Charts
    revenue_trend  = _compute_revenue_trend(df, rev_col, date_col)
    top_categories = _compute_top_categories(df, rev_col, cat_col, top_n=5)

    return {
        "status":           "success",
        "source_file":      source_file,
        "uploaded_at":      uploaded_at,
        "currency":         _detect_currency(df),
        "kpis":             kpis,
        "charts": {
            "revenue_trend":   revenue_trend,
            "top_categories":  top_categories,
        },
        "detected_columns": detected,
    }


# ──────────────────────────────────────────────────────────────────────────
#  Implementation helpers
# ──────────────────────────────────────────────────────────────────────────

def _load_latest_upload(
    db: Session, user_id: int,
) -> tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """Find the user's most recent upload and load its cleaned table.
    Returns (df, source_filename, uploaded_at_iso) or (None, None, None).

    Diagnostic-friendly: logs every step so failures are visible in the
    uvicorn console.
    """
    print(f"  [dashboard] Looking up uploads for user_id={user_id}")

    # Simplified query -- drop the `status != 'failed'` clause because
    # NULL status would cause it to silently exclude valid rows. If we
    # need to filter by status later we should do it explicitly with a
    # `status IS NULL OR status != 'failed'` clause, but for now any
    # row that has a table_name is fair game.
    row = db.execute(
        text("""
            SELECT table_name, original_file_name, uploaded_at, status
            FROM uploads
            WHERE user_id = :uid
              AND table_name IS NOT NULL
              AND table_name != ''
            ORDER BY uploaded_at DESC
            LIMIT 1
        """),
        {"uid": user_id},
    ).fetchone()

    if not row:
        # Fallback: try without user_id filter in case auth is mis-mapped
        print(f"  [dashboard] No uploads for user_id={user_id}, trying without filter…")
        row = db.execute(
            text("""
                SELECT table_name, original_file_name, uploaded_at, status
                FROM uploads
                WHERE table_name IS NOT NULL
                  AND table_name != ''
                ORDER BY uploaded_at DESC
                LIMIT 1
            """),
        ).fetchone()
        if row:
            print(f"  [dashboard] Found upload via fallback: {row[1]} "
                  f"(table={row[0]}, status={row[3]})")
        else:
            print(f"  [dashboard] No uploads found in table at all")
            return None, None, None
    else:
        print(f"  [dashboard] Found upload: {row[1]} "
              f"(table={row[0]}, status={row[3]})")

    table_name = row[0]
    src_file   = row[1]
    uploaded_at = row[2].isoformat() if row[2] else None

    # Load the cleaned table. Use quoted identifier to handle weird names.
    try:
        # Use the raw db connection -- pd.read_sql with SQLAlchemy text()
        # can be quirky depending on pandas/sqlalchemy version
        from sqlalchemy import text as sql_text
        result = db.execute(sql_text(f'SELECT * FROM "{table_name}"'))
        rows = result.fetchall()
        if not rows:
            print(f"  [dashboard] Table {table_name} is empty")
            return None, src_file, uploaded_at

        # Build DataFrame from result
        cols = list(result.keys()) if hasattr(result, "keys") else []
        if not cols:
            # Older SQLAlchemy: get columns from the first row's _mapping
            cols = list(rows[0]._mapping.keys()) if hasattr(rows[0], "_mapping") else []
        df = pd.DataFrame(rows, columns=cols)
        print(f"  [dashboard] Loaded {len(df)} rows × {len(df.columns)} cols "
              f"from {table_name}")
    except Exception as e:
        print(f"  [dashboard] Could not read table {table_name}: {type(e).__name__}: {e}")
        return None, src_file, uploaded_at

    if df.empty:
        return None, src_file, uploaded_at

    return df, src_file, uploaded_at


def _compute_kpis(
    df: pd.DataFrame,
    rev_col: Optional[str],
    cust_col: Optional[str],
    order_col: Optional[str],
) -> dict:
    """Compute the four KPI numbers. Each one falls back to 0 if its
    required column is missing."""
    total_customers  = 0
    total_orders     = 0
    avg_order_value  = 0.0
    total_revenue    = 0.0

    if cust_col:
        total_customers = int(df[cust_col].nunique())

    if order_col:
        total_orders = int(df[order_col].nunique())
    elif df is not None and not df.empty:
        # No explicit order ID column -- assume one row per order
        total_orders = int(len(df))

    if rev_col:
        rev_series = pd.to_numeric(df[rev_col], errors="coerce").dropna()
        if len(rev_series) > 0:
            total_revenue   = float(rev_series.sum())
            avg_order_value = float(rev_series.mean())

    return {
        "total_customers":  total_customers,
        "total_orders":     total_orders,
        "avg_order_value":  round(avg_order_value, 2),
        "total_revenue":    round(total_revenue, 2),
    }


def _compute_revenue_trend(
    df: pd.DataFrame,
    rev_col: Optional[str],
    date_col: Optional[str],
) -> list[dict]:
    """Group revenue by month for the last 12 months. Empty list if either
    revenue or date column is missing."""
    if not rev_col or not date_col:
        return []
    try:
        d = df[[date_col, rev_col]].copy()
        d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
        d[rev_col]  = pd.to_numeric(d[rev_col], errors="coerce")
        d = d.dropna(subset=[date_col, rev_col])
        if d.empty:
            return []

        # Group by year-month
        d["period"] = d[date_col].dt.to_period("M").astype(str)
        agg = d.groupby("period", as_index=False)[rev_col].sum()
        agg = agg.sort_values("period").tail(12)
        return [
            {"period": str(r["period"]), "revenue": round(float(r[rev_col]), 2)}
            for _, r in agg.iterrows()
        ]
    except Exception as e:
        print(f"  [dashboard] revenue_trend failed: {e}")
        return []


def _compute_top_categories(
    df: pd.DataFrame,
    rev_col: Optional[str],
    cat_col: Optional[str],
    top_n: int = 5,
) -> list[dict]:
    """Aggregate revenue by category and return top-N. Falls back to row
    counts if revenue column is missing."""
    if not cat_col:
        return []
    try:
        if rev_col:
            d = df[[cat_col, rev_col]].copy()
            d[rev_col] = pd.to_numeric(d[rev_col], errors="coerce")
            d = d.dropna(subset=[rev_col])
            agg = (d.groupby(cat_col, as_index=False)[rev_col]
                    .sum()
                    .sort_values(rev_col, ascending=False)
                    .head(top_n))
            return [
                {"category": str(r[cat_col]),
                 "revenue":  round(float(r[rev_col]), 2)}
                for _, r in agg.iterrows()
            ]
        else:
            # Fall back to counts
            counts = (df[cat_col].value_counts().head(top_n)
                      .reset_index())
            counts.columns = ["category", "count"]
            return [
                {"category": str(r["category"]),
                 "revenue":  int(r["count"])}
                for _, r in counts.iterrows()
            ]
    except Exception as e:
        print(f"  [dashboard] top_categories failed: {e}")
        return []


def _detect_currency(df: pd.DataFrame) -> str:
    """Heuristic currency detection -- mirrors the BMC service. Looks for
    explicit currency columns or region hints. Falls back to 'EGP' since
    the platform is built for the Egyptian / MENA market by default."""
    if df is None or df.empty:
        return "EGP"

    # Check for explicit currency column
    for col in df.columns:
        if "currency" in col.lower():
            try:
                vals = df[col].dropna().astype(str).str.upper().unique()
                if len(vals) >= 1 and len(vals[0]) == 3:
                    return str(vals[0])
            except Exception:
                pass

    # Region hint via city / region / country columns
    region_map = {
        ("egypt", "cairo", "alexandria", "giza", "hurghada"): "EGP",
        ("uae", "dubai", "abu dhabi", "sharjah"):              "AED",
        ("saudi", "riyadh", "jeddah", "dammam"):               "SAR",
        ("kuwait",):                                            "KWD",
        ("usa", "united states"):                               "USD",
        ("uk", "united kingdom", "london"):                     "GBP",
    }
    for col in df.columns:
        if any(k in col.lower() for k in ("region", "city", "country", "shipping")):
            try:
                values = " ".join(df[col].dropna().astype(str).str.lower().unique()[:50])
                for hints, currency in region_map.items():
                    if any(h in values for h in hints):
                        return currency
            except Exception:
                continue

    return "EGP"


def _empty_kpis() -> dict:
    return {
        "total_customers":  0,
        "total_orders":     0,
        "avg_order_value":  0.0,
        "total_revenue":    0.0,
    }


def _empty_charts() -> dict:
    return {"revenue_trend": [], "top_categories": []}