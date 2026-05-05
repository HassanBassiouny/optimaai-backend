"""
app/services/aggregation_service.py

Takes an event-level (order-level) cleaned table and produces two
derived tables:

  - customer_* : one row per customer, with total_orders, total_spent,
                 avg_order_value, tenure, return_rate, etc.
                 (what the Churn model expects)

  - monthly_*  : one row per month, with monthly_revenue, lag_1_rev,
                 mom_growth, etc.
                 (what the Growth model expects)

The service:
  1. Uses column_mapping_service synonyms to detect the
     customer-id / date / amount / quantity / return columns in the
     user's cleaned table.
  2. If the required shape is present, builds the aggregation(s) and
     writes each to a new PostgreSQL table.
  3. Returns {"customer_table": name_or_None, "monthly_table": name_or_None,
              "diagnostics": {...}} — never raises, so the upload flow
     isn't blocked if aggregation isn't possible.
"""
import re
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.column_mapping_service import SYNONYMS, _normalize


# ══════════════════════════════════════════════════════
#  Column role detection  (uses existing synonyms)
# ══════════════════════════════════════════════════════

# Extra role keys that aren't in REQUIRED but are needed for aggregation
_ROLE_SYNONYMS = {
    "customer_id":   ["customer_id", "customerid", "cust_id", "user_id",
                      "subscriber_id", "client_id", "account_id"],
    "date":          ["order_date", "date", "purchase_date", "created_at",
                      "timestamp", "transaction_date", "invoice_date",
                      "signup_date"],
    "amount":        SYNONYMS["price"] + SYNONYMS["total_spent"]
                      + ["order_amount", "revenue", "sales", "total"],
    "quantity":      SYNONYMS["quantity_sold"],
    "category":      ["category", "product_category", "dept", "department"],
    "is_returned":   ["is_returned", "returned", "return_flag", "is_return"],
    "rating":        SYNONYMS["avg_rating_given"],
    "item_count":    SYNONYMS["total_items"],
}


def _detect_roles(columns: list) -> dict:
    """Map each role → the actual user column name (or None)."""
    user_norm = {_normalize(c): c for c in columns}
    roles = {}
    for role, syns in _ROLE_SYNONYMS.items():
        found = None
        # Exact match on normalized synonym
        for syn in syns:
            syn_norm = _normalize(syn)
            if syn_norm in user_norm:
                found = user_norm[syn_norm]
                break
        # Fuzzy substring fallback
        if not found:
            for syn in syns:
                syn_norm = _normalize(syn)
                for u_norm, u_orig in user_norm.items():
                    if syn_norm in u_norm or u_norm in syn_norm:
                        found = u_orig
                        break
                if found:
                    break
        roles[role] = found
    return roles


# ══════════════════════════════════════════════════════
#  Customer-level aggregation  (for churn)
# ══════════════════════════════════════════════════════

def _build_customer_aggregate(df: pd.DataFrame, roles: dict) -> Optional[pd.DataFrame]:
    """
    Needs customer_id at minimum. Amount/date/qty are used if available.
    Returns a DataFrame or None if not enough data.
    """
    cid = roles.get("customer_id")
    if not cid or cid not in df.columns:
        return None

    df = df.copy()

    # Parse date if present
    date_col = roles.get("date")
    has_date = date_col and date_col in df.columns
    if has_date:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Ensure numeric columns are numeric
    for role_key in ("amount", "quantity", "is_returned", "rating"):
        col = roles.get(role_key)
        if col and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    groups = df.groupby(cid)
    agg = pd.DataFrame(index=groups.size().index)

    # total_orders = row count per customer
    agg["total_orders"] = groups.size()

    # total_spent, avg_order_value, spend_per_day
    amount_col = roles.get("amount")
    if amount_col and amount_col in df.columns:
        agg["total_spent"]     = groups[amount_col].sum()
        agg["avg_order_value"] = groups[amount_col].mean()
    else:
        agg["total_spent"]     = 0.0
        agg["avg_order_value"] = 0.0

    # tenure & spend_per_day (need date)
    if has_date:
        first = groups[date_col].min()
        last  = groups[date_col].max()
        tenure_days = (last - first).dt.days.clip(lower=1)
        agg["customer_tenure_days"] = tenure_days
        agg["spend_per_day"] = agg["total_spent"] / tenure_days

        # avg_days_between_orders = tenure / (orders - 1), clipped for 1-order customers
        safe_orders = (agg["total_orders"] - 1).clip(lower=1)
        agg["avg_days_between_orders"] = tenure_days / safe_orders
    else:
        agg["customer_tenure_days"]     = 0
        agg["spend_per_day"]            = 0.0
        agg["avg_days_between_orders"]  = 0

    # total_items (qty sum)
    qty_col = roles.get("quantity")
    if qty_col and qty_col in df.columns:
        agg["total_items"] = groups[qty_col].sum()
    else:
        agg["total_items"] = agg["total_orders"]  # fallback: assume 1 item per order

    # return_rate (is_returned is 0/1 per row)
    ret_col = roles.get("is_returned")
    if ret_col and ret_col in df.columns:
        agg["return_rate"] = groups[ret_col].mean()
    else:
        agg["return_rate"] = 0.0

    # avg_rating_given
    rating_col = roles.get("rating")
    if rating_col and rating_col in df.columns:
        agg["avg_rating_given"] = groups[rating_col].mean()
    else:
        agg["avg_rating_given"] = 4.0

    # unique_categories
    cat_col = roles.get("category")
    if cat_col and cat_col in df.columns:
        agg["unique_categories"] = groups[cat_col].nunique()
    else:
        agg["unique_categories"] = 1

    # predicted_clv — simple proxy: total_spent × (expected_remaining_months / active_months)
    # Keep it simple: use total_spent × 2 as a naive "next-year" projection
    agg["predicted_clv"] = agg["total_spent"] * 2.0

    agg = agg.reset_index().rename(columns={cid: "customer_id"})
    agg = agg.fillna(0)
    return agg


# ══════════════════════════════════════════════════════
#  Monthly-level aggregation  (for growth)
# ══════════════════════════════════════════════════════

def _build_monthly_aggregate(df: pd.DataFrame, roles: dict) -> Optional[pd.DataFrame]:
    """
    Needs date + amount at minimum. Returns None otherwise.
    """
    date_col   = roles.get("date")
    amount_col = roles.get("amount")
    if not date_col or date_col not in df.columns:
        return None
    if not amount_col or amount_col not in df.columns:
        return None

    df = df.copy()
    df[date_col]   = pd.to_datetime(df[date_col], errors="coerce")
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
    df = df.dropna(subset=[date_col, amount_col])
    if df.empty:
        return None

    df["month_key"] = df[date_col].dt.to_period("M").dt.to_timestamp()
    monthly = (
        df.groupby("month_key")
          .agg(monthly_revenue=(amount_col, "sum"),
               monthly_orders=(amount_col, "count"))
          .sort_index()
    )

    # Lag & growth features
    monthly["lag_1_rev"]    = monthly["monthly_revenue"].shift(1)
    monthly["lag_2_rev"]    = monthly["monthly_revenue"].shift(2)
    monthly["lag_12_rev"]   = monthly["monthly_revenue"].shift(12)
    monthly["mom_growth"]   = (
        (monthly["monthly_revenue"] - monthly["lag_1_rev"]) / monthly["lag_1_rev"]
    )
    monthly["yoy_growth"]   = (
        (monthly["monthly_revenue"] - monthly["lag_12_rev"]) / monthly["lag_12_rev"]
    )
    monthly["3m_rolling_avg"] = monthly["monthly_revenue"].rolling(3).mean()

    monthly = monthly.reset_index()
    monthly["month_num"]   = monthly["month_key"].dt.month
    monthly["quarter_num"] = ((monthly["month_key"].dt.month - 1) // 3 + 1)
    monthly = monthly.fillna(0)
    return monthly


# ══════════════════════════════════════════════════════
#  Public entry point
# ══════════════════════════════════════════════════════

def _safe_table_suffix(s: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", s.lower()).strip("_")[:60]


def build_aggregates(engine: Engine, cleaned_table: str) -> dict:
    """
    Reads the cleaned table, detects roles, builds whatever aggregates
    are possible, and writes them to new tables.

    Returns diagnostics for logging — never raises.
    """
    out = {
        "customer_table": None,
        "monthly_table":  None,
        "diagnostics":    {},
    }

    try:
        df = pd.read_sql_table(cleaned_table, con=engine)
    except Exception as e:
        out["diagnostics"]["load_error"] = str(e)
        return out

    if df.empty:
        out["diagnostics"]["empty"] = True
        return out

    roles = _detect_roles(df.columns.tolist())
    out["diagnostics"]["detected_roles"] = roles

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = _safe_table_suffix(cleaned_table.replace("cleaned_", ""))

    # Customer-level
    try:
        cust = _build_customer_aggregate(df, roles)
        if cust is not None and not cust.empty:
            name = f"agg_customer_{base}_{ts}"[:60]
            cust.to_sql(name, con=engine, if_exists="replace",
                        index=False, chunksize=1000)
            out["customer_table"] = name
            out["diagnostics"]["customer_rows"] = len(cust)
    except Exception as e:
        out["diagnostics"]["customer_error"] = str(e)

    # Monthly-level
    try:
        monthly = _build_monthly_aggregate(df, roles)
        if monthly is not None and not monthly.empty:
            name = f"agg_monthly_{base}_{ts}"[:60]
            monthly.to_sql(name, con=engine, if_exists="replace",
                           index=False, chunksize=1000)
            out["monthly_table"] = name
            out["diagnostics"]["monthly_rows"] = len(monthly)
    except Exception as e:
        out["diagnostics"]["monthly_error"] = str(e)

    return out