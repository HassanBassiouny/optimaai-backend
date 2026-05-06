"""
app/services/odoo_extractor.py
═══════════════════════════════════════════════════════════════════════════
ODOO → BACKEND PIPELINE BRIDGE
═══════════════════════════════════════════════════════════════════════════

Reads transactional data out of Odoo via OdooConnector, transforms it into
a single tidy DataFrame keyed at the line-item level, and feeds it through
the existing DynamicPreprocessingPipeline so the result lands in the same
``uploads`` / ``cleaned_*`` infrastructure as a CSV upload.

Why the same pipeline?
    Once Odoo data is registered as an ``upload`` row, every downstream
    feature already in OptimaAi works for free:
      • column-mapping wizard
      • predictions (revenue / churn / growth)
      • KPI snapshots
      • RAG / knowledge base
      • BMC + reports

Output schema (column names match the synonyms table in
``app/services/column_mapping_service.py``, so the auto-mapper picks them up):

    customer_id, customer_name, country, industry,
    order_id, order_date, amount, amount_untaxed, quantity_sold,
    state, payment_state,
    salesperson_id, team_id,
    source            # "sale.order" | "account.move" | "crm.lead"
    expected_revenue, probability, stage   # leads only
"""
from __future__ import annotations

import os
import logging
import re
import tempfile
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text

from app.services.odoo_service import (
    OdooConnector, OdooConnectionError, get_connector,
)
from app.database import engine, SessionLocal, Upload

_logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════
#  Many2one helper
#  Odoo returns Many2one fields as either ``False`` or ``[id, "display name"]``.
#  These two helpers normalise that shape across the codebase.
# ══════════════════════════════════════════════════════

def _m2o_id(value: Any) -> Optional[int]:
    """Extract the id from a Many2one tuple. Returns None if unset."""
    if isinstance(value, (list, tuple)) and len(value) >= 1:
        return value[0]
    return None


def _m2o_name(value: Any) -> Optional[str]:
    """Extract the display name from a Many2one tuple. Returns None if unset."""
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return value[1]
    return None


# ══════════════════════════════════════════════════════
#  Transformers
#  Each ``_to_records_*`` function turns a list of raw Odoo dicts into a
#  list of flat dicts ready for pandas. Keeping them separate makes the
#  field mappings reviewable and unit-testable in isolation.
# ══════════════════════════════════════════════════════

def _to_records_sales(orders: list[dict]) -> list[dict]:
    """Normalise sale.order records to the unified schema."""
    rows = []
    for o in orders:
        rows.append({
            "source":           "sale.order",
            "order_id":         o.get("id"),
            "order_name":       o.get("name"),
            "order_date":       o.get("date_order"),
            "amount":           o.get("amount_total")   or 0.0,
            "amount_untaxed":   o.get("amount_untaxed") or 0.0,
            "amount_tax":       o.get("amount_tax")     or 0.0,
            "state":            o.get("state"),
            "payment_state":    None,
            "customer_id":      _m2o_id(o.get("partner_id")),
            "customer_name":    _m2o_name(o.get("partner_id")),
            "salesperson_id":   _m2o_id(o.get("user_id")),
            "salesperson_name": _m2o_name(o.get("user_id")),
            "team_id":          _m2o_id(o.get("team_id")),
            "team_name":        _m2o_name(o.get("team_id")),
            "expected_revenue": None,
            "probability":      None,
            "stage":            None,
            "create_date":      o.get("create_date"),
        })
    return rows


def _to_records_invoices(invoices: list[dict]) -> list[dict]:
    """Normalise account.move records (customer invoices)."""
    rows = []
    for i in invoices:
        rows.append({
            "source":           "account.move",
            "order_id":         i.get("id"),
            "order_name":       i.get("name"),
            "order_date":       i.get("invoice_date"),
            "amount":           i.get("amount_total")    or 0.0,
            "amount_untaxed":   i.get("amount_untaxed")  or 0.0,
            "amount_tax":       (i.get("amount_total") or 0.0) - (i.get("amount_untaxed") or 0.0),
            "state":            i.get("state"),
            "payment_state":    i.get("payment_state"),
            "customer_id":      _m2o_id(i.get("partner_id")),
            "customer_name":    _m2o_name(i.get("partner_id")),
            "salesperson_id":   None,
            "salesperson_name": None,
            "team_id":          None,
            "team_name":        None,
            "expected_revenue": None,
            "probability":      None,
            "stage":            None,
            "create_date":      i.get("create_date"),
        })
    return rows


def _to_records_leads(leads: list[dict]) -> list[dict]:
    """Normalise crm.lead records — used for pipeline coverage features."""
    rows = []
    for l in leads:
        rows.append({
            "source":           "crm.lead",
            "order_id":         l.get("id"),
            "order_name":       l.get("name"),
            "order_date":       l.get("date_open") or l.get("create_date"),
            "amount":           l.get("expected_revenue") or 0.0,
            "amount_untaxed":   l.get("expected_revenue") or 0.0,
            "amount_tax":       0.0,
            "state":            "lead" if l.get("type") == "lead" else "opportunity",
            "payment_state":    None,
            "customer_id":      _m2o_id(l.get("partner_id")),
            "customer_name":    _m2o_name(l.get("partner_id")),
            "salesperson_id":   _m2o_id(l.get("user_id")),
            "salesperson_name": _m2o_name(l.get("user_id")),
            "team_id":          _m2o_id(l.get("team_id")),
            "team_name":        _m2o_name(l.get("team_id")),
            "expected_revenue": l.get("expected_revenue") or 0.0,
            "probability":      l.get("probability") or 0.0,
            "stage":            _m2o_name(l.get("stage_id")),
            "create_date":      l.get("create_date"),
        })
    return rows


# ══════════════════════════════════════════════════════
#  Partner enrichment
#  We fetch partners separately and left-join to enrich every row with
#  country / industry. This avoids pulling these fields per-order via Odoo's
#  read-related which can be slow on large datasets.
# ══════════════════════════════════════════════════════

def _build_partner_lookup(partners: list[dict]) -> dict[int, dict]:
    """Map partner_id → {country, industry, is_company}."""
    return {
        p["id"]: {
            "country":    _m2o_name(p.get("country_id")),
            "industry":   _m2o_name(p.get("industry_id")),
            "is_company": p.get("is_company", False),
        }
        for p in partners
    }


# ══════════════════════════════════════════════════════
#  Main extraction
# ══════════════════════════════════════════════════════

def extract_to_dataframe(
    connector:  Optional[OdooConnector] = None,
    since:      Optional[datetime] = None,
    include:    Optional[list[str]] = None,
    sales_limit:    Optional[int] = None,
    invoice_limit:  Optional[int] = None,
    lead_limit:     Optional[int] = None,
) -> pd.DataFrame:
    """
    Build a unified DataFrame across the four Odoo models.

    Parameters
    ----------
    connector : OdooConnector, optional
        Reuse an existing connector. If None, creates one from env vars.
    since : datetime, optional
        Only fetch records modified on or after this datetime. ``None`` means
        full-history pull (use sparingly — incremental syncs should pass a
        timestamp from the last successful run).
    include : list of str, optional
        Subset of {"sales", "invoices", "leads"}. Defaults to all three.
    *_limit : int, optional
        Per-model row caps — useful in dev / smoke tests.
    """
    conn = connector or get_connector()
    include = include or ["sales", "invoices", "leads"]

    all_rows: list[dict] = []

    # 1. Sales orders ──────────────────────────────────
    if "sales" in include:
        orders = conn.fetch_sale_orders(since=since, limit=sales_limit)
        _logger.info("Odoo: fetched %d sale.order records", len(orders))
        all_rows.extend(_to_records_sales(orders))

    # 2. Customer invoices ─────────────────────────────
    if "invoices" in include:
        invoices = conn.fetch_invoices(since=since, limit=invoice_limit)
        _logger.info("Odoo: fetched %d account.move records", len(invoices))
        all_rows.extend(_to_records_invoices(invoices))

    # 3. CRM leads ─────────────────────────────────────
    if "leads" in include:
        leads = conn.fetch_leads(since=since, limit=lead_limit)
        _logger.info("Odoo: fetched %d crm.lead records", len(leads))
        all_rows.extend(_to_records_leads(leads))

    if not all_rows:
        _logger.warning("Odoo: extraction returned 0 records")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # 4. Enrich with partner attributes ────────────────
    customer_ids = [c for c in df["customer_id"].dropna().unique().tolist() if c]
    if customer_ids:
        # batch read partners we actually saw
        partners = conn.execute_kw(
            "res.partner", "read",
            [customer_ids, ["id", "country_id", "industry_id", "is_company"]],
        )
        lookup = _build_partner_lookup(partners)
        df["country"]    = df["customer_id"].map(lambda x: lookup.get(x, {}).get("country"))
        df["industry"]   = df["customer_id"].map(lambda x: lookup.get(x, {}).get("industry"))
        df["is_company"] = df["customer_id"].map(lambda x: lookup.get(x, {}).get("is_company"))
    else:
        df["country"] = None
        df["industry"] = None
        df["is_company"] = None

    # 5. Derived columns the column-mapper expects ─────
    # Synonyms table maps these → ML model features automatically.
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["month"]      = df["order_date"].dt.month
    df["quarter"]    = df["order_date"].dt.quarter
    df["year"]       = df["order_date"].dt.year

    # quantity_sold isn't on the order header — for parity with the
    # CSV-upload schema we synthesise a per-row qty of 1 so the revenue
    # model still works on aggregate amounts. Real line-item quantities
    # require a separate sale.order.line / account.move.line extraction
    # which we leave as a follow-up (Stage 5+ in Chapter 3).
    df["quantity_sold"] = 1
    df["price"]         = df["amount"]

    return df


# ══════════════════════════════════════════════════════
#  Persistence — register Odoo data as an Upload
#  Mirrors what datasets_routes.upload_dataset does for CSVs.
# ══════════════════════════════════════════════════════

def sync_to_uploads(
    user_id:        int,
    since:          Optional[datetime] = None,
    include:        Optional[list[str]] = None,
    run_pipeline:   bool = True,
    ingest_kb:      bool = True,
    limit:          Optional[int] = None,
) -> dict:
    """
    Run a full Odoo → backend sync.

    Steps
    -----
    1. Pull the unified DataFrame from Odoo.
    2. Write it to a temp CSV (the pipeline accepts file paths or bytes).
    3. Run DynamicPreprocessingPipeline → cleaned_* table + uploads row.
    4. Build customer / monthly aggregate tables (existing code path).
    5. (Optional) Ingest into the ChromaDB knowledge base.

    Returns the upload metadata dict the frontend Datasets page expects.
    """
    started_at = datetime.utcnow()

    # 1. Extract ───────────────────────────────────────
    df = extract_to_dataframe(
        since=since,
        include=include,
        sales_limit=limit,
        invoice_limit=limit,
        lead_limit=limit,
    )

    if df.empty:
        return {
            "status":       "empty",
            "rows":         0,
            "message":      "Odoo returned no records for the given filters.",
            "extracted_at": started_at.isoformat(),
        }

    # 2. Persist as a CSV the pipeline can ingest ───────
    ts = started_at.strftime("%Y%m%d_%H%M%S")
    fname = f"odoo_export_{ts}.csv"

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    upload_id:  Optional[int] = None
    table_name: Optional[str] = None
    quality_before = 0.0
    quality_after  = 0.0

    try:
        if run_pipeline:
            # 3. Run the existing preprocessing pipeline ─
            import sys
            ml_path = os.path.join(os.path.dirname(__file__), "..", "..", "ml")
            if ml_path not in sys.path:
                sys.path.insert(0, ml_path)

            from dynamic_preprocessing_pipeline import DynamicPreprocessingPipeline

            db_url = os.getenv(
                "DATABASE_URL",
                "postgresql://optimaai:optimaai123@localhost:5432/optimaai_db"
            )
            pipeline = DynamicPreprocessingPipeline()
            cleaned_df, report = pipeline.run(
                source  = tmp_path,
                db_url  = db_url,
                user_id = user_id,
            )
            quality_before = report.get("quality_score", {}).get("before", 0)
            quality_after  = report.get("quality_score", {}).get("after", 0)

            # 4. Look up the upload row the pipeline registered ─
            #    Pipeline registers under the temp filename — we patch the
            #    real one back in to match the CSV-upload behaviour.
            db = SessionLocal()
            try:
                tmp_basename = os.path.basename(tmp_path)
                row = db.execute(
                    text(
                        "SELECT upload_id, table_name FROM uploads "
                        "WHERE original_file_name = :fname "
                        "ORDER BY uploaded_at DESC LIMIT 1"
                    ),
                    {"fname": tmp_basename},
                ).fetchone()
                if row:
                    upload_id, table_name = row[0], row[1]
                    db.execute(
                        text(
                            "UPDATE uploads "
                            "SET original_file_name = :real, category = :cat "
                            "WHERE upload_id = :id"
                        ),
                        {"real": fname, "cat": "odoo", "id": upload_id},
                    )
                    db.commit()

                    # Build aggregates (same as CSV path)
                    try:
                        from app.services.aggregation_service import build_aggregates
                        agg = build_aggregates(engine, table_name)
                        db.execute(
                            text(
                                "UPDATE uploads SET "
                                "customer_table_name = :c, "
                                "monthly_table_name = :m "
                                "WHERE upload_id = :id"
                            ),
                            {
                                "c":  agg.get("customer_table"),
                                "m":  agg.get("monthly_table"),
                                "id": upload_id,
                            },
                        )
                        db.commit()
                    except Exception as e:
                        _logger.warning("Aggregate build failed: %s", e)
            finally:
                db.close()
        else:
            # Pipeline disabled — manual register so the Datasets page sees it.
            base  = re.sub(r"[^a-z0-9_]", "_", fname.lower()).strip("_")
            table_name = f"odoo_raw_{ts}"
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            db = SessionLocal()
            try:
                db.execute(
                    text(
                        "INSERT INTO uploads "
                        "(user_id, original_file_name, table_name, "
                        " rows_count, columns_count, status, category) "
                        "VALUES (:uid, :fname, :tn, :r, :c, 'completed', 'odoo') "
                        "RETURNING upload_id"
                    ),
                    {
                        "uid":   user_id,
                        "fname": fname,
                        "tn":    table_name,
                        "r":     len(df),
                        "c":     len(df.columns),
                    },
                )
                db.commit()
                row = db.execute(
                    text("SELECT upload_id FROM uploads "
                         "WHERE table_name = :tn"),
                    {"tn": table_name},
                ).fetchone()
                upload_id = row[0] if row else None
            finally:
                db.close()

        # 5. Knowledge base ingestion ──────────────────
        # Signature: ingest_file(file_path, category="general", source=None)
        # Passing the friendly fname as `source` keeps the KB tagged with
        # "odoo_export_<ts>.csv" instead of the random temp filename.
        if ingest_kb and upload_id:
            try:
                from app.services.knowledge_base import ingest_file as kb_ingest_file
                kb_ingest_file(
                    file_path=tmp_path,
                    category="odoo",
                    source=fname,
                )
            except Exception as e:
                _logger.warning("KB ingestion failed: %s", e)

    finally:
        # Always clean up the temp file.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return {
        "status":         "completed",
        "upload_id":      upload_id,
        "table_name":     table_name,
        "file_name":      fname,
        "rows":           int(len(df)),
        "columns":        int(len(df.columns)),
        "quality_before": quality_before,
        "quality_after":  quality_after,
        "extracted_at":   started_at.isoformat(),
        "completed_at":   datetime.utcnow().isoformat(),
        "filters": {
            "since":   since.isoformat() if since else None,
            "include": include or ["sales", "invoices", "leads"],
            "limit":   limit,
        },
    }


def get_last_sync(user_id: Optional[int] = None) -> Optional[datetime]:
    """
    Return the timestamp of the most recent successful Odoo sync.

    Used by Celery to do incremental pulls — pass it as ``since`` next run.
    """
    db = SessionLocal()
    try:
        q = (
            "SELECT MAX(uploaded_at) FROM uploads "
            "WHERE category = 'odoo' AND status = 'completed'"
        )
        params: dict = {}
        if user_id is not None:
            q += " AND user_id = :uid"
            params["uid"] = user_id
        row = db.execute(text(q), params).fetchone()
        return row[0] if row and row[0] else None
    finally:
        db.close()