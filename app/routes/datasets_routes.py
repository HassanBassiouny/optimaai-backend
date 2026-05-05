"""
app/routes/datasets_routes.py

Connects the Datasets page to:
  - DynamicPreprocessingPipeline (cleans uploaded files)
  - PostgreSQL uploads table (tracks all datasets)
  - ChromaDB knowledge base (makes data queryable by LLM)

Endpoints:
  POST   /api/v1/datasets/upload     ← Upload Data button
  GET    /api/v1/datasets            ← datasets table list
  GET    /api/v1/datasets/{id}       ← single dataset detail
  DELETE /api/v1/datasets/{id}       ← Delete button
"""

import os
import shutil
import tempfile
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Form
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db, engine
from app.services.knowledge_base import ingest_file as kb_ingest_file

router = APIRouter(prefix="/api/v1/datasets", tags=["Datasets"])

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".pdf", ".txt", ".json"}


# ══════════════════════════════════════════════════════
#  POST /upload  — Upload Data button on Datasets page
# ══════════════════════════════════════════════════════

@router.post("/upload")
async def upload_dataset(
    file:     UploadFile = File(...),
    user_id:  int = Form(1),
    category: str = Form("general"),
    db: Session = Depends(get_db),
):
    """
    Full pipeline for uploaded file:
    1. Validate file type
    2. Run DynamicPreprocessingPipeline (clean + quality score)
    3. Save cleaned data to PostgreSQL as cleaned_<name>_<ts> table
    4. Register upload in uploads table
    5. Ingest into ChromaDB knowledge base
    6. Return dataset metadata to frontend
    """
    # ── Validate ────────────────────────────────────────────────
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {ALLOWED_EXTENSIONS}"
        )

    # ── Save to temp file ────────────────────────────────────────
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    try:
        db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://optimaai:optimaai123@localhost:5432/optimaai_db"
        )

        # ── Run preprocessing pipeline ───────────────────────────
        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ml"))
            from dynamic_preprocessing_pipeline import DynamicPreprocessingPipeline

            pipeline = DynamicPreprocessingPipeline()
            cleaned_df, report = pipeline.run(
                source  = tmp_path,
                db_url  = db_url,
                user_id = user_id,
            )

            quality_before = report.get("quality_score", {}).get("before", 0)
            quality_after  = report.get("quality_score", {}).get("after", 0)
            rows           = len(cleaned_df)
            cols           = len(cleaned_df.columns)

            # The pipeline wrote the temp basename into uploads.original_file_name,
            # so look it up by that — then patch in the real filename.
            tmp_basename = os.path.basename(tmp_path)
            result = db.execute(
                text("SELECT upload_id, table_name FROM uploads "
                     "WHERE original_file_name = :fname "
                     "ORDER BY uploaded_at DESC LIMIT 1"),
                {"fname": tmp_basename}
            ).fetchone()

            upload_id  = result[0] if result else None
            table_name = result[1] if result else None

            # ── Patch the real filename back in ─────────────────────
            if upload_id:
                db.execute(
                    text("UPDATE uploads SET original_file_name = :real "
                         "WHERE upload_id = :id"),
                    {"real": file.filename, "id": upload_id}
                )
                db.commit()

                # ── Build derived aggregate tables (for churn + growth) ─
                try:
                    from app.services.aggregation_service import build_aggregates
                    agg = build_aggregates(engine, table_name)
                    print(f"  [aggregates] roles: {agg['diagnostics'].get('detected_roles')}")
                    if agg.get("customer_table"):
                        print(f"  [aggregates] customer table: {agg['customer_table']} "
                              f"({agg['diagnostics'].get('customer_rows')} rows)")
                    if agg.get("monthly_table"):
                        print(f"  [aggregates] monthly table: {agg['monthly_table']} "
                              f"({agg['diagnostics'].get('monthly_rows')} rows)")
                    db.execute(
                        text("UPDATE uploads SET "
                             "customer_table_name = :c, monthly_table_name = :m "
                             "WHERE upload_id = :id"),
                        {"c": agg.get("customer_table"),
                         "m": agg.get("monthly_table"),
                         "id": upload_id}
                    )
                    db.commit()
                except Exception as e:
                    print(f"  [aggregates] failed: {e}")

        except ImportError:
            # Pipeline not available — still register the upload manually
            quality_before, quality_after = 0, 0
            rows, cols = 0, 0
            upload_id, table_name = None, None

            # Manual registration fallback
            import re
            base  = os.path.splitext(file.filename)[0]
            clean = re.sub(r'[^a-z0-9_]', '_', base.lower()).strip('_')
            ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_name = f"cleaned_{clean}_{ts}"

            db.execute(text("""
                INSERT INTO uploads
                  (user_id, original_file_name, table_name, rows_count,
                   columns_count, quality_before, quality_after, status)
                VALUES (:uid, :fname, :tname, :rows, :cols, :qb, :qa, 'pending')
            """), {
                "uid": user_id, "fname": file.filename,
                "tname": table_name, "rows": rows, "cols": cols,
                "qb": quality_before, "qa": quality_after,
            })
            db.commit()

        # ── Ingest into knowledge base ────────────────────────────
        kb_result = kb_ingest_file(
            tmp_path,
            category=category,
            source=file.filename,   # pass real upload name, not temp path
        )

        return {
            "status":        "success",
            "upload_id":     upload_id,
            "filename":      file.filename,
            "table_name":    table_name,
            "rows":          rows,
            "columns":       cols,
            "quality_before": quality_before,
            "quality_after":  quality_after,
            "kb_chunks":     kb_result.get("chunks", 0),
            "category":      category,
        }

    finally:
        os.unlink(tmp_path)


# ══════════════════════════════════════════════════════
#  GET /  — Load datasets table on Datasets page
# ══════════════════════════════════════════════════════

@router.get("")
def list_datasets(db: Session = Depends(get_db)):
    """
    Returns all datasets for the Datasets page table.
    """
    rows = db.execute(text("""
        SELECT
            upload_id,
            original_file_name,
            table_name,
            rows_count,
            columns_count,
            quality_before,
            quality_after,
            uploaded_at,
            status,
            user_id
        FROM uploads
        ORDER BY uploaded_at DESC
    """)).fetchall()

    return [
        {
            "id":             r._mapping["upload_id"],
            "name":           _friendly_name(r._mapping["original_file_name"]),
            "file":           r._mapping["original_file_name"],
            "table_name":     r._mapping["table_name"],
            "rows":           r._mapping["rows_count"] or 0,
            "columns":        r._mapping["columns_count"] or 0,
            "quality_before": r._mapping["quality_before"],
            "quality_after":  r._mapping["quality_after"],
            "uploaded_at":    r._mapping["uploaded_at"].isoformat() if r._mapping["uploaded_at"] else None,
            "status":         r._mapping["status"] or "completed",
            "user_id":        r._mapping["user_id"],
        }
        for r in rows
    ]

# ══════════════════════════════════════════════════════
#  GET /{id}/preview/{view}  — Data preview (cleaned / customer / monthly)
# ══════════════════════════════════════════════════════

@router.get("/{upload_id}/preview/{view}")
def preview_dataset(
    upload_id: int,
    view: str,                  # "cleaned" | "customer" | "monthly"
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """
    Returns up to `limit` rows of the cleaned/customer/monthly table
    plus the list of column names and total row count.
    """
    upload = db.execute(
        text("SELECT upload_id, table_name, customer_table_name, monthly_table_name "
             "FROM uploads WHERE upload_id = :id"),
        {"id": upload_id}
    ).fetchone()

    if not upload:
        raise HTTPException(status_code=404, detail="Dataset not found")

    table_by_view = {
        "cleaned":  upload.table_name,
        "customer": upload.customer_table_name,
        "monthly":  upload.monthly_table_name,
    }
    if view not in table_by_view:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown view '{view}'. Use: cleaned | customer | monthly"
        )

    source_table = table_by_view[view]
    if not source_table:
        return {
            "upload_id":   upload_id,
            "view":        view,
            "table":       None,
            "available":   False,
            "total_rows":  0,
            "columns":     [],
            "rows":        [],
        }

    # Clamp limit to a sane range
    limit = max(1, min(limit, 500))

    # Total row count
    try:
        total = db.execute(
            text(f'SELECT COUNT(*) FROM "{source_table}"')
        ).scalar() or 0
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not count rows: {e}")

    # Fetch preview rows
    try:
        result = db.execute(
            text(f'SELECT * FROM "{source_table}" LIMIT :lim'),
            {"lim": limit}
        ).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read table: {e}")

    columns: list = []
    rows: list = []
    for r in result:
        m = dict(r._mapping)
        if not columns:
            columns = list(m.keys())
        row = {}
        for k, v in m.items():
            if v is None:
                row[k] = None
            elif isinstance(v, (int, float, str, bool)):
                row[k] = v
            else:
                row[k] = str(v)
        rows.append(row)

    # If the table is completely empty, get columns from information_schema
    if not columns:
        columns = list(db.execute(
            text("SELECT column_name FROM information_schema.columns "
                 "WHERE table_name = :t ORDER BY ordinal_position"),
            {"t": source_table}
        ).scalars())

    return {
        "upload_id":  upload_id,
        "view":       view,
        "table":      source_table,
        "available":  True,
        "total_rows": total,
        "columns":    columns,
        "rows":       rows,
    }

# ══════════════════════════════════════════════════════
#  GET /{id}  — Dataset detail
# ══════════════════════════════════════════════════════

@router.get("/{upload_id}")
def get_dataset(upload_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            SELECT upload_id, user_id, original_file_name, table_name,
                   rows_count, columns_count, quality_before, quality_after,
                   uploaded_at, status, customer_table_name, monthly_table_name
            FROM uploads WHERE upload_id = :id
        """),
        {"id": upload_id}
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")

    m = row._mapping
    return {
        "id":          m["upload_id"],
        "user_id":     m["user_id"],
        "file":        m["original_file_name"],
        "table_name":  m["table_name"],
        "rows":        m["rows_count"],
        "columns":     m["columns_count"],
        "quality_before": m["quality_before"],
        "quality_after":  m["quality_after"],
        "uploaded_at": m["uploaded_at"].isoformat() if m["uploaded_at"] else None,
        "status":      m["status"],
        "customer_table_name": m["customer_table_name"],
        "monthly_table_name":  m["monthly_table_name"],
    }


# ══════════════════════════════════════════════════════
#  DELETE /{id}  — Delete button on Datasets page
# ══════════════════════════════════════════════════════

@router.delete("/{upload_id}")
def delete_dataset(upload_id: int, db: Session = Depends(get_db)):
    """
    Deletes a dataset and everything attached to it:
      - column_mappings rows (FK)
      - predictions rows (FK, ON DELETE is not CASCADE, so we delete explicitly)
      - the cleaned_* table (dropped from PostgreSQL)
      - the agg_customer_* table if built
      - the agg_monthly_* table if built
      - the uploads row itself
      - the knowledge base entries for the file
    """
    row = db.execute(
        text("""
            SELECT table_name, customer_table_name, monthly_table_name,
                   original_file_name
            FROM uploads WHERE upload_id = :id
        """),
        {"id": upload_id}
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")

    m = row._mapping
    table_name     = m["table_name"]
    customer_table = m["customer_table_name"]
    monthly_table  = m["monthly_table_name"]
    file_name      = m["original_file_name"]

    # ── 1. Remove FK-referencing rows (column_mappings, predictions) ────
    try:
        db.execute(
            text("DELETE FROM column_mappings WHERE upload_id = :id"),
            {"id": upload_id}
        )
        db.execute(
            text("DELETE FROM predictions WHERE upload_id = :id"),
            {"id": upload_id}
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Could not clear dependent rows: {e}"
        )

    # ── 2. Drop derived data tables ─────────────────────────────────────
    for t in (table_name, customer_table, monthly_table):
        if t:
            try:
                db.execute(text(f'DROP TABLE IF EXISTS "{t}"'))
            except Exception as e:
                print(f"  [datasets] could not drop {t}: {e}")

    # ── 3. Delete the uploads row ───────────────────────────────────────
    try:
        db.execute(
            text("DELETE FROM uploads WHERE upload_id = :id"),
            {"id": upload_id}
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Could not delete upload: {e}")

    # ── 4. Remove from knowledge base ───────────────────────────────────
    try:
        from app.services.knowledge_base import delete_source
        delete_source(file_name)
    except Exception as e:
        print(f"  [datasets] KB cleanup failed for {file_name}: {e}")

    return {
        "status":         "deleted",
        "upload_id":      upload_id,
        "table_name":     table_name,
        "customer_table": customer_table,
        "monthly_table":  monthly_table,
    }


# ── Helper ─────────────────────────────────────────────────────────
def _friendly_name(filename: str) -> str:
    """Convert filename to display name: customer-churn-q1.csv → Customer Churn Q1"""
    if not filename:
        return "Unknown"
    base = os.path.splitext(filename)[0]
    return base.replace("-", " ").replace("_", " ").title()