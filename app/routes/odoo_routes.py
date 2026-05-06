"""
app/routes/odoo_routes.py
═══════════════════════════════════════════════════════════════════════════
ODOO API ROUTES
═══════════════════════════════════════════════════════════════════════════

Endpoints exposed to the Next.js frontend for managing the Odoo data source:

  GET   /api/v1/odoo/status            current configuration + last sync
  POST  /api/v1/odoo/test-connection   ping Odoo (auth + record counts)
  POST  /api/v1/odoo/sync              trigger an extraction now
  GET   /api/v1/odoo/preview           sample N rows without persisting
  GET   /api/v1/odoo/syncs             list previous Odoo-sourced uploads

All endpoints require an authenticated user. The user_id is taken from the
JWT — no override permitted (data-scope enforcement, per Chapter 3 §3.2.4).
"""
from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db, User
from app.services.auth_service import get_current_user
from app.services.odoo_service import (
    OdooConnector, OdooConnectionError, OdooConfigError, get_connector,
    reset_connector,
)
from app.services.odoo_extractor import (
    extract_to_dataframe, sync_to_uploads, get_last_sync,
)

_logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/odoo", tags=["Odoo Integration"])


# ══════════════════════════════════════════════════════
#  Schemas
# ══════════════════════════════════════════════════════

class OdooSyncRequest(BaseModel):
    """Body for POST /sync."""
    incremental: bool = Field(
        default=True,
        description="If true, only pull records modified since the last sync.",
    )
    since: Optional[datetime] = Field(
        default=None,
        description="Override timestamp. Ignored if incremental=false.",
    )
    include: Optional[list[str]] = Field(
        default=None,
        description='Subset of ["sales","invoices","leads"]. Default = all.',
    )
    limit: Optional[int] = Field(
        default=None, ge=1, le=100_000,
        description="Per-model row cap — useful for smoke tests.",
    )
    ingest_kb: bool = Field(
        default=True,
        description="Also push the cleaned snapshot into the RAG KB.",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "incremental": True,
                "include":     ["sales", "invoices"],
                "limit":       1000,
                "ingest_kb":   True,
            }
        }


class OdooPreviewRequest(BaseModel):
    """Query params for GET /preview, expressed as a model for clarity."""
    model:  str = Field(default="sale.order", description="Odoo model to preview.")
    limit:  int = Field(default=10, ge=1, le=200)


# ══════════════════════════════════════════════════════
#  GET /status — surfaces config + last sync to the frontend
# ══════════════════════════════════════════════════════

@router.get("/status")
def get_status(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """
    Lightweight status endpoint — does NOT contact Odoo.

    The Datasets page renders the Odoo card using this data; a live ping
    happens only when the user clicks "Test connection".
    """
    last_sync = get_last_sync()

    sync_count = db.execute(
        text("SELECT COUNT(*) FROM uploads WHERE category = 'odoo'")
    ).scalar() or 0

    return {
        "configured": bool(
            os.getenv("ODOO_URL") and os.getenv("ODOO_DB")
            and os.getenv("ODOO_USERNAME")
            and (os.getenv("ODOO_PASSWORD") or os.getenv("ODOO_API_KEY"))
        ),
        "url":          os.getenv("ODOO_URL"),
        "db":           os.getenv("ODOO_DB"),
        "username":     os.getenv("ODOO_USERNAME"),
        "auth_method":  "api_key" if os.getenv("ODOO_API_KEY") else "password",
        "last_sync_at": last_sync.isoformat() if last_sync else None,
        "sync_count":   sync_count,
    }


# ══════════════════════════════════════════════════════
#  POST /test-connection — live auth + record counts
# ══════════════════════════════════════════════════════

@router.post("/test-connection")
def test_connection(current_user: User = Depends(get_current_user)):
    """
    Force a fresh authentication against Odoo and return record counts.

    Calls ``reset_connector()`` first so an old cached failure or rotated
    credential doesn't mask the real state.
    """
    try:
        reset_connector()
        conn = get_connector()
        return {"ok": True, **conn.ping()}
    except OdooConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except OdooConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        _logger.exception("Unexpected error during Odoo test-connection")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


# ══════════════════════════════════════════════════════
#  POST /sync — full extraction + pipeline run
# ══════════════════════════════════════════════════════

@router.post("/sync")
def sync_now(
    req:              OdooSyncRequest,
    background_tasks: BackgroundTasks,
    current_user:     User = Depends(get_current_user),
):
    """
    Trigger an Odoo → backend sync.

    For small instances we run inline; for bigger pulls (limit > 5000 or
    incremental=False) we hand off to a background task so the HTTP response
    isn't blocked. In production, swap the BackgroundTask for a Celery
    ``.delay()`` call — see ``app/tasks/odoo_tasks.py``.
    """
    # Resolve the ``since`` timestamp.
    since: Optional[datetime] = None
    if req.incremental:
        since = req.since or get_last_sync()
        # First-ever run: leave as None to do a full historical pull.

    # Decide between sync and background execution.
    is_heavy = (req.limit is None) or (req.limit > 5000) or (since is None)

    if is_heavy:
        background_tasks.add_task(
            sync_to_uploads,
            user_id   = current_user.id,
            since     = since,
            include   = req.include,
            ingest_kb = req.ingest_kb,
            limit     = req.limit,
        )
        return {
            "status":   "queued",
            "message":  "Sync running in background — check /api/v1/odoo/syncs.",
            "since":    since.isoformat() if since else None,
            "include":  req.include or ["sales", "invoices", "leads"],
        }

    try:
        result = sync_to_uploads(
            user_id   = current_user.id,
            since     = since,
            include   = req.include,
            ingest_kb = req.ingest_kb,
            limit     = req.limit,
        )
        return result
    except OdooConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except OdooConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        _logger.exception("Odoo sync failed")
        raise HTTPException(status_code=500, detail=f"Sync failed: {e}")


# ══════════════════════════════════════════════════════
#  GET /preview — sample without persisting
# ══════════════════════════════════════════════════════

@router.get("/preview")
def preview(
    model: str = "sale.order",
    limit: int = 10,
    current_user: User = Depends(get_current_user),
):
    """
    Return a tiny sample of records from Odoo without writing anything.

    Used by the Datasets page to show users what their Odoo data looks like
    before they trigger a full sync.
    """
    if model not in {"sale.order", "account.move", "crm.lead", "res.partner"}:
        raise HTTPException(
            status_code=400,
            detail="Unsupported model. Use sale.order, account.move, "
                   "crm.lead, or res.partner.",
        )
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be 1–200")

    try:
        conn = get_connector()
        if model == "sale.order":
            rows = conn.fetch_sale_orders(limit=limit)
        elif model == "account.move":
            rows = conn.fetch_invoices(limit=limit)
        elif model == "crm.lead":
            rows = conn.fetch_leads(limit=limit)
        else:
            rows = conn.fetch_partners(limit=limit)
        return {"model": model, "count": len(rows), "rows": rows}
    except OdooConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except OdooConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))


# ══════════════════════════════════════════════════════
#  GET /syncs — past Odoo-sourced uploads for the user
# ══════════════════════════════════════════════════════

@router.get("/syncs")
def list_syncs(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
    limit:        int     = 50,
):
    """List previous Odoo extractions for the current user."""
    rows = db.execute(
        text(
            "SELECT upload_id, original_file_name, table_name, "
            "       rows_count, columns_count, "
            "       quality_before, quality_after, "
            "       uploaded_at, status "
            "FROM uploads "
            "WHERE category = 'odoo' AND user_id = :uid "
            "ORDER BY uploaded_at DESC "
            "LIMIT :lim"
        ),
        {"uid": current_user.id, "lim": limit},
    ).fetchall()

    return {
        "data": [
            {
                "id":             r[0],
                "file":           r[1],
                "table_name":     r[2],
                "rows":           r[3] or 0,
                "columns":        r[4] or 0,
                "quality_before": r[5],
                "quality_after":  r[6],
                "uploaded_at":    r[7].isoformat() if r[7] else None,
                "status":         r[8] or "completed",
            }
            for r in rows
        ]
    }
