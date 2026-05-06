"""
app/tasks/odoo_tasks.py
═══════════════════════════════════════════════════════════════════════════
ODOO SCHEDULED TASKS (Celery)
═══════════════════════════════════════════════════════════════════════════

Implements the nightly extraction described in Chapter 3, Stage 5:

    "A scheduled Celery task was configured to execute nightly data
     extraction at 01:00 UTC, connecting to the Odoo instance via
     OdooRPC / XML-RPC, querying the relevant Odoo models, and writing
     the consolidated dataset to the backend's analytics snapshot table
     in PostgreSQL."

Skip this file entirely if you don't have Celery wired up yet — the
``/api/v1/odoo/sync`` endpoint already runs syncs on demand. Add it the
moment you spin up a Celery worker.

Wire-up
-------
1. ``pip install celery[redis]==5.3.4``
2. Create ``app/celery_app.py``::

       from celery import Celery
       celery = Celery(
           "optimaai",
           broker  = os.getenv("REDIS_URL", "redis://localhost:6379/0"),
           backend = os.getenv("REDIS_URL", "redis://localhost:6379/0"),
       )
       celery.autodiscover_tasks(["app.tasks"])

3. Add the schedule to ``celery.conf.beat_schedule``::

       celery.conf.beat_schedule = {
           "odoo-nightly-sync": {
               "task": "app.tasks.odoo_tasks.nightly_odoo_sync",
               "schedule": crontab(hour=1, minute=0),  # 01:00 UTC
           },
       }

4. Run::

       celery -A app.celery_app worker -l info
       celery -A app.celery_app beat   -l info
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

_logger = logging.getLogger(__name__)

# Soft-import Celery so the module loads even when celery isn't installed —
# importing the task functions directly still works for manual scripts.
try:
    from app.celery_app import celery  # type: ignore
    _CELERY_AVAILABLE = True
except Exception:
    _CELERY_AVAILABLE = False

    # Decorator no-op so the function definitions below still parse and run.
    class _DummyCelery:
        def task(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

    celery = _DummyCelery()  # type: ignore


from app.services.odoo_extractor import sync_to_uploads, get_last_sync


# ══════════════════════════════════════════════════════
#  Service-account user
#  Scheduled syncs aren't tied to a real user, so we run them under a
#  dedicated service account. ID is configurable via env so you can
#  point it at whichever user owns the operational data.
# ══════════════════════════════════════════════════════

def _service_user_id() -> int:
    return int(os.getenv("ODOO_SYNC_USER_ID", "1"))


# ══════════════════════════════════════════════════════
#  Tasks
# ══════════════════════════════════════════════════════

@celery.task(
    bind=True,
    name="app.tasks.odoo_tasks.nightly_odoo_sync",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def nightly_odoo_sync(self, since_iso: Optional[str] = None) -> dict:
    """
    Pull everything modified since the last successful sync (or since the
    optional ``since_iso`` override) and feed it through the backend
    pipeline.

    Auto-retries up to 3 times with exponential backoff on any failure —
    Odoo restarts and transient network blips shouldn't kill the night.
    """
    since: Optional[datetime] = None
    if since_iso:
        since = datetime.fromisoformat(since_iso)
    else:
        since = get_last_sync()

    _logger.info(
        "Nightly Odoo sync starting (since=%s, user=%d)",
        since, _service_user_id(),
    )
    result = sync_to_uploads(
        user_id   = _service_user_id(),
        since     = since,
        include   = ["sales", "invoices", "leads"],
        ingest_kb = True,
    )
    _logger.info("Nightly Odoo sync result: %s", result)
    return result


@celery.task(name="app.tasks.odoo_tasks.full_odoo_resync")
def full_odoo_resync() -> dict:
    """
    Wipe-and-replace style sync — pulls full history (no ``since`` filter).

    Use after a credential change, schema change, or when the analytics
    snapshot drifts. Heavier than the nightly run, so trigger manually.
    """
    return sync_to_uploads(
        user_id   = _service_user_id(),
        since     = None,
        include   = ["sales", "invoices", "leads"],
        ingest_kb = True,
    )
