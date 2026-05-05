"""
app/routes/dashboard_routes.py
═══════════════════════════════════════════════════════════════════════════
DASHBOARD API
═══════════════════════════════════════════════════════════════════════════

Endpoint:
  GET  /api/v1/dashboard/stats          ← live KPIs + chart data

Returns dashboard data computed fresh from the user's most recent upload.
Frontend hits this on every page load -- pandas aggregation over a few
thousand rows is fast (~50ms).
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.dashboard_service import get_dashboard_stats

router = APIRouter(prefix="/api/v1/dashboard", tags=["Dashboard"])


@router.get("/stats")
def get_stats(
    user_id: int = Query(1, description="User whose dashboard to compute"),
    db: Session = Depends(get_db),
):
    """
    Live dashboard stats. Computed on every request from the user's most
    recent uploaded dataset. Returns:

      kpis           — total_customers, total_orders, avg_order_value, total_revenue
      charts         — revenue_trend (last 12 months), top_categories (top 5)
      currency       — auto-detected from data (EGP / AED / USD / etc.)
      source_file    — which upload these stats came from
      uploaded_at    — when that file was uploaded
    """
    try:
        return get_dashboard_stats(db, user_id=user_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Dashboard stats computation failed: {e}",
        )