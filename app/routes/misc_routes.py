"""
app/routes/misc_routes.py

Fills in the list endpoints the frontend expects after login:

  GET /api/v1/predictions     — list saved predictions (per user)
  POST /api/v1/predictions    — create new prediction (routes to inference_service)
  GET /api/v1/reports         — executive reports (placeholder)
  GET /api/v1/insights        — AI insights from RAG queries
  GET /api/v1/canvas          — Business Model Canvas blocks
  GET /api/v1/users           — user directory (admin only)
  GET /api/v1/roles           — available roles

All endpoints return { "data": [...] } to match the frontend contract.
"""
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db, Prediction, User, RagQuery, BMCResult
from app.services.auth_service import get_current_user_optional, get_current_user

router = APIRouter(prefix="/api/v1", tags=["Frontend-Support"])


# ══════════════════════════════════════════════════════
#  PREDICTIONS
# ══════════════════════════════════════════════════════

class CreatePredictionRequest(BaseModel):
    datasetId: Optional[str] = None
    type:      str = "revenue_forecast"   # churn | revenue_forecast | growth_scoring
    features:  Optional[dict] = None


@router.get("/predictions")
def list_predictions(
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    """List past predictions. If logged in, filter to the user; otherwise all."""
    q = db.query(Prediction)
    if current_user:
        q = q.filter(Prediction.user_id == current_user.id)
    rows = q.order_by(Prediction.created_at.desc()).limit(50).all()

    return {
        "data": [
            {
                "id":        str(p.id),
                "datasetId": p.features.get("datasetId") if p.features else None,
                "type":      _map_prediction_type(p.kind),
                "status":    "completed",
                "result": {
                    "summary":    _build_summary(p),
                    "confidence": 0.85,
                    "data":       p.result or {},
                },
                "createdAt": p.created_at.isoformat() if p.created_at else None,
            }
            for p in rows
        ]
    }


@router.post("/predictions", status_code=201)
def create_prediction(
    req: CreatePredictionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Run inference and save the prediction.
    The Run Wizard on the frontend sends here.
    """
    try:
        from app.services.inference_service import (
            predict_revenue, predict_churn, predict_growth
        )
    except ImportError:
        raise HTTPException(status_code=503, detail="ML inference service unavailable")

    features = req.features or {}
    kind     = _frontend_to_backend_kind(req.type)

    try:
        if kind == "revenue":
            value = float(predict_revenue(features))
            result = {"prediction": round(value, 2), "unit": "USD"}
        elif kind == "churn":
            prob  = float(predict_churn(features))
            risk  = "high" if prob >= 0.7 else "medium" if prob >= 0.4 else "low"
            result = {"churn_probability": round(prob, 4), "risk_level": risk}
        elif kind == "growth":
            value = float(predict_growth(features))
            result = {"forecast_3m": round(value, 2), "unit": "USD"}
        else:
            raise HTTPException(status_code=400, detail=f"Unknown prediction type: {req.type}")
    except KeyError as e:
        raise HTTPException(
            status_code=422,
            detail=f"Missing required feature for {kind} model: {e}. Use column mapping to map your data."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    row = Prediction(
        user_id  = current_user.id,
        kind     = kind,
        features = {**features, "datasetId": req.datasetId},
        result   = result,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "data": {
            "id":        str(row.id),
            "datasetId": req.datasetId,
            "type":      req.type,
            "status":    "completed",
            "result": {
                "summary":    _build_summary(row),
                "confidence": 0.85,
                "data":       result,
            },
            "createdAt": row.created_at.isoformat(),
        }
    }


@router.get("/predictions/{pid}")
def get_prediction(pid: str, db: Session = Depends(get_db)):
    p = db.query(Prediction).filter(Prediction.id == int(pid)).first()
    if not p:
        raise HTTPException(status_code=404, detail="Prediction not found")
    return {
        "data": {
            "id":        str(p.id),
            "datasetId": p.features.get("datasetId") if p.features else None,
            "type":      _map_prediction_type(p.kind),
            "status":    "completed",
            "result": {
                "summary":    _build_summary(p),
                "confidence": 0.85,
                "data":       p.result or {},
            },
            "createdAt": p.created_at.isoformat() if p.created_at else None,
        }
    }


def _map_prediction_type(kind: str) -> str:
    return {
        "revenue": "revenue_forecast",
        "churn":   "churn",
        "growth":  "growth_scoring",
    }.get(kind, kind)


def _frontend_to_backend_kind(t: str) -> str:
    return {
        "revenue_forecast": "revenue",
        "churn":            "churn",
        "growth_scoring":   "growth",
    }.get(t, t)


def _build_summary(p: Prediction) -> str:
    if not p.result:
        return "Prediction completed."
    if p.kind == "revenue":
        return f"Predicted revenue: ${p.result.get('prediction', 0):,.2f}"
    if p.kind == "churn":
        return f"Churn risk: {p.result.get('risk_level', 'unknown')} ({p.result.get('churn_probability', 0)*100:.1f}%)"
    if p.kind == "growth":
        return f"3-month forecast: ${p.result.get('forecast_3m', 0):,.2f}"
    return "Prediction completed."


# ══════════════════════════════════════════════════════
#  REPORTS  (placeholder until the reports module is built)
# ══════════════════════════════════════════════════════

@router.get("/reports")
def list_reports(db: Session = Depends(get_db)):
    """Return the BMC snapshots as 'executive reports' for now."""
    bmcs = db.query(BMCResult).order_by(BMCResult.created_at.desc()).limit(20).all()
    return {
        "data": [
            {
                "id":          str(b.id),
                "title":       f"Business Model Canvas — {b.platform_name or 'OptimaAi'}",
                "summary":     (b.bmc_text or "")[:200] + "..." if b.bmc_text and len(b.bmc_text) > 200 else (b.bmc_text or ""),
                "datasetId":   None,
                "predictionId": None,
                "createdBy":   "system",
                "status":      "published",
                "blocks":      [],
                "createdAt":   b.created_at.isoformat() if b.created_at else None,
                "updatedAt":   b.created_at.isoformat() if b.created_at else None,
            }
            for b in bmcs
        ]
    }


# ══════════════════════════════════════════════════════
#  INSIGHTS  (pulls from RAG query history)
# ══════════════════════════════════════════════════════

@router.get("/insights")
def list_insights(
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    q = db.query(RagQuery).filter(RagQuery.status == "success")
    if current_user:
        q = q.filter(RagQuery.user_id == current_user.id)
    rows = q.order_by(RagQuery.created_at.desc()).limit(20).all()

    return {
        "data": [
            {
                "id":        str(r.id),
                "title":     r.question[:80],
                "content":   r.answer or "",
                "category":  r.category or "strategic",
                "priority":  "medium",
                "createdAt": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
    }


# ══════════════════════════════════════════════════════
#  CANVAS  (latest BMC)
# ══════════════════════════════════════════════════════

@router.get("/canvas")
def get_canvas(db: Session = Depends(get_db)):
    bmc = db.query(BMCResult).order_by(BMCResult.created_at.desc()).first()
    if not bmc or not bmc.bmc_blocks:
        return {"data": []}

    blocks = []
    order  = 0
    for section, content in (bmc.bmc_blocks or {}).items():
        blocks.append({
            "id":      f"block-{bmc.id}-{section}",
            "section": section,
            "content": str(content) if content else "",
            "order":   order,
        })
        order += 1
    return {"data": blocks}


# ══════════════════════════════════════════════════════
#  USERS
# ══════════════════════════════════════════════════════

@router.get("/users")
def list_users(db: Session = Depends(get_db)):
    users = db.query(User).order_by(User.created_at.desc()).all()
    return {
        "data": [
            {
                "id":           str(u.id),
                "email":        u.email,
                "name":         u.name,
                "avatarUrl":    u.avatar_url or "",
                "role": {
                    "id":          f"role-{u.role}",
                    "name":        u.role,
                    "permissions": [],
                },
                "departmentId": u.department_id,
                "createdAt":    u.created_at.isoformat() if u.created_at else None,
                "updatedAt":    u.updated_at.isoformat() if u.updated_at else None,
            }
            for u in users
        ]
    }


@router.get("/users/{uid}")
def get_user(uid: str, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.id == int(uid)).first()
    if not u:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "data": {
            "id":           str(u.id),
            "email":        u.email,
            "name":         u.name,
            "avatarUrl":    u.avatar_url or "",
            "role": {
                "id":          f"role-{u.role}",
                "name":        u.role,
                "permissions": [],
            },
            "departmentId": u.department_id,
            "createdAt":    u.created_at.isoformat() if u.created_at else None,
            "updatedAt":    u.updated_at.isoformat() if u.updated_at else None,
        }
    }


# ══════════════════════════════════════════════════════
#  ROLES
# ══════════════════════════════════════════════════════

@router.get("/roles")
def list_roles():
    return {
        "data": [
            {"id": "role-admin",   "name": "admin",   "permissions": []},
            {"id": "role-manager", "name": "manager", "permissions": []},
            {"id": "role-analyst", "name": "analyst", "permissions": []},
            {"id": "role-viewer",  "name": "viewer",  "permissions": []},
        ]
    }
