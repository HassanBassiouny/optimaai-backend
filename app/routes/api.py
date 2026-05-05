"""
app/routes/api.py
All OptimaAi API endpoints in one file:
  POST /api/v1/predict/revenue
  POST /api/v1/predict/churn
  POST /api/v1/predict/growth
  GET  /api/v1/kpis
  POST /api/v1/recommend
  POST /api/v1/bmc
  GET  /api/v1/bmc/latest
"""
from fastapi  import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, Any
from app.services.auth_service import get_current_user_optional
from app.database import get_db, KPISnapshot, Recommendation, BMCResult, User
from app.cache    import (
    cache_get, cache_set,
    kpi_key, forecast_key, recommend_key, bmc_key,
    TTL_KPI, TTL_FORECAST, TTL_RECOMMEND, TTL_BMC,
)
from app.services.inference_service    import (
    predict_revenue, predict_churn, predict_growth, get_latest_kpis
)
from app.services.recommendation_service import generate_recommendation
from app.services.bmc_service            import generate_bmc

router = APIRouter(prefix="/api/v1", tags=["OptimaAi"])


# ══════════════════════════════════════════════════════
#  REQUEST / RESPONSE SCHEMAS
# ══════════════════════════════════════════════════════

class RevenueRequest(BaseModel):
    features: dict
    class Config:
        json_schema_extra = {"example": {
            "features": {
                "quantity_sold": 3, "price": 299.0,
                "discount_percent": 10, "seasonal_index": 1.0,
                "gross_margin_pct": 0.45, "month": 6, "quarter": 2,
            }
        }}

class ChurnRequest(BaseModel):
    features: dict
    class Config:
        json_schema_extra = {"example": {
            "features": {
                "days_since_last_order": 95, "total_orders": 2,
                "avg_days_between_orders": 60, "churn_risk_score": 0.72,
                "total_spent": 1200.0, "avg_order_value": 600.0,
            }
        }}

class GrowthRequest(BaseModel):
    features: dict

class RecommendRequest(BaseModel):
    role: str = "Sales Manager"
    class Config:
        json_schema_extra = {"example": {"role": "Sales Manager"}}

class BMCRequest(BaseModel):
    platform_name: Optional[str] = "OptimaAi"
    data_source:   Optional[str] = "Odoo ERP"
    target_users:  Optional[str] = "SME Sales Managers, Finance Controllers, Executives"


# ══════════════════════════════════════════════════════
#  HEALTH CHECK
# ══════════════════════════════════════════════════════

@router.get("/health")
def health():
    return {"status": "ok", "service": "OptimaAi API", "timestamp": datetime.utcnow()}


# ══════════════════════════════════════════════════════
#  PREDICT — REVENUE
# ══════════════════════════════════════════════════════

@router.post("/predict/revenue")
def predict_revenue_route(
    req:          RevenueRequest,
    db:           Session                = Depends(get_db),
    current_user: Optional[User]         = Depends(get_current_user_optional),
):
    try:
        uid = current_user.id if current_user else None
        prediction = predict_revenue(req.features, user_id=uid, db=db)
        return {
            "prediction":  round(float(prediction), 2),
            "unit":        "USD",
            "model":       "XGBoost/GBM Revenue Regressor",
            "calibrated":  uid is not None,
            "timestamp":   datetime.utcnow(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════
#  PREDICT — CHURN
# ══════════════════════════════════════════════════════

@router.post("/predict/churn")
def predict_churn_route(
    req:          ChurnRequest,
    db:           Session                = Depends(get_db),
    current_user: Optional[User]         = Depends(get_current_user_optional),
):
    try:
        uid = current_user.id if current_user else None
        probability = predict_churn(req.features, user_id=uid, db=db)
        risk_level  = (
            "high"   if probability >= 0.7 else
            "medium" if probability >= 0.4 else
            "low"
        )
        return {
            "churn_probability": round(float(probability), 4),
            "risk_level":        risk_level,
            "model":             "XGBoost/GBM Churn Classifier",
            "calibrated":        uid is not None,
            "timestamp":         datetime.utcnow(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════
#  PREDICT — GROWTH
# ══════════════════════════════════════════════════════

@router.post("/predict/growth")
def predict_growth_route(
    req:          GrowthRequest,
    db:           Session                = Depends(get_db),
    current_user: Optional[User]         = Depends(get_current_user_optional),
):
    try:
        uid = current_user.id if current_user else None
        forecast = predict_growth(req.features, user_id=uid, db=db)
        return {
            "forecast_3m":  round(float(forecast), 2),
            "unit":         "USD",
            "model":        "XGBoost/GBM Growth Regressor",
            "calibrated":   uid is not None,
            "timestamp":    datetime.utcnow(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════
#  GET LATEST KPIs
#  Reads from cache first, then ML artefacts
# ══════════════════════════════════════════════════════

@router.get("/kpis")
def get_kpis(db: Session = Depends(get_db)):
    """
    Returns the latest KPI snapshot.
    Checks Redis cache first — if stale, reloads from artefacts
    and saves to PostgreSQL + Redis.
    """
    # 1. Try Redis cache
    cached = cache_get(kpi_key())
    if cached:
        cached["source"] = "cache"
        return cached

    # 2. Load from ML artefacts
    try:
        kpis = get_latest_kpis()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    rev   = kpis.get("revenue_model", {})
    churn = kpis.get("churn_model",   {})
    grow  = kpis.get("growth_model",  {})
    prop  = kpis.get("prophet_model", {})

    # 3. Save snapshot to PostgreSQL
    snapshot = KPISnapshot(
        revenue_mape         = rev.get("MAPE_pct"),
        revenue_r2           = rev.get("R2"),
        revenue_mae          = rev.get("MAE"),
        forecast_bias        = rev.get("forecast_bias"),
        churn_roc_auc        = churn.get("ROC_AUC"),
        churn_f1             = churn.get("F1_Score"),
        churn_rate_pct       = churn.get("churn_rate_pct"),
        growth_mape          = grow.get("MAPE_pct"),
        growth_r2            = grow.get("R2"),
        prophet_mape         = prop.get("MAPE_pct"),
        revenue_forecast_12  = rev.get("12_period_forecast"),
        growth_forecast_12   = grow.get("12_month_forecast"),
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)

    # 4. Build response
    result = {
        "snapshot_id":    snapshot.id,
        "created_at":     snapshot.created_at.isoformat(),
        "revenue_model":  rev,
        "churn_model":    churn,
        "growth_model":   grow,
        "prophet_model":  prop,
        "source":         "database",
    }

    # 5. Cache in Redis
    cache_set(kpi_key(), result, TTL_KPI)

    return result


# ══════════════════════════════════════════════════════
#  RECOMMEND — LLM role-based recommendation
# ══════════════════════════════════════════════════════

@router.post("/recommend")
def get_recommendation(req: RecommendRequest, db: Session = Depends(get_db)):
    """
    Generate an AI recommendation for a given role.
    Pulls latest KPIs from cache/artefacts and sends to LLM.
    Result is cached for 1 hour and saved to PostgreSQL.
    """
    role = req.role

    # 1. Try cache
    cached = cache_get(recommend_key(role))
    if cached:
        cached["source"] = "cache"
        return cached

    # 2. Get KPIs
    try:
        kpis = get_latest_kpis()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    rev   = kpis.get("revenue_model", {})
    churn = kpis.get("churn_model",   {})
    grow  = kpis.get("growth_model",  {})

    kpi_dict = {
        "mape":             rev.get("MAPE_pct", "N/A"),
        "forecast_bias":    rev.get("forecast_bias", "N/A"),
        "churn_rate":       churn.get("churn_rate_pct", "N/A"),
        "revenue_variance": f"R²={rev.get('R2', 'N/A')}",
        "growth_trend":     f"MAPE={grow.get('MAPE_pct', 'N/A')}%  R²={grow.get('R2', 'N/A')}",
    }

    # 3. Call LLM
    result = generate_recommendation(role=role, kpis=kpi_dict)
    if result.get("status") != "success":
        raise HTTPException(status_code=502, detail=result.get("error"))

    # 4. Save to PostgreSQL
    rec = Recommendation(
        role=role,
        recommendation=result["recommendation"],
        model_used=result.get("model", "openrouter/free"),
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)

    response = {
        "id":             rec.id,
        "role":           role,
        "recommendation": result["recommendation"],
        "model":          result.get("model"),
        "created_at":     rec.created_at.isoformat(),
        "source":         "live",
    }

    # 5. Cache for 1 hour
    cache_set(recommend_key(role), response, TTL_RECOMMEND)
    return response


# ══════════════════════════════════════════════════════
#  BMC — Business Model Canvas
# ══════════════════════════════════════════════════════

@router.post("/bmc")
def generate_bmc_route(req: BMCRequest, db: Session = Depends(get_db)):
    """
    Generate a full 9-block Business Model Canvas from live ML KPIs.
    Result cached for 24 hours and saved to PostgreSQL.
    """
    # 1. Try cache
    cached = cache_get(bmc_key())
    if cached:
        cached["source"] = "cache"
        return cached

    # 2. Load KPIs
    try:
        kpis = get_latest_kpis()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # 3. Generate BMC
    platform_config = {
        "platform_name": req.platform_name,
        "data_source":   req.data_source,
        "target_users":  req.target_users,
    }
    result = generate_bmc(kpis=kpis, platform_config=platform_config)
    if result.get("status") != "success":
        raise HTTPException(status_code=502, detail=result.get("error"))

    # 4. Save to PostgreSQL
    bmc = BMCResult(
        platform_name=req.platform_name,
        bmc_text=result["bmc_text"],
        bmc_blocks=result.get("bmc_blocks", {}),
        model_used=result.get("model", "openrouter/free"),
    )
    db.add(bmc)
    db.commit()
    db.refresh(bmc)

    response = {
        "id":         bmc.id,
        "bmc_blocks": result.get("bmc_blocks", {}),
        "bmc_text":   result["bmc_text"],
        "model":      result.get("model"),
        "created_at": bmc.created_at.isoformat(),
        "source":     "live",
    }

    # 5. Cache for 24 hours
    cache_set(bmc_key(), response, TTL_BMC)
    return response


@router.get("/bmc/latest")
def get_latest_bmc(db: Session = Depends(get_db)):
    """Return the most recently generated BMC from the database."""
    bmc = db.query(BMCResult).order_by(BMCResult.created_at.desc()).first()
    if not bmc:
        raise HTTPException(status_code=404, detail="No BMC generated yet.")
    return {
        "id":         bmc.id,
        "bmc_blocks": bmc.bmc_blocks,
        "model":      bmc.model_used,
        "created_at": bmc.created_at.isoformat(),
    }
