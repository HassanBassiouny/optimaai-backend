"""
app/routes/mapping_routes.py

Column-mapping endpoints used by the Run Prediction wizard.
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd

from app.database import get_db, engine as _engine, Upload, ColumnMapping, User, Prediction
from app.services.auth_service import get_current_user
from app.services.column_mapping_service import (
    suggest_mapping, apply_mapping, required_features, all_model_kinds,
)

router = APIRouter(prefix="/api/v1/mapping", tags=["Column Mapping"])


class SaveMappingRequest(BaseModel):
    upload_id:  int
    model_kind: str
    mapping:    dict


class PredictWithMappingRequest(BaseModel):
    upload_id:     int
    model_kind:    str
    user_features: dict


@router.get("/required/{model_kind}")
def get_required_features(model_kind: str):
    feats = required_features(model_kind)
    if not feats:
        raise HTTPException(status_code=400,
                            detail=f"Unknown model_kind. Valid: {all_model_kinds()}")
    return {
        "model_kind": model_kind,
        "features":   feats,
        "count":      len(feats),
    }


@router.post("/suggest/{upload_id}")
def suggest(
    upload_id:  int,
    model_kind: str = "revenue",
    db:         Session = Depends(get_db),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")

    source_table = upload.table_name
    if model_kind == "churn" and getattr(upload, "customer_table_name", None):
        source_table = upload.customer_table_name
    elif model_kind == "growth" and getattr(upload, "monthly_table_name", None):
        source_table = upload.monthly_table_name

    if not source_table:
        raise HTTPException(
            status_code=400,
            detail="Upload has no cleaned table yet — preprocessing may have failed"
        )

    try:
        row = db.execute(
            text(f'SELECT * FROM "{source_table}" LIMIT 1')
        ).fetchone()
        if row is None:
            cols = list(db.execute(
                text(f'SELECT column_name FROM information_schema.columns '
                     f'WHERE table_name = :t'),
                {"t": source_table}
            ).scalars())
        else:
            cols = list(row._mapping.keys())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read table: {e}")

    suggestion = suggest_mapping(cols, model_kind)
    suggestion["upload_id"] = upload_id
    suggestion["table"]     = source_table
    return suggestion


@router.get("/aggregate-rows/{upload_id}/{model_kind}")
def list_aggregate_rows(
    upload_id:  int,
    model_kind: str,
    db:         Session = Depends(get_db),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")

    if model_kind == "churn":
        source_table = getattr(upload, "customer_table_name", None)
        label_col    = "customer_id"
    elif model_kind == "growth":
        source_table = getattr(upload, "monthly_table_name", None)
        label_col    = "month_key"
    else:
        return {
            "upload_id":  upload_id,
            "model_kind": model_kind,
            "table":      None,
            "rows":       [],
        }

    if not source_table:
        raise HTTPException(
            status_code=404,
            detail=f"No aggregate table built for {model_kind}."
        )

    try:
        result = db.execute(
            text(f'SELECT * FROM "{source_table}" LIMIT 500')
        ).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read aggregate table: {e}")

    rows = []
    for r in result:
        m = dict(r._mapping)
        features = {}
        for k, v in m.items():
            if v is None:
                features[k] = None
            elif isinstance(v, (int, float, str, bool)):
                features[k] = v
            else:
                features[k] = str(v)
        label = features.get(label_col, f"row_{len(rows) + 1}")
        rows.append({"label": str(label), "features": features})

    return {
        "upload_id":  upload_id,
        "model_kind": model_kind,
        "table":      source_table,
        "rows":       rows,
    }


@router.post("/save")
def save(
    req: SaveMappingRequest,
    db:  Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    upload = db.query(Upload).filter(Upload.upload_id == req.upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")

    existing = db.query(ColumnMapping).filter(
        ColumnMapping.upload_id  == req.upload_id,
        ColumnMapping.model_kind == req.model_kind,
        ColumnMapping.is_active  == True,
    ).all()
    for m in existing:
        m.is_active = False

    row = ColumnMapping(
        upload_id  = req.upload_id,
        model_kind = req.model_kind,
        mapping    = req.mapping,
        is_active  = True,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "status":     "saved",
        "id":         row.id,
        "upload_id":  row.upload_id,
        "model_kind": row.model_kind,
        "mapping":    row.mapping,
    }


@router.get("/{upload_id}/{model_kind}")
def get_saved(upload_id: int, model_kind: str, db: Session = Depends(get_db)):
    row = db.query(ColumnMapping).filter(
        ColumnMapping.upload_id  == upload_id,
        ColumnMapping.model_kind == model_kind,
        ColumnMapping.is_active  == True,
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="No active mapping found")
    return {
        "id":         row.id,
        "upload_id":  row.upload_id,
        "model_kind": row.model_kind,
        "mapping":    row.mapping,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.post("/predict")
def predict_with_mapping(
    req: PredictWithMappingRequest,
    db:  Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row = db.query(ColumnMapping).filter(
        ColumnMapping.upload_id  == req.upload_id,
        ColumnMapping.model_kind == req.model_kind,
        ColumnMapping.is_active  == True,
    ).first()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No mapping saved for upload {req.upload_id} + model {req.model_kind}."
        )

    model_features = apply_mapping(req.user_features, row.mapping, req.model_kind)

    try:
        from app.services.inference_service import (
            predict_revenue, predict_churn, predict_growth
        )
    except ImportError:
        raise HTTPException(status_code=503, detail="ML inference service unavailable")

    uid = current_user.id
    try:
        if req.model_kind == "revenue":
            value  = float(predict_revenue(model_features, user_id=uid, db=db))
            result = {"prediction": round(value, 2), "unit": "USD", "calibrated": True}
        elif req.model_kind == "churn":
            prob   = float(predict_churn(model_features, user_id=uid, db=db))
            risk   = "high" if prob >= 0.7 else "medium" if prob >= 0.4 else "low"
            result = {"churn_probability": round(prob, 4), "risk_level": risk, "calibrated": True}
        elif req.model_kind == "growth":
            value  = float(predict_growth(model_features, user_id=uid, db=db))
            result = {"forecast_3m": round(value, 2), "unit": "USD", "calibrated": True}
        else:
            raise HTTPException(status_code=400, detail=f"Unknown model: {req.model_kind}")
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Inference failed: {e}")

    pred = Prediction(
        user_id   = current_user.id,
        upload_id = req.upload_id,
        kind      = req.model_kind,
        features  = {"user_input": req.user_features, "mapped": model_features},
        result    = result,
    )
    db.add(pred)
    db.commit()
    db.refresh(pred)

    return {
        "prediction_id":  pred.id,
        "model_kind":     req.model_kind,
        "result":         result,
        "features_used":  model_features,
        "mapping_used":   row.mapping,
        "created_at":     pred.created_at.isoformat(),
    }


# ══════════════════════════════════════════════════════
#  BATCH PREDICTION HELPERS
# ══════════════════════════════════════════════════════

def _read_table(engine: Engine, table_name: str) -> pd.DataFrame:
    return pd.read_sql_table(table_name, con=engine)


def _save_batch_summary(
    db:        Session,
    user_id:   int,
    upload_id: int,
    kind:      str,
    summary:   dict,
):
    pred = Prediction(
        user_id   = user_id,
        upload_id = upload_id,
        kind      = f"{kind}_batch",
        features  = {"batch_size": summary.get("n_rows") or summary.get("n_customers") or 0},
        result    = summary,
    )
    db.add(pred)
    db.commit()
    db.refresh(pred)
    return pred


def _prophet_forecast(monthly_df: pd.DataFrame, horizon: int = 12) -> list:
    """
    Fit Prophet on the user's monthly series and forecast `horizon` months
    forward from the last observed month.

    monthly_df must have columns ['month_key', 'monthly_revenue'].
    Returns [{month: 'YYYY-MM', predicted_revenue: float}, ...]
    Returns [] if <6 points or Prophet fails.
    """
    try:
        from prophet import Prophet
    except ImportError:
        return []

    if monthly_df is None or monthly_df.empty:
        return []
    if len(monthly_df) < 6:
        # Not enough history to fit a seasonal model
        return []

    df = monthly_df.copy()
    df["month_key"] = pd.to_datetime(df["month_key"], errors="coerce")
    df = df.dropna(subset=["month_key"]).sort_values("month_key")

    prophet_input = pd.DataFrame({
        "ds": df["month_key"],
        "y":  pd.to_numeric(df["monthly_revenue"], errors="coerce"),
    }).dropna()

    if len(prophet_input) < 6:
        return []

    try:
        model = Prophet(
            yearly_seasonality="auto",
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.80,
        )
        # Suppress Prophet's stdout chatter during fit
        import logging
        logging.getLogger("prophet").setLevel(logging.WARNING)
        logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

        model.fit(prophet_input)

        future = model.make_future_dataframe(periods=horizon, freq="MS")
        forecast = model.predict(future)

        # Keep only the forward rows (exclude the fitted historical portion)
        last_hist_date = prophet_input["ds"].max()
        forward = forecast[forecast["ds"] > last_hist_date].head(horizon)

        return [
            {
                "month":             d.strftime("%Y-%m"),
                "predicted_revenue": round(float(v), 2),
            }
            for d, v in zip(forward["ds"], forward["yhat"])
        ]
    except Exception:
        import traceback
        traceback.print_exc()
        return []


# ══════════════════════════════════════════════════════
#  BATCH: REVENUE  — per-row predictions + Prophet forward forecast
# ══════════════════════════════════════════════════════

@router.post("/predict-batch/revenue/{upload_id}")
def batch_predict_revenue(
    upload_id:    int,
    db:           Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if not upload.table_name:
        raise HTTPException(status_code=400, detail="No cleaned table for this upload")

    mapping_row = db.query(ColumnMapping).filter(
        ColumnMapping.upload_id  == upload_id,
        ColumnMapping.model_kind == "revenue",
        ColumnMapping.is_active  == True,
    ).first()
    if not mapping_row:
        raise HTTPException(
            status_code=404,
            detail="No mapping saved for this upload + revenue. Save one first."
        )

    try:
        df = _read_table(_engine, upload.table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read cleaned table: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Cleaned table is empty")

    try:
        from app.services.inference_service import predict_revenue as _pred_rev
    except ImportError:
        raise HTTPException(status_code=503, detail="ML inference unavailable")

    # Step 1: per-row revenue prediction (calibrated for this user if a
    # calibrator has been fitted via /api/v1/calibrate/revenue/{upload_id})
    predictions: List[float] = []
    errors = 0
    uid = current_user.id
    for _, row in df.iterrows():
        try:
            raw = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            mapped = apply_mapping(raw, mapping_row.mapping, "revenue")
            predictions.append(float(_pred_rev(mapped, user_id=uid, db=db)))
        except Exception:
            errors += 1
            predictions.append(0.0)

    df = df.copy()
    df["_predicted_revenue"] = predictions

    # Step 2: monthly aggregation of per-row predictions (historical)
    date_source = None
    for feat, src in mapping_row.mapping.items():
        if isinstance(src, str) and src.startswith("_from_date:"):
            date_source = src.replace("_from_date:", "")
            break

    monthly_actual = []
    monthly_for_prophet = None
    if date_source and date_source in df.columns:
        try:
            df[date_source] = pd.to_datetime(df[date_source], errors="coerce")
            df = df.dropna(subset=[date_source])
            m = (
                df.assign(month=df[date_source].dt.to_period("M").dt.to_timestamp())
                  .groupby("month")["_predicted_revenue"]
                  .sum()
                  .sort_index()
            )
            monthly_actual = [
                {"month": k.strftime("%Y-%m"), "predicted_revenue": round(float(v), 2)}
                for k, v in m.items()
            ]
            monthly_for_prophet = pd.DataFrame({
                "month_key":       m.index,
                "monthly_revenue": m.values,
            })
        except Exception:
            monthly_actual = []

    # Step 3: Prophet forward forecast from the user's own monthly series
    forward_forecast = []
    forecast_note = None
    if monthly_for_prophet is not None and len(monthly_for_prophet) >= 6:
        forward_forecast = _prophet_forecast(monthly_for_prophet, horizon=12)
        if not forward_forecast:
            forecast_note = "Forecast unavailable (Prophet fit failed)."
    else:
        n = 0 if monthly_for_prophet is None else len(monthly_for_prophet)
        forecast_note = (
            f"Forward forecast requires ≥6 months of data; found {n}. "
            f"Forecast skipped."
        )

    total_predicted   = round(float(sum(predictions)), 2)
    avg_row_predicted = round(float(sum(predictions) / max(len(predictions), 1)), 2)

    summary = {
        "n_rows":             len(predictions),
        "errors":             errors,
        "total_predicted":    total_predicted,
        "avg_row_predicted":  avg_row_predicted,
        "monthly_actual":     monthly_actual,
        "forward_forecast":   forward_forecast,
        "forecast_note":      forecast_note,
        "unit":               "USD",
    }

    pred = _save_batch_summary(db, current_user.id, upload_id, "revenue", summary)

    return {
        "prediction_id": pred.id,
        "model_kind":    "revenue",
        "summary":       summary,
        "created_at":    pred.created_at.isoformat(),
    }


# ══════════════════════════════════════════════════════
#  BATCH: CHURN — static probability + recency-adjusted "next 90 days"
# ══════════════════════════════════════════════════════

def _recency_adjusted_churn(base_prob: float, row: dict) -> float:
    """
    Blend the static trained probability with a recency heuristic to
    produce a 'probability the customer churns in the next 90 days' score.

    If avg_days_between_orders is much less than customer_tenure_days,
    the customer is active — reduce risk slightly.
    If they haven't ordered in 3x their typical cadence, they're likely
    already drifting — raise risk.
    """
    try:
        cadence = float(row.get("avg_days_between_orders") or 0)
        tenure  = float(row.get("customer_tenure_days") or 0)
        if cadence <= 0 or tenure <= 0:
            return base_prob

        # Heuristic: ratio of tenure to cadence → higher is healthier
        health = min(tenure / (cadence * 3), 2.0)  # cap at 2
        # Dampen probability when health > 1, amplify when < 0.5
        if health > 1.0:
            return base_prob * 0.85  # active, slightly lower risk
        elif health < 0.5:
            return min(base_prob * 1.15, 0.999)  # at risk, slightly higher
        return base_prob
    except Exception:
        return base_prob


@router.post("/predict-batch/churn/{upload_id}")
def batch_predict_churn(
    upload_id:    int,
    db:           Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if not upload.customer_table_name:
        raise HTTPException(
            status_code=404,
            detail="No customer aggregate exists for this upload."
        )

    mapping_row = db.query(ColumnMapping).filter(
        ColumnMapping.upload_id  == upload_id,
        ColumnMapping.model_kind == "churn",
        ColumnMapping.is_active  == True,
    ).first()
    if not mapping_row:
        raise HTTPException(
            status_code=404,
            detail="No mapping saved for this upload + churn. Save one first."
        )

    try:
        df = _read_table(_engine, upload.customer_table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read customer table: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Customer aggregate is empty")

    try:
        from app.services.inference_service import predict_churn as _pred_churn
    except ImportError:
        raise HTTPException(status_code=503, detail="ML inference unavailable")

    results: List[dict] = []
    errors = 0
    uid = current_user.id

    for _, row in df.iterrows():
        cust_id = row.get("customer_id", "?")
        raw_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        try:
            mapped   = apply_mapping(raw_dict, mapping_row.mapping, "churn")
            base_prob = float(_pred_churn(mapped, user_id=uid, db=db))
            prob_90d  = _recency_adjusted_churn(base_prob, raw_dict)
        except Exception:
            errors += 1
            base_prob = 0.0
            prob_90d  = 0.0

        risk = "high" if prob_90d >= 0.7 else "medium" if prob_90d >= 0.4 else "low"
        results.append({
            "customer_id":         str(cust_id),
            "churn_probability":   round(prob_90d, 4),
            "base_probability":    round(base_prob, 4),
            "risk_level":          risk,
            "horizon_days":        90,
        })

    results.sort(key=lambda r: r["churn_probability"], reverse=True)

    counts = {"high": 0, "medium": 0, "low": 0}
    for r in results:
        counts[r["risk_level"]] += 1

    summary = {
        "n_customers":   len(results),
        "errors":        errors,
        "risk_counts":   counts,
        "rows":          results,
        "horizon_label": "Next 90 days",
    }

    pred = _save_batch_summary(db, current_user.id, upload_id, "churn", summary)

    return {
        "prediction_id": pred.id,
        "model_kind":    "churn",
        "summary":       summary,
        "created_at":    pred.created_at.isoformat(),
    }


# ══════════════════════════════════════════════════════
#  BATCH: GROWTH — actual + Prophet forward forecast from user's data
# ══════════════════════════════════════════════════════

@router.post("/predict-batch/growth/{upload_id}")
def batch_predict_growth(
    upload_id:    int,
    db:           Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if not upload.monthly_table_name:
        raise HTTPException(
            status_code=404,
            detail="No monthly aggregate exists for this upload."
        )

    try:
        df = _read_table(_engine, upload.monthly_table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read monthly table: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Monthly aggregate is empty")

    actual_series: List[dict] = []
    if "month_key" in df.columns and "monthly_revenue" in df.columns:
        try:
            d = df.copy()
            d["month_key"] = pd.to_datetime(d["month_key"], errors="coerce")
            d = d.dropna(subset=["month_key"]).sort_values("month_key")
            actual_series = [
                {
                    "month":          r["month_key"].strftime("%Y-%m"),
                    "actual_revenue": round(float(r["monthly_revenue"]), 2),
                }
                for _, r in d.iterrows()
            ]
        except Exception:
            actual_series = []

    # Forward forecast via Prophet on the user's own monthly data
    forward_forecast: List[dict] = []
    forecast_note = None
    if "month_key" in df.columns and "monthly_revenue" in df.columns and len(df) >= 6:
        forward_forecast = _prophet_forecast(df, horizon=12)
        if not forward_forecast:
            forecast_note = "Forecast unavailable (Prophet fit failed)."
    else:
        forecast_note = (
            f"Forward forecast requires ≥6 months of data; found {len(df)}. "
            f"Forecast skipped."
        )

    summary = {
        "n_months_actual":   len(actual_series),
        "n_months_forecast": len(forward_forecast),
        "actual":            actual_series,
        "forecast":          forward_forecast,
        "forecast_note":     forecast_note,
        "unit":               "USD",
    }

    pred = _save_batch_summary(db, current_user.id, upload_id, "growth", summary)

    return {
        "prediction_id": pred.id,
        "model_kind":    "growth",
        "summary":       summary,
        "created_at":    pred.created_at.isoformat(),
    }