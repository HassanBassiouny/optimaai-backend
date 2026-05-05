"""
app/routes/calibrate_routes.py

Per-user calibration endpoints (Option C — hybrid generalization).

Flow:
  1. User uploads their historical CSV (existing /upload endpoint).
  2. User saves a column mapping for revenue or churn (existing /mapping/save).
  3. User calls POST /calibrate/{revenue|churn}/{upload_id} pointing at
     the column in their cleaned table that holds the actual value.
  4. The service runs the global model on each row, fits a small calibrator
     on (base_pred, actual) pairs, and stores it keyed by (user_id, model_kind).
  5. Subsequent predict calls that pass user_id will be calibrated.
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import (
    get_db, Upload, ColumnMapping, User, UserCalibrator,
)
from app.services.auth_service       import get_current_user
from app.services.column_mapping_service import apply_mapping
from app.services.calibration_service    import (
    fit_regression_calibrator,
    fit_classification_calibrator,
    save_calibrator,
    invalidate_cache,
    collect_pairs_revenue,
    collect_pairs_churn,
)


router = APIRouter(prefix="/api/v1/calibrate", tags=["Calibration"])


class CalibrateRequest(BaseModel):
    target_column: str
    sample_cap:    Optional[int] = 5000


def _read_table(engine, table_name: str):
    import pandas as pd
    return pd.read_sql_table(table_name, con=engine)


def _get_active_mapping(db: Session, upload_id: int, model_kind: str):
    row = db.query(ColumnMapping).filter(
        ColumnMapping.upload_id  == upload_id,
        ColumnMapping.model_kind == model_kind,
        ColumnMapping.is_active  == True,
    ).first()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No active mapping for upload {upload_id} + {model_kind}. "
                   f"Save one via /api/v1/mapping/save first.",
        )
    return row


@router.post("/revenue/{upload_id}")
def calibrate_revenue(
    upload_id:    int,
    req:          CalibrateRequest,
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if upload.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Upload belongs to another user")
    if not upload.table_name:
        raise HTTPException(status_code=400, detail="No cleaned table for this upload")

    mapping_row = _get_active_mapping(db, upload_id, "revenue")

    from app.database import engine as _engine
    try:
        df = _read_table(_engine, upload.table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read cleaned table: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Cleaned table is empty")

    from app.services.inference_service import predict_revenue as _raw_predict
    def _raw(features):
        return _raw_predict(features, user_id=None, db=None)

    try:
        base_preds, actuals = collect_pairs_revenue(
            df             = df,
            target_column  = req.target_column,
            mapping        = mapping_row.mapping,
            predict_fn     = _raw,
            apply_mapping_fn = apply_mapping,
            sample_cap     = req.sample_cap or 5000,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        est, kind, metrics = fit_regression_calibrator(base_preds, actuals)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    row = save_calibrator(
        db, current_user.id, "revenue", upload_id, est, kind, metrics
    )

    return {
        "status":          "fitted",
        "calibrator_id":   row.id,
        "model_kind":      "revenue",
        "calibrator_kind": kind,
        "metrics":         metrics,
        "created_at":      row.created_at.isoformat(),
    }


@router.post("/churn/{upload_id}")
def calibrate_churn(
    upload_id:    int,
    req:          CalibrateRequest,
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    upload = db.query(Upload).filter(Upload.upload_id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if upload.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Upload belongs to another user")
    if not upload.customer_table_name:
        raise HTTPException(
            status_code=400,
            detail="No customer aggregate exists for this upload",
        )

    mapping_row = _get_active_mapping(db, upload_id, "churn")

    from app.database import engine as _engine
    try:
        df = _read_table(_engine, upload.customer_table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read customer table: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Customer table is empty")

    from app.services.inference_service import predict_churn as _raw_predict
    def _raw(features):
        return _raw_predict(features, user_id=None, db=None)

    try:
        base_probs, labels = collect_pairs_churn(
            df               = df,
            target_column    = req.target_column,
            mapping          = mapping_row.mapping,
            predict_fn       = _raw,
            apply_mapping_fn = apply_mapping,
            sample_cap       = req.sample_cap or 5000,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        est, kind, metrics = fit_classification_calibrator(base_probs, labels)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    row = save_calibrator(
        db, current_user.id, "churn", upload_id, est, kind, metrics
    )

    return {
        "status":          "fitted",
        "calibrator_id":   row.id,
        "model_kind":      "churn",
        "calibrator_kind": kind,
        "metrics":         metrics,
        "created_at":      row.created_at.isoformat(),
    }


@router.get("/status")
def calibrator_status(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    rows: List[UserCalibrator] = db.query(UserCalibrator).filter(
        UserCalibrator.user_id   == current_user.id,
        UserCalibrator.is_active == True,
    ).order_by(UserCalibrator.created_at.desc()).all()

    return {
        "user_id":     current_user.id,
        "calibrators": [
            {
                "id":              r.id,
                "model_kind":      r.model_kind,
                "calibrator_kind": r.calibrator_kind,
                "n_samples":       r.n_samples,
                "metrics":         r.metrics,
                "upload_id":       r.upload_id,
                "created_at":      r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


@router.delete("/{model_kind}")
def delete_calibrator(
    model_kind:   str,
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    if model_kind not in ("revenue", "churn", "growth"):
        raise HTTPException(status_code=400, detail="Unknown model_kind")

    n = db.query(UserCalibrator).filter(
        UserCalibrator.user_id    == current_user.id,
        UserCalibrator.model_kind == model_kind,
        UserCalibrator.is_active  == True,
    ).update({"is_active": False})
    db.commit()
    invalidate_cache(current_user.id, model_kind)

    return {"status": "deleted", "deactivated": int(n), "model_kind": model_kind}