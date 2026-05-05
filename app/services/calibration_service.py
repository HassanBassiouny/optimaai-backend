"""
app/services/calibration_service.py

Per-user prediction calibrators (Option C — hybrid).
"""
from __future__ import annotations

import io
import math
from typing import Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LinearRegression
from sqlalchemy.orm import Session

from app.database import UserCalibrator


_CACHE: dict[Tuple[int, str], object] = {}


def _cache_key(user_id: int, model_kind: str) -> Tuple[int, str]:
    return (int(user_id), str(model_kind))


def invalidate_cache(user_id: int, model_kind: Optional[str] = None) -> None:
    if model_kind is None:
        for k in list(_CACHE.keys()):
            if k[0] == int(user_id):
                _CACHE.pop(k, None)
    else:
        _CACHE.pop(_cache_key(user_id, model_kind), None)


def _dump(estimator) -> bytes:
    buf = io.BytesIO()
    joblib.dump(estimator, buf)
    return buf.getvalue()


def _load(blob: bytes):
    return joblib.load(io.BytesIO(blob))


MIN_SAMPLES_REGRESSION = 30
MIN_SAMPLES_CLASSIFICATION = 50


def _mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def fit_regression_calibrator(
    base_preds: np.ndarray,
    actuals: np.ndarray,
) -> Tuple[object, str, dict]:
    base_preds = np.asarray(base_preds, dtype=float).ravel()
    actuals    = np.asarray(actuals,    dtype=float).ravel()

    mask = np.isfinite(base_preds) & np.isfinite(actuals)
    base_preds = base_preds[mask]
    actuals    = actuals[mask]

    n = len(base_preds)
    if n < MIN_SAMPLES_REGRESSION:
        raise ValueError(
            f"Need at least {MIN_SAMPLES_REGRESSION} (base_pred, actual) pairs "
            f"to fit a regression calibrator; got {n}."
        )

    mae_before = _mae(actuals, base_preds)

    if n >= 100 and len(np.unique(base_preds)) >= 10:
        est = IsotonicRegression(out_of_bounds="clip")
        est.fit(base_preds, actuals)
        kind = "isotonic"
        calibrated = est.predict(base_preds)
    else:
        est = LinearRegression()
        est.fit(base_preds.reshape(-1, 1), actuals)
        kind = "linear"
        calibrated = est.predict(base_preds.reshape(-1, 1))

    mae_after = _mae(actuals, calibrated)
    metrics = {
        "n_samples":  int(n),
        "mae_before": round(mae_before, 4),
        "mae_after":  round(mae_after, 4),
        "improvement_pct": round(
            100.0 * (mae_before - mae_after) / max(mae_before, 1e-9), 2
        ),
    }
    return est, kind, metrics


def fit_classification_calibrator(
    base_probs: np.ndarray,
    labels: np.ndarray,
) -> Tuple[object, str, dict]:
    base_probs = np.asarray(base_probs, dtype=float).ravel()
    labels     = np.asarray(labels,     dtype=float).ravel()

    mask = np.isfinite(base_probs) & np.isfinite(labels)
    base_probs = np.clip(base_probs[mask], 0.0, 1.0)
    labels     = labels[mask]

    n = len(base_probs)
    if n < MIN_SAMPLES_CLASSIFICATION:
        raise ValueError(
            f"Need at least {MIN_SAMPLES_CLASSIFICATION} (prob, label) pairs "
            f"to fit a churn calibrator; got {n}."
        )

    pos = labels.sum()
    if pos == 0 or pos == n:
        raise ValueError(
            "Cannot fit churn calibrator: the provided labels are all 0 or all 1. "
            "Need both churned and active customers in the calibration data."
        )

    brier_before = float(np.mean((base_probs - labels) ** 2))

    est = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    est.fit(base_probs, labels)
    calibrated = est.predict(base_probs)
    brier_after = float(np.mean((calibrated - labels) ** 2))

    metrics = {
        "n_samples":          int(n),
        "positive_rate":      round(float(pos) / n, 4),
        "brier_before":       round(brier_before, 4),
        "brier_after":        round(brier_after, 4),
        "improvement_pct":    round(
            100.0 * (brier_before - brier_after) / max(brier_before, 1e-9), 2
        ),
    }
    return est, "isotonic", metrics


def save_calibrator(
    db: Session,
    user_id: int,
    model_kind: str,
    upload_id: Optional[int],
    estimator,
    kind: str,
    metrics: dict,
) -> UserCalibrator:
    db.query(UserCalibrator).filter(
        UserCalibrator.user_id    == user_id,
        UserCalibrator.model_kind == model_kind,
        UserCalibrator.is_active  == True,
    ).update({"is_active": False})

    row = UserCalibrator(
        user_id         = user_id,
        upload_id       = upload_id,
        model_kind      = model_kind,
        calibrator_kind = kind,
        blob            = _dump(estimator),
        n_samples       = int(metrics.get("n_samples", 0)),
        metrics         = metrics,
        is_active       = True,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    invalidate_cache(user_id, model_kind)
    return row


def load_active_calibrator(
    db: Session,
    user_id: int,
    model_kind: str,
):
    key = _cache_key(user_id, model_kind)
    if key in _CACHE:
        return _CACHE[key]

    row = db.query(UserCalibrator).filter(
        UserCalibrator.user_id    == user_id,
        UserCalibrator.model_kind == model_kind,
        UserCalibrator.is_active  == True,
    ).order_by(UserCalibrator.created_at.desc()).first()

    if row is None:
        _CACHE[key] = None
        return None

    est = _load(row.blob)
    _CACHE[key] = est
    return est


def apply_calibrator(estimator, model_kind: str, raw_value: float) -> float:
    if estimator is None:
        return raw_value
    try:
        x = np.asarray([raw_value], dtype=float)
        if isinstance(estimator, IsotonicRegression):
            y = estimator.predict(x)
        else:
            y = estimator.predict(x.reshape(-1, 1))
        out = float(y[0])
        if not math.isfinite(out):
            return raw_value
        if model_kind == "churn":
            out = max(0.0, min(1.0, out))
        elif model_kind in ("revenue", "growth"):
            out = max(0.0, out)
        return out
    except Exception:
        return raw_value


def collect_pairs_revenue(
    df: pd.DataFrame,
    target_column: str,
    mapping: dict,
    predict_fn,
    apply_mapping_fn,
    sample_cap: int = 5000,
) -> Tuple[np.ndarray, np.ndarray]:
    if target_column not in df.columns:
        raise ValueError(
            f"Target column '{target_column}' not found in cleaned table."
        )

    if len(df) > sample_cap:
        df = df.sample(sample_cap, random_state=42).reset_index(drop=True)

    base_preds: list[float] = []
    actuals:    list[float] = []

    for _, row in df.iterrows():
        actual = row.get(target_column)
        if actual is None or (isinstance(actual, float) and math.isnan(actual)):
            continue
        try:
            actual_f = float(actual)
        except (TypeError, ValueError):
            continue

        raw = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        try:
            mapped = apply_mapping_fn(raw, mapping, "revenue")
            pred   = float(predict_fn(mapped))
        except Exception:
            continue

        base_preds.append(pred)
        actuals.append(actual_f)

    return np.asarray(base_preds), np.asarray(actuals)


def collect_pairs_churn(
    df: pd.DataFrame,
    target_column: str,
    mapping: dict,
    predict_fn,
    apply_mapping_fn,
    sample_cap: int = 5000,
) -> Tuple[np.ndarray, np.ndarray]:
    if target_column not in df.columns:
        raise ValueError(
            f"Target column '{target_column}' not found in customer table."
        )

    if len(df) > sample_cap:
        df = df.sample(sample_cap, random_state=42).reset_index(drop=True)

    base_probs: list[float] = []
    labels:     list[int]   = []

    for _, row in df.iterrows():
        actual = row.get(target_column)
        if actual is None or (isinstance(actual, float) and math.isnan(actual)):
            continue
        try:
            label = int(float(actual))
            if label not in (0, 1):
                label = 1 if float(actual) >= 0.5 else 0
        except (TypeError, ValueError):
            continue

        raw = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        try:
            mapped = apply_mapping_fn(raw, mapping, "churn")
            prob   = float(predict_fn(mapped))
        except Exception:
            continue

        base_probs.append(prob)
        labels.append(label)

    return np.asarray(base_probs), np.asarray(labels)