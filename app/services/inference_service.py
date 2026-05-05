"""
app/services/inference_service.py
Loads trained ML artefacts and exposes predict functions.

Two-stage prediction (Option C — hybrid):
  1. Global model.predict(features)        — trained on Amazon-style data
  2. (optional) User calibrator.transform  — fitted on the user's own history
The calibrator is opt-in: pass user_id + db to the predict_* functions.
If the user has no active calibrator the raw global prediction is returned.
"""
import os
import sys
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.ml_bridge import load_latest_evaluation
from app.services.calibration_service import (
    apply_calibrator, load_active_calibrator,
)

_import_error: Exception | None = None
OptimaAiInferenceV3 = None

for _module_path in ("ml.optimaai_ml_trainer_v3", "ml.ml.optimaai_ml_trainer_v3"):
    try:
        _mod = __import__(_module_path, fromlist=["OptimaAiInferenceV3"])
        OptimaAiInferenceV3 = _mod.OptimaAiInferenceV3
        print(f"  [inference] Trainer module found at: {_module_path}")
        break
    except ImportError as e:
        _import_error = e

_inference = None


def get_inference():
    global _inference
    if OptimaAiInferenceV3 is None:
        raise RuntimeError(
            f"ML trainer module could not be imported. Last error: {_import_error}. "
            f"Expected file at: ml/optimaai_ml_trainer_v3.py"
        )
    if _inference is None:
        artefact_dir = os.path.join(
            os.path.dirname(__file__), "..", "..", "optimaai_artefacts"
        )
        _inference = OptimaAiInferenceV3(artefact_dir=artefact_dir)
        print("  [inference] Models loaded.")
    return _inference


def _to_float(v, fallback: float) -> float:
    try:
        if v is None or v == "":
            return fallback
        return float(v)
    except (TypeError, ValueError):
        return fallback


def _pad_revenue(features: dict, required: list) -> dict:
    out = dict(features)
    price  = _to_float(out.get("price"),              0.0)
    qty    = _to_float(out.get("quantity_sold"),      1.0)
    disc   = _to_float(out.get("discount_percent"),   0.0)
    margin = _to_float(out.get("gross_margin_pct"),   0.3)

    margin_frac = margin / 100.0 if margin > 1 else margin
    discounted  = price * (1.0 - disc / 100.0)
    cost        = price * (1.0 - margin_frac)

    now = datetime.utcnow()

    defaults = {
        "quantity_sold":    qty,
        "price":            price,
        "discount_percent": disc,
        "seasonal_index":   _to_float(out.get("seasonal_index"), 1.0),
        "gross_margin_pct": margin,
        "month":            _to_float(out.get("month"),   now.month),
        "quarter":          _to_float(out.get("quarter"), (now.month - 1) // 3 + 1),
        "year":             _to_float(out.get("year"),         now.year),
        "week_of_year":     _to_float(out.get("week_of_year"), now.isocalendar()[1]),
        "day_of_week":      _to_float(out.get("day_of_week"),  now.weekday()),
        "is_weekend":       _to_float(out.get("is_weekend"),   1 if now.weekday() >= 5 else 0),
        "product_category": 0,
        "customer_region":  0,
        "payment_method":   0,
        "discounted_price": discounted,
        "cost_of_goods":    cost,
        "is_returned":      0,
        "refund_amount":    0,
        "rating":           4.0,
        "review_count":     5,
        # Lag/rolling default to 0 (was: current sale_amount, which caused
        # the tree model to lock onto its own input). Per-user calibrator
        # corrects the resulting bias.
        "revenue_lag_7d":   0.0,
        "revenue_lag_30d":  0.0,
        "revenue_roll_7d":  0.0,
        "revenue_roll_30d": 0.0,
        "revenue_roll_90d": 0.0,
        "orders_roll_7d":   0,
    }

    for feat in required:
        if feat not in out or out[feat] is None or out[feat] == "":
            out[feat] = defaults.get(feat, 0)
    return out


def _pad_zero(features: dict, required: list) -> dict:
    out = dict(features)
    for feat in required:
        if feat not in out or out[feat] is None or out[feat] == "":
            out[feat] = 0
        else:
            out[feat] = _to_float(out[feat], 0)
    return out


def _maybe_calibrate(
    raw_value: float,
    model_kind: str,
    user_id: Optional[int],
    db: Optional[Session],
) -> float:
    if user_id is None or db is None:
        return raw_value
    estimator = load_active_calibrator(db, user_id, model_kind)
    return apply_calibrator(estimator, model_kind, raw_value)


def predict_revenue(
    features: dict,
    user_id: Optional[int] = None,
    db: Optional[Session]  = None,
) -> float:
    inf = get_inference()
    if not inf or not inf.rev_model:
        raise RuntimeError("Revenue model not loaded.")
    padded = _pad_revenue(features, inf.rev_feats)
    raw    = float(inf.predict_revenue(padded))
    return _maybe_calibrate(raw, "revenue", user_id, db)


def predict_churn(
    features: dict,
    user_id: Optional[int] = None,
    db: Optional[Session]  = None,
) -> float:
    inf = get_inference()
    if not inf or not inf.churn_model:
        raise RuntimeError("Churn model not loaded.")
    padded = _pad_zero(features, inf.churn_feats)
    raw    = float(inf.predict_churn(padded))
    return _maybe_calibrate(raw, "churn", user_id, db)


def predict_growth(
    features: dict,
    user_id: Optional[int] = None,
    db: Optional[Session]  = None,
) -> float:
    inf = get_inference()
    if not inf or not inf.growth_model:
        raise RuntimeError("Growth model not loaded.")
    padded = _pad_zero(features, inf.growth_feats)
    raw    = float(inf.predict_growth(padded))
    return _maybe_calibrate(raw, "growth", user_id, db)


def get_latest_kpis() -> dict:
    return load_latest_evaluation()