"""
app/services/column_mapping_service.py

Maps user-uploaded CSV column names to the features your trained ML models
expect. Does three jobs:

  1. suggest_mapping()   — auto-suggest mappings based on column name heuristics
                           (runs after upload so the UI can show a review form)
  2. apply_mapping()     — rename a user's feature dict to the ML model's schema
                           (runs at predict time)
  3. required_features() — list the features each model needs

ML model feature contracts (from optimaai_ml_trainer_v3.py):

  REVENUE:  quantity_sold, price, discount_percent, seasonal_index,
            gross_margin_pct, month, quarter
  CHURN:    customer_tenure_days, total_orders, total_spent, avg_order_value,
            avg_days_between_orders, return_rate, total_items, avg_rating_given,
            unique_categories, spend_per_day, predicted_clv,
            customer_segment, preferred_region
  GROWTH:   monthly_revenue, lag_1_rev, lag_2_rev, mom_growth, yoy_growth,
            3m_rolling_avg, month_num, quarter_num
"""
from typing import Optional
from datetime import datetime
import pandas as pd


# ══════════════════════════════════════════════════════
#  Required features by model
# ══════════════════════════════════════════════════════

REQUIRED = {
    "revenue": [
        "quantity_sold", "price", "discount_percent",
        "seasonal_index", "gross_margin_pct", "month", "quarter",
    ],
    "churn": [
        "customer_tenure_days", "total_orders", "total_spent",
        "avg_order_value", "avg_days_between_orders", "return_rate",
        "total_items", "avg_rating_given", "unique_categories",
        "spend_per_day", "predicted_clv",
    ],
    "growth": [
        "monthly_revenue", "lag_1_rev", "lag_2_rev",
        "mom_growth", "yoy_growth",
    ],
}


# ══════════════════════════════════════════════════════
#  Synonym dictionary — extend over time as users upload more data
# ══════════════════════════════════════════════════════
SYNONYMS = {
    # Revenue model
    "quantity_sold":     ["qty", "quantity", "units", "units_sold", "count", "volume", "items_sold"],
    "price":             ["unit_price", "price_each", "order_amount", "amount", "sale_price", "item_price", "cost"],
    "discount_percent":  ["discount", "discount_pct", "disc", "promotion_percent", "disc_percent"],
    "seasonal_index":    ["season_idx", "seasonality", "season"],
    "gross_margin_pct":  ["margin", "margin_percent", "gross_margin", "profit_margin"],
    "month":             ["month_num", "order_month"],
    "quarter":           ["qtr", "fiscal_quarter"],

    # Churn model
    "customer_tenure_days":   ["tenure", "days_as_customer", "account_age_days", "customer_age"],
    "total_orders":           ["order_count", "num_orders", "orders", "total_purchases"],
    "total_spent":            ["lifetime_value", "total_revenue", "total_sales", "revenue", "amount_spent"],
    "avg_order_value":        ["aov", "average_order", "avg_basket"],
    "avg_days_between_orders":["order_frequency_days", "days_between", "purchase_frequency"],
    "return_rate":            ["returns_rate", "return_pct", "returns_percent"],
    "total_items":            ["items_purchased", "total_units", "sku_count"],
    "avg_rating_given":       ["rating", "avg_rating", "mean_rating", "review_score"],
    "unique_categories":      ["category_count", "num_categories", "categories"],
    "spend_per_day":          ["daily_spend", "avg_daily_spend"],
    "predicted_clv":          ["clv", "lifetime_value", "customer_value"],

    # Growth model
    "monthly_revenue":  ["revenue", "sales", "monthly_sales", "total_revenue"],
    "lag_1_rev":        ["prev_month_revenue", "last_month_revenue"],
    "lag_2_rev":        ["revenue_2_months_ago"],
    "mom_growth":       ["month_over_month", "mom", "monthly_growth"],
    "yoy_growth":       ["year_over_year", "yoy", "annual_growth"],
}


# Columns that might hold dates — used to auto-derive month/quarter
DATE_COLUMN_HINTS = [
    "date", "order_date", "purchase_date", "created_at", "timestamp",
    "transaction_date", "invoice_date", "month_label",
]


def _normalize(col: str) -> str:
    """Lowercase + strip spaces/punctuation for matching."""
    return "".join(c.lower() if c.isalnum() else "_" for c in str(col)).strip("_")


# ══════════════════════════════════════════════════════
#  1. SUGGEST MAPPING
# ══════════════════════════════════════════════════════

def suggest_mapping(user_columns: list, model_kind: str) -> dict:
    """
    Given a list of user CSV columns, suggest mappings for the given model.

    Returns:
      {
        "mapping":     {expected_feature: user_column_or_null},
        "confidence":  {expected_feature: 0.0-1.0},
        "unmapped":    [user_columns_not_matched],
        "missing":     [features_with_no_suggestion],
      }
    """
    if model_kind not in REQUIRED:
        return {"error": f"Unknown model kind: {model_kind}"}

    user_cols     = [str(c) for c in user_columns]
    user_norm     = {_normalize(c): c for c in user_cols}
    mapping       = {}
    confidence    = {}

    for feature in REQUIRED[model_kind]:
        match, score = _find_best_match(feature, user_cols, user_norm)
        mapping[feature]    = match
        confidence[feature] = score

    # Month/quarter can be derived from any date-looking column
    date_col = _find_date_column(user_cols)
    if date_col:
        if "month" in mapping and mapping["month"] is None:
            mapping["month"]   = f"_from_date:{date_col}"
            confidence["month"] = 0.9
        if "quarter" in mapping and mapping["quarter"] is None:
            mapping["quarter"]   = f"_from_date:{date_col}"
            confidence["quarter"] = 0.9

    used = {v for v in mapping.values() if v and not str(v).startswith("_from_date")}
    return {
        "model_kind":  model_kind,
        "mapping":     mapping,
        "confidence":  confidence,
        "unmapped":    [c for c in user_cols if c not in used],
        "missing":     [f for f, v in mapping.items() if v is None],
    }


def _find_best_match(feature: str, user_cols: list, user_norm: dict):
    """Exact match → synonym match → fuzzy substring match."""
    feat_norm = _normalize(feature)

    # 1. Exact normalized match
    if feat_norm in user_norm:
        return user_norm[feat_norm], 1.0

    # 2. Synonym match
    for syn in SYNONYMS.get(feature, []):
        syn_norm = _normalize(syn)
        if syn_norm in user_norm:
            return user_norm[syn_norm], 0.9
        for u_norm, u_orig in user_norm.items():
            if syn_norm in u_norm or u_norm in syn_norm:
                return u_orig, 0.75

    # 3. Fuzzy substring match on the feature name itself
    for u_norm, u_orig in user_norm.items():
        if feat_norm in u_norm or u_norm in feat_norm:
            return u_orig, 0.6

    return None, 0.0


def _find_date_column(user_cols: list) -> Optional[str]:
    for col in user_cols:
        norm = _normalize(col)
        if any(hint in norm for hint in DATE_COLUMN_HINTS):
            return col
    return None


# ══════════════════════════════════════════════════════
#  2. APPLY MAPPING  (runs at predict time)
# ══════════════════════════════════════════════════════

def apply_mapping(user_features: dict, mapping: dict, model_kind: str) -> dict:
    """
    Translate a row of user data (keyed by user-column-names) into the
    feature dict the ML model expects.

    user_features:  {"OrderAmount": 299, "Qty": 3, "OrderDate": "2024-06-15"}
    mapping:        {"price": "OrderAmount", "quantity_sold": "Qty", "month": "_from_date:OrderDate"}
    model_kind:     "revenue"

    Returns a new dict with keys matching REQUIRED[model_kind].
    Missing features get a neutral default (0 or the mean).
    """
    out = {}
    for feature in REQUIRED.get(model_kind, []):
        src = mapping.get(feature)
        if not src:
            out[feature] = _default_for(feature)
            continue

        if isinstance(src, str) and src.startswith("_from_date:"):
            date_col = src.replace("_from_date:", "")
            dt = user_features.get(date_col)
            out[feature] = _extract_date_part(feature, dt)
            continue

        val = user_features.get(src)
        out[feature] = _coerce(val, feature)

    return out


def _coerce(val, feature: str):
    if val is None or val == "":
        return _default_for(feature)
    try:
        return float(val)
    except (TypeError, ValueError):
        return _default_for(feature)


def _extract_date_part(feature: str, dt) -> float:
    if dt is None:
        return _default_for(feature)
    try:
        if isinstance(dt, str):
            dt = pd.to_datetime(dt, errors="coerce")
        if pd.isna(dt):
            return _default_for(feature)
        if feature == "month":
            return float(dt.month)
        if feature == "quarter":
            return float((dt.month - 1) // 3 + 1)
    except Exception:
        pass
    return _default_for(feature)


def _default_for(feature: str) -> float:
    """Reasonable neutral default when a feature is missing."""
    defaults = {
        "quantity_sold":      1,
        "price":              0,
        "discount_percent":   0,
        "seasonal_index":     1.0,
        "gross_margin_pct":   0.3,
        "month":              datetime.utcnow().month,
        "quarter":            (datetime.utcnow().month - 1) // 3 + 1,
        "customer_tenure_days": 180,
        "return_rate":        0.05,
        "avg_rating_given":   4.0,
    }
    return float(defaults.get(feature, 0))


# ══════════════════════════════════════════════════════
#  3. UTILITIES
# ══════════════════════════════════════════════════════

def required_features(model_kind: str) -> list:
    return REQUIRED.get(model_kind, [])


def all_model_kinds() -> list:
    return list(REQUIRED.keys())
