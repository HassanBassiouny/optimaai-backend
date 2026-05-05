"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         OPTIMAAI ML TRAINER  v3  —  FULL IMPROVEMENT                       ║
║                                                                              ║
║  What's new vs v2:                                                           ║
║  [1] Each model trains on its OWN purpose-built dataset                     ║
║      → Revenue  : amazon_revenue_forecasting.csv   (50 000 rows, 32 cols)  ║
║      → Churn    : amazon_churn_prediction.csv      (10 842 customers)       ║
║      → Growth   : amazon_growth_projection.csv     (24 monthly points)      ║
║  [2] Revenue — net_revenue target (after refunds), seasonal_index,          ║
║      gross_margin_pct, return/refund flags, daily rolling lags              ║
║  [3] Churn — REAL churn labels (is_churned), RFM features, CLV,             ║
║      churn_risk_score, customer_segment, 65 % base rate handled             ║
║  [4] Growth — monthly aggregates, MoM / YoY growth, 3-month rolling avg,   ║
║      iterative 12-month forward forecast with confidence band               ║
║  [5] Prophet — trained on clean monthly net_revenue from revenue CSV        ║
║  [6] Iterative forecast for revenue model (lag feedback loop) - FIXED       ║
║  [7] v3.1 — Reads transparency reports from churn_labeler, revenue_profiler ║
║      and growth_profiler. Skips models honestly when data is insufficient.  ║
║      Each report flows into self.results → evaluation_results.json so the   ║
║      LLM that writes the executive summary has the EXACT definition that    ║
║      was applied to the user's data.                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

FOLDER STRUCTURE EXPECTED:
    optimaai-backend/
    ├── ml/
    │   ├── optimaai_ml_trainer_v3.py   ← this file
    │   └── train.py                    ← entry point
    ├── data/
    │   ├── amazon_revenue_forecasting.csv
    │   ├── amazon_churn_prediction.csv
    │   ├── amazon_growth_projection.csv
    │   └── amazon_sales_ml_ready.xlsx  (optional — full enriched set)
    └── optimaai_artefacts/             ← models saved here

HOW TO RUN:
    python ml/train.py
    — or directly —
    python ml/optimaai_ml_trainer_v3.py
"""

import os, sys, json, warnings, pickle, zipfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── optional imports ───────────────────────────────────────────────────────────
try:
    import joblib; HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False

try:
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import (
        mean_squared_error, mean_absolute_error, r2_score,
        roc_auc_score, f1_score, classification_report,
        precision_score, recall_score,
    )
except ImportError:
    print("[ERROR] scikit-learn not found.  pip install scikit-learn")
    sys.exit(1)

try:
    import xgboost as xgb; HAS_XGB = True
except ImportError:
    HAS_XGB = False
    from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
    print("[INFO] XGBoost not found — using GradientBoosting (identical API).")
    print("       Install XGBoost for production:  pip install xgboost")

try:
    from prophet import Prophet; HAS_PROPHET = True
except ImportError:
    try:
        from prophet import Prophet; HAS_PROPHET = True
    except ImportError:
        HAS_PROPHET = False
        print("[WARNING] Prophet not installed — time-series model skipped.")
        print("          pip install prophet")


# ══════════════════════════════════════════════════════
#  TERMINAL COLOURS
# ══════════════════════════════════════════════════════
class C:
    G="\033[92m"; Y="\033[93m"; R="\033[91m"
    B="\033[94m"; BOLD="\033[1m"; END="\033[0m"

def log(msg, level="INFO"):
    col  = {"INFO":C.B,"SUCCESS":C.G,"WARNING":C.Y,"ERROR":C.R,"STEP":C.BOLD+C.B}.get(level,"")
    icon = {"INFO":"•","SUCCESS":"✔","WARNING":"⚠","ERROR":"✖","STEP":"▶"}.get(level,"•")
    print(f"{col}{icon}  {msg}{C.END}")


# ══════════════════════════════════════════════════════
#  TRAINER
# ══════════════════════════════════════════════════════
class OptimaAiMLTrainerV3:

    # ── default data file paths (override via constructor) ─────────────────────
    DEFAULT_PATHS = {
        "revenue" : "data/amazon_revenue_forecasting.csv",
        "churn"   : "data/amazon_churn_prediction.csv",
        "growth"  : "data/amazon_growth_projection.csv",
    }

    def __init__(
        self,
        artefact_dir : str  = "optimaai_artefacts",
        data_paths   : dict = None,
        run_gridsearch: bool = False,
    ):
        self.artefact_dir   = artefact_dir
        self.paths          = {**self.DEFAULT_PATHS, **(data_paths or {})}
        self.run_gridsearch = run_gridsearch
        self.version        = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results        = {}
        self.label_encoders = {}
        os.makedirs(self.artefact_dir, exist_ok=True)

    # ──────────────────────────────────────────────────
    #  ENTRY POINT
    # ──────────────────────────────────────────────────
    def run(self):
        self._banner("OPTIMAAI ML TRAINER v3 — START")
        self._train_revenue_model()
        self._train_churn_model()
        self._train_growth_model()
        # Prophet disabled due to poor performance with limited data
        # if HAS_PROPHET:
        #     self._train_prophet_model()
        self._save_artefacts()
        self._print_summary()
        return self.results

    # ══════════════════════════════════════════════════
    #  MODEL 1 — REVENUE FORECASTING
    #  Dataset : amazon_revenue_forecasting.csv
    #  Target  : net_revenue  (revenue after refunds)
    # ══════════════════════════════════════════════════
    def _train_revenue_model(self):
        self._section("MODEL 1 — Revenue Forecasting  [net_revenue target]")

        # ── transparency report (from revenue_profiler) ───
        data_profile = self._load_report("revenue", "revenue_data_report.json")
        if data_profile and data_profile.get("strategy") == "insufficient_data":
            log("  Revenue model SKIPPED — profiler reported insufficient data.", "WARNING")
            self.results["revenue_model"] = {
                "status": "skipped_insufficient_data",
                "reason": data_profile.get("definition"),
                "data_profile": data_profile,
            }
            return

        # ── load ──────────────────────────────────────
        df = self._load_csv("revenue")
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        df = df.sort_values("order_date").reset_index(drop=True)
        log(f"  Loaded {len(df):,} rows | {df['order_date'].min().date()} → {df['order_date'].max().date()}", "INFO")

        # ── fill lag NAs with forward/backward fill ───
        for col in ["revenue_lag_7d","revenue_lag_30d","revenue_yoy_growth"]:
            if col in df.columns:
                df[col] = df[col].bfill().fillna(0)

        # ── encode categoricals ───────────────────────
        cat_cols = ["product_category","customer_region","payment_method"]
        for col in cat_cols:
            if col in df.columns:
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))
                self.label_encoders[f"rev_{col}"] = le

        # Features used to PREDICT net_revenue.
        # Anything computed from or simultaneously with revenue is excluded —
        # those are leakage features and inflate R² to ~1.0.
        FEATURES = [
            # Date decomposition (always known in advance)
            "year", "month", "quarter", "week_of_year", "day_of_week",
            "is_weekend", "seasonal_index",
            # Product / transaction (known at order time, BEFORE outcome)
            "product_category", "customer_region", "payment_method",
            "price", "discount_percent", "quantity_sold", "discounted_price",
            "rating", "review_count",
            # Lag / rolling — historical revenue signal, the legitimate way
            # to give the model temporal context
            "revenue_lag_7d", "revenue_lag_30d",
            "revenue_roll_7d", "revenue_roll_30d", "revenue_roll_90d",
            "orders_roll_7d",
        ]

        # EXCLUDED as target leakage (do not re-add):
        #   refund_amount    — directly subtracted to get net_revenue
        #   is_returned      — flag controlling whether refund applies
        #   cost_of_goods    — co-determined with revenue at sale time
        #   gross_margin_pct — derived as (revenue - cost) / revenue

        TARGET = "net_revenue"

        # keep only features that actually exist in this file
        feats = [f for f in FEATURES if f in df.columns]
        log(f"  Features used: {len(feats)}", "INFO")

        X = df[feats].fillna(0)
        y = df[TARGET]

        # ── temporal split — use ratio from profiler when available ──
        split_ratio = (data_profile or {}).get("train_test_split_ratio") or 0.80
        split = int(len(df) * split_ratio)
        X_tr, y_tr = X.iloc[:split], y.iloc[:split]
        X_te, y_te = X.iloc[split:],  y.iloc[split:]
        log(f"  Train: {len(X_tr):,}  |  Test: {len(X_te):,}  (split={split_ratio:.0%})", "INFO")

        sc = StandardScaler()
        X_tr_s = sc.fit_transform(X_tr)
        X_te_s  = sc.transform(X_te)

        # ── train ─────────────────────────────────────
        model = self._reg(n_estimators=300, max_depth=6, learning_rate=0.08)
        if HAS_XGB:
            model.fit(X_tr_s, y_tr, eval_set=[(X_te_s, y_te)], verbose=False)
        else:
            model.fit(X_tr_s, y_tr)

        yp = model.predict(X_te_s)

        rmse  = float(np.sqrt(mean_squared_error(y_te, yp)))
        mae   = float(mean_absolute_error(y_te, yp))
        r2    = float(r2_score(y_te, yp))
        mape  = self._mape(y_te, yp)
        smape = self._smape(y_te, yp)
        bias  = float(np.mean(yp - np.array(y_te)))

        log(f"  RMSE          : {rmse:>12,.4f}", "INFO")
        log(f"  MAE           : {mae:>12,.4f}",  "INFO")
        log(f"  MAPE          : {mape:>11.2f}%", "INFO")
        log(f"  sMAPE         : {smape:>11.2f}%","INFO")
        log(f"  R²            : {r2:>12.6f}",   "INFO")
        log(f"  Forecast Bias : {bias:>12,.4f}", "INFO")

        # ── iterative 12-period forecast (FIXED) ──────────────
        forecast_vals = self._iterative_forecast_fixed(
            model, sc, df, feats, TARGET, n_steps=12
        )
        log(f"  12-period forecast: {[round(v,2) for v in forecast_vals]}", "SUCCESS")

        # ── feature importance ────────────────────────
        imp = self._top_features(model, feats)

        # ── store ─────────────────────────────────────
        self.rev_model  = model
        self.rev_scaler = sc
        self.rev_feats  = feats

        self.results["revenue_model"] = {
            "RMSE": round(rmse,4), "MAE": round(mae,4),
            "MAPE_pct": round(mape,2), "sMAPE_pct": round(smape,2),
            "R2": round(r2,6), "forecast_bias": round(bias,4),
            "target": "net_revenue (after refunds)",
            "train_rows": len(X_tr), "test_rows": len(X_te),
            "features_used": len(feats),
            "12_period_forecast": [round(v,2) for v in forecast_vals],
            "top_features": imp,
            "data_profile": data_profile,
        }

    # ══════════════════════════════════════════════════
    #  MODEL 2 — CHURN PREDICTION
    #  Dataset : amazon_churn_prediction.csv
    #  Target  : is_churned  (90-day window, real labels)
    # ══════════════════════════════════════════════════
    def _train_churn_model(self):
        self._section("MODEL 2 — Churn Prediction  [real is_churned labels]")

        # ── transparency report (from churn_labeler) ─────
        # Read BEFORE _load_csv — when the labeller refuses, no CSV exists.
        labelling = self._load_report("churn", "churn_label_report.json")
        if labelling and labelling.get("strategy") == "insufficient_data":
            log("  Churn model SKIPPED — labeller reported insufficient data.", "WARNING")
            self.results["churn_model"] = {
                "status": "skipped_insufficient_data",
                "reason": labelling.get("definition"),
                "labelling": labelling,
            }
            return

        df = self._load_csv("churn")
        log(f"  Loaded {len(df):,} customers | Churn rate: {df['is_churned'].mean()*100:.1f}%", "INFO")

        # Remove leaking features if they exist
        leaking_cols = ['days_since_last_order', 'churn_risk_score']
        for col in leaking_cols:
            if col in df.columns:
                df = df.drop(columns=[col])
                log(f"  Removed leaking feature: {col}", "INFO")

        # ── encode customer_segment + region ──────────
        for col in ["customer_segment","preferred_region"]:
            if col in df.columns:
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))
                self.label_encoders[f"churn_{col}"] = le

        # ── feature set (without leaking features) ───
        FEATURES = [
            # RFM core
            "customer_tenure_days","total_orders","total_spent",
            "avg_order_value","avg_days_between_orders",
            # Behavioural
            "return_rate","total_items","avg_rating_given",
            "unique_categories","spend_per_day",
            # Value
            "predicted_clv",
            # Categorical (encoded)
            "customer_segment","preferred_region",
        ]
        TARGET = "is_churned"

        feats = [f for f in FEATURES if f in df.columns]
        log(f"  Features used: {len(feats)}", "INFO")

        # Shuffle for classification (no temporal dependency here)
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
        split = int(len(df) * 0.80)

        X_tr = df[feats].iloc[:split].fillna(0)
        y_tr = df[TARGET].iloc[:split]
        X_te = df[feats].iloc[split:].fillna(0)
        y_te = df[TARGET].iloc[split:]
        log(f"  Train: {len(X_tr):,}  |  Test: {len(X_te):,}", "INFO")

        sc = StandardScaler()
        X_tr_s = sc.fit_transform(X_tr)
        X_te_s  = sc.transform(X_te)

        # ── class weight ──────────────────────────────
        neg, pos = (y_tr==0).sum(), (y_tr==1).sum()
        spw = float(neg/pos) if pos > 0 else 1.0
        log(f"  Class balance → 0:{neg:,}  1:{pos:,}  spw:{spw:.2f}", "INFO")

        model = self._clf(spw=spw)
        if HAS_XGB:
            model.fit(X_tr_s, y_tr, eval_set=[(X_te_s,y_te)], verbose=False)
        else:
            model.fit(X_tr_s, y_tr)

        pp = model.predict_proba(X_te_s)[:,1]
        yp = (pp >= 0.5).astype(int)

        roc  = float(roc_auc_score(y_te, pp))
        f1   = float(f1_score(y_te, yp, zero_division=0))
        prec = float(precision_score(y_te, yp, zero_division=0))
        rec  = float(recall_score(y_te, yp, zero_division=0))

        log(f"  ROC-AUC   : {roc:.4f}",  "INFO")
        log(f"  F1-Score  : {f1:.4f}",   "INFO")
        log(f"  Precision : {prec:.4f}",  "INFO")
        log(f"  Recall    : {rec:.4f}",   "INFO")
        print(classification_report(y_te, yp, zero_division=0,
              target_names=["Active","Churned"]))

        # Churn risk breakdown by segment
        df_test = df.iloc[split:].copy()
        df_test["churn_prob"] = pp
        if "customer_segment" in df.columns:
            log("  Churn prob by encoded segment (0=Loyal,1=New,2=Regular,3=VIP approx):", "INFO")
            seg_col = "customer_segment"
            if seg_col in df_test.columns:
                seg_risk = df_test.groupby(seg_col)["churn_prob"].mean().sort_values(ascending=False)
                for seg, prob in seg_risk.items():
                    bar = "█" * int(prob * 30)
                    print(f"    Segment {seg}: {bar}  {prob:.2%}")

        imp = self._top_features(model, feats)

        self.churn_model  = model
        self.churn_scaler = sc
        self.churn_feats  = feats

        self.results["churn_model"] = {
            "ROC_AUC": round(roc,4), "F1_Score": round(f1,4),
            "Precision": round(prec,4), "Recall": round(rec,4),
            "churn_rate_pct": round(df["is_churned"].mean()*100, 2),
            "train_rows": len(X_tr), "test_rows": len(X_te),
            "features_used": len(feats),
            "top_features": imp,
            "labelling": labelling,
        }

# ══════════════════════════════════════════════════
#  MODEL 3 — GROWTH PROJECTION
#  Dataset : amazon_growth_projection.csv
#  Target  : revenue_3m_forecast  (absolute $)
# ══════════════════════════════════════════════════
    def _train_growth_model(self):
        self._section("MODEL 3 — Growth Projection  [3-month forward revenue]")

        # ── transparency report (from growth_profiler) ────
        data_profile = self._load_report("growth", "growth_data_report.json")
        if data_profile and data_profile.get("strategy") == "insufficient_data":
            log("  Growth model SKIPPED — profiler reported insufficient data.", "WARNING")
            self.results["growth_model"] = {
                "status": "skipped_insufficient_data",
                "reason": data_profile.get("definition"),
                "data_profile": data_profile,
            }
            return

        df = self._load_csv("growth")
        df["month_label"] = pd.to_datetime(df["month_label"], errors="coerce")
        df = df.sort_values("month_label").reset_index(drop=True)
        log(f"  Loaded {len(df)} monthly rows | "
            f"{df['month_label'].min().date()} → {df['month_label'].max().date()}", "INFO")

        # ── fill NAs ──────────────────────────────────
        df["mom_growth"] = df["mom_growth"].fillna(0)
        df["yoy_growth"] = df["yoy_growth"].fillna(0)

        # ── lag features ──────────────────────────────
        df["lag_1_rev"]  = df["monthly_revenue"].shift(1).fillna(df["monthly_revenue"].mean())
        df["lag_2_rev"]  = df["monthly_revenue"].shift(2).fillna(df["monthly_revenue"].mean())
        df["lag_3_rev"]  = df["monthly_revenue"].shift(3).fillna(df["monthly_revenue"].mean())
        df["lag_12_rev"] = df["monthly_revenue"].shift(12).fillna(df["monthly_revenue"].mean())
        df["month_num"]  = df["month_label"].dt.month
        df["year_num"]   = df["month_label"].dt.year

        # Fourier terms for seasonality (sin/cos)
        df["sin_m"] = np.sin(2 * np.pi * df["month_num"] / 12)
        df["cos_m"] = np.cos(2 * np.pi * df["month_num"] / 12)

        # Features used to PREDICT 3-month-forward revenue.
        # EXCLUDED as leakage / quasi-leakage:
        #   monthly_revenue   — this month's revenue is part of the target window
        #   revenue_3m_avg    — overlapping computation with revenue_3m_forecast
        #   mom_growth        — derived from monthly_revenue and lag_1_rev (redundant
        #                       and dominated the model in the leaky version)
        FEATURES = [
            # Historical revenue signal — proper way to give the model temporal context
            "lag_1_rev", "lag_2_rev", "lag_3_rev", "lag_12_rev",
            # Customer / order activity
            "monthly_orders", "new_customers", "avg_order_value",
            # YoY (computed from lag_12, less leaky than mom_growth)
            "yoy_growth",
            # Seasonality
            "month_num", "sin_m", "cos_m",
        ]
        TARGET = "revenue_3m_forecast"

        feats = [f for f in FEATURES if f in df.columns]
        log(f"  Features used: {len(feats)}", "INFO")

        # ── temporal split — use profile-recommended sizes when available ─
        if data_profile:
            holdout = data_profile.get("recommended_test_months") or 4
            holdout = min(holdout, max(1, len(df) // 4))   # never more than 25% of data
        else:
            holdout = 4
        train = df.iloc[:-holdout]
        test  = df.iloc[-holdout:]
        log(f"  Train: {len(train)} months  |  Test: {len(test)} months", "INFO")

        X_tr, y_tr = train[feats].fillna(0), train[TARGET]
        X_te, y_te = test[feats].fillna(0),  test[TARGET]

        sc = StandardScaler()
        X_tr_s = sc.fit_transform(X_tr)
        X_te_s = sc.transform(X_te)

        # Lighter model — small dataset, avoid overfitting
        model = self._reg(n_estimators=100, max_depth=3, learning_rate=0.05)
        if HAS_XGB:
            model.fit(X_tr_s, y_tr, eval_set=[(X_te_s, y_te)], verbose=False)
        else:
            model.fit(X_tr_s, y_tr)

        yp   = model.predict(X_te_s)
        rmse = float(np.sqrt(mean_squared_error(y_te, yp)))
        mae  = float(mean_absolute_error(y_te, yp))
        r2   = float(r2_score(y_te, yp))
        mape = self._mape(y_te, yp)

        log(f"  RMSE : {rmse:>12,.2f}", "INFO")
        log(f"  MAE  : {mae:>12,.2f}",  "INFO")
        log(f"  MAPE : {mape:>11.2f}%", "INFO")
        log(f"  R²   : {r2:>12.4f}",   "INFO")

        # ── iterative forward forecast ────────────────
        # Horizon comes from the profiler — it knows how much data exists and
        # what's defensible. Without a report, fall back to the v3 heuristic.
        if data_profile:
            HORIZON = int(data_profile.get("safe_horizon_months") or 6)
            log(f"  Forecast horizon: {HORIZON} months (from profiler, "
                f"strategy='{data_profile.get('strategy')}')", "INFO")
        else:
            HORIZON = min(6, max(1, len(train) // 3))
            log(f"  Forecast horizon: {HORIZON} months "
                f"(no profile report — using fallback heuristic)", "INFO")

        df_fc     = df.copy()
        last_i    = len(df_fc) - 1
        fc_vals   = []
        fc_months = []

        for step in range(HORIZON):
            row  = df_fc.iloc[last_i]
            Xrow = pd.DataFrame([row[feats].fillna(0)])
            pred = float(model.predict(sc.transform(Xrow))[0])
            fc_vals.append(round(pred, 2))

            # Build synthetic next-month row, updating ALL temporal features
            new = row.copy()
            last_date = df_fc.iloc[last_i]["month_label"]
            next_date = last_date + pd.DateOffset(months=1)
            fc_months.append(str(next_date)[:7])

            new["month_label"]      = next_date
            new["month_num"]        = next_date.month
            new["year_num"]         = next_date.year
            new["sin_m"]            = np.sin(2 * np.pi * next_date.month / 12)
            new["cos_m"]            = np.cos(2 * np.pi * next_date.month / 12)

            # Use the predicted target as next month's "monthly_revenue" proxy
            # (so the lag chain keeps working as we walk forward)
            new["monthly_revenue"]  = pred

            # All four lag features now updated (lag_12 was being missed before)
            new["lag_1_rev"]  = df_fc.iloc[last_i]["monthly_revenue"]
            new["lag_2_rev"]  = df_fc.iloc[max(0, last_i - 1)]["monthly_revenue"]
            new["lag_3_rev"]  = df_fc.iloc[max(0, last_i - 2)]["monthly_revenue"]
            new["lag_12_rev"] = df_fc.iloc[max(0, last_i - 11)]["monthly_revenue"]

            # Recompute YoY using the updated lag_12 (was stale before)
            prev_year = df_fc.iloc[max(0, last_i - 11)]["monthly_revenue"]
            new["yoy_growth"] = (pred - prev_year) / max(1, abs(prev_year))

            new["revenue_3m_forecast"] = pred
            df_fc = pd.concat([df_fc, new.to_frame().T], ignore_index=True)
            last_i += 1

        log(f"  {HORIZON}-Month Growth Forecast:", "SUCCESS")
        for m, v in zip(fc_months, fc_vals):
            print(f"    {m}   ${v:>12,.2f}")

        imp = self._top_features(model, feats)

        self.growth_model  = model
        self.growth_scaler = sc
        self.growth_feats  = feats

        self.results["growth_model"] = {
            "RMSE":          round(rmse, 2),
            "MAE":           round(mae, 2),
            "MAPE_pct":      round(mape, 2),
            "R2":            round(r2, 4),
            "target":        "revenue_3m_forecast (absolute $)",
            "train_months":  len(train),
            "test_months":   len(test),
            "features_used": len(feats),
            "forecast_horizon_months": HORIZON,
            "forecast": [   # honest naming — length = HORIZON, not always 12
                {"period": m, "forecast": v}
                for m, v in zip(fc_months, fc_vals)
            ],
            "12_month_forecast": [          # legacy alias — kept for backward compat
                {"period": m, "forecast": v}
                for m, v in zip(fc_months, fc_vals)
            ],
            "top_features": imp,
            "data_profile": data_profile,
        }

    # ══════════════════════════════════════════════════
    #  MODEL 4 — PROPHET TIME-SERIES (DISABLED BY DEFAULT)
    # ══════════════════════════════════════════════════
    def _train_prophet_model(self):
        self._section("MODEL 4 — Prophet Time-Series  [IQR-clipped monthly]")

        if not HAS_PROPHET:
            log("  Prophet not installed — skipping.", "WARNING")
            self.results["prophet_model"] = {"status": "skipped"}
            return

        df = self._load_csv("revenue")
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        # Monthly net_revenue aggregation
        mts = (
            df[["order_date","net_revenue"]].dropna()
            .set_index("order_date")
            .resample("ME")["net_revenue"].sum()
            .reset_index()
        )
        mts.columns = ["ds","y"]

        if len(mts) < 6:
            log("  Fewer than 6 monthly points — skipped.", "WARNING")
            self.results["prophet_model"] = {"status": "skipped — too few points"}
            return

        # IQR clip
        Q1, Q3 = mts["y"].quantile(0.25), mts["y"].quantile(0.75)
        IQR = Q3 - Q1
        mts["y"] = mts["y"].clip(lower=Q1-1.5*IQR, upper=Q3+1.5*IQR)
        log(f"  Monthly points: {len(mts)} | IQR range: [{Q1-1.5*IQR:,.0f} – {Q3+1.5*IQR:,.0f}]", "INFO")

        holdout  = 3
        train_df = mts.iloc[:-holdout]
        test_df  = mts.iloc[-holdout:]

        prophet = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            seasonality_mode="multiplicative",
            interval_width=0.80,
            changepoint_prior_scale=0.05,
        )
        prophet.fit(train_df)

        fut  = prophet.make_future_dataframe(periods=holdout, freq="ME")
        fc   = prophet.predict(fut).tail(holdout)
        yt, yp = test_df["y"].values, fc["yhat"].values

        rmse = float(np.sqrt(mean_squared_error(yt, yp)))
        mae  = float(mean_absolute_error(yt, yp))
        mape = self._mape(yt, yp)

        log(f"  Holdout RMSE : {rmse:>12,.2f}", "INFO")
        log(f"  Holdout MAE  : {mae:>12,.2f}",  "INFO")
        log(f"  Holdout MAPE : {mape:>11.2f}%", "INFO")

        # 12-month forward
        fut12 = prophet.make_future_dataframe(periods=12, freq="ME")
        fc12  = prophet.predict(fut12).tail(12)
        fc_list = [
            {"period":   str(r["ds"])[:7],
             "forecast": round(r["yhat"], 2),
             "lower_80": round(r["yhat_lower"], 2),
             "upper_80": round(r["yhat_upper"], 2)}
            for _, r in fc12.iterrows()
        ]
        log("  12-Month Prophet Forecast:", "SUCCESS")
        for e in fc_list:
            print(f"    {e['period']}   ${e['forecast']:>12,.2f}"
                  f"  [{e['lower_80']:>12,.2f}  –  {e['upper_80']:>12,.2f}]")

        self.prophet_model = prophet
        self.results["prophet_model"] = {
            "RMSE": round(rmse,2), "MAE": round(mae,2),
            "MAPE_pct": round(mape,2),
            "12_month_forecast": fc_list,
        }

    # ══════════════════════════════════════════════════
    #  FIXED ITERATIVE FORECAST FOR REVENUE MODEL
    # ══════════════════════════════════════════════════
    def _iterative_forecast_fixed(self, model, scaler, df, feats, target, n_steps=12):
        """
        Fixed multi-step iterative forecast with proper feature updating.
        This creates a dynamic forecast where each prediction influences the next.
        """
        df_fc = df.copy()
        fc_vals = []
        
        # Get the last row as starting point
        last_row = df_fc.iloc[-1:].copy()
        
        # Define seasonal index mapping
        seasonal_base = {1:0.75, 2:0.78, 3:0.85, 4:0.90, 5:0.92, 6:0.88,
                         7:0.87, 8:0.91, 9:0.95, 10:1.00, 11:1.25, 12:1.30}
        
        for step in range(n_steps):
            # Prepare features for prediction
            X_pred = last_row[feats].fillna(0)
            
            # Scale and predict
            X_scaled = scaler.transform(X_pred)
            pred = float(model.predict(X_scaled)[0])
            fc_vals.append(pred)
            
            # Create next row by updating time-dependent features
            next_row = last_row.copy()
            
            # Update target with prediction
            next_row[target] = pred
            
            # Update lag and rolling features
            if 'revenue_lag_7d' in feats:
                next_row['revenue_lag_7d'] = last_row[target].values[0] if step > 0 else last_row[target].values[0]
            if 'revenue_lag_30d' in feats:
                next_row['revenue_lag_30d'] = last_row[target].values[0] if step > 0 else last_row[target].values[0]
            if 'revenue_roll_7d' in feats:
                # Use previous prediction for rolling average
                next_row['revenue_roll_7d'] = pred
            if 'revenue_roll_30d' in feats:
                next_row['revenue_roll_30d'] = pred
            if 'revenue_roll_90d' in feats:
                next_row['revenue_roll_90d'] = pred
            
            # Update time-based features
            if 'year' in feats and 'month' in feats:
                # Increment month for next prediction
                current_year = int(next_row['year'].values[0])
                current_month = int(next_row['month'].values[0])
                
                # Calculate next month/year
                if current_month == 12:
                    next_month = 1
                    next_year = current_year + 1
                else:
                    next_month = current_month + 1
                    next_year = current_year
                
                next_row['year'] = next_year
                next_row['month'] = next_month
                
                if 'quarter' in feats:
                    next_row['quarter'] = (next_month - 1) // 3 + 1
                if 'seasonal_index' in feats:
                    next_row['seasonal_index'] = seasonal_base.get(next_month, 1.0)
                if 'week_of_year' in feats:
                    # Approximate - in production you'd calculate properly
                    next_row['week_of_year'] = min(52, int((next_month - 1) * 4.33) + 1)
                if 'day_of_week' in feats:
                    next_row['day_of_week'] = 0  # Reset to Monday as approximation
            
            last_row = next_row
        
        return fc_vals

    # ══════════════════════════════════════════════════
    #  SAVE ARTEFACTS
    # ══════════════════════════════════════════════════
    def _save_artefacts(self):
        self._section("Saving artefacts")
        saved = []

        def sav(obj, name):
            ext  = "joblib" if HAS_JOBLIB else "pkl"
            path = f"{self.artefact_dir}/{name}_v{self.version}.{ext}"
            if HAS_JOBLIB:
                joblib.dump(obj, path)
            else:
                with open(path, "wb") as f:
                    pickle.dump(obj, f)
            log(f"  {name}", "SUCCESS")
            saved.append(path)

        if hasattr(self,"rev_model"):
            sav(self.rev_model,  "revenue_model")
            sav(self.rev_scaler, "revenue_scaler")
            sav(self.rev_feats,  "revenue_feature_list")

        if hasattr(self,"churn_model"):
            sav(self.churn_model,  "churn_model")
            sav(self.churn_scaler, "churn_scaler")
            sav(self.churn_feats,  "churn_feature_list")

        if hasattr(self,"growth_model"):
            sav(self.growth_model,  "growth_model")
            sav(self.growth_scaler, "growth_scaler")
            sav(self.growth_feats,  "growth_feature_list")

        if hasattr(self,"prophet_model"):
            pp = f"{self.artefact_dir}/prophet_model_v{self.version}.pkl"
            with open(pp, "wb") as f:
                pickle.dump(self.prophet_model, f)
            log("  prophet_model", "SUCCESS")
            saved.append(pp)

        if self.label_encoders:
            sav(self.label_encoders, "label_encoders")

        manifest = {
            "version":        self.version,
            "trainer_version":"v3.1",
            "trained_at":     datetime.now().isoformat(),
            "data_files":     self.paths,
            "models":         list(self.results.keys()),
            "improvements": [
                "dedicated_datasets_per_model",
                "net_revenue_target_with_refunds",
                "adaptive_churn_labelling_with_transparency_report",
                "growth_uses_3m_forward_revenue",
                "iterative_lag_feedback_forecast_fixed",
                "prophet_iqr_outlier_clipping",
                "seasonal_index_feature",
                "gross_margin_pct_feature",
                "clv_and_rfm_churn_features",
                "removed_data_leakage_from_churn",
                "v3.1_revenue_profiler_data_report",
                "v3.1_growth_profiler_safe_horizon",
                "v3.1_skip_models_when_data_insufficient",
                "v3.1_reports_attached_to_results_for_llm",
            ],
        }
        mp = f"{self.artefact_dir}/manifest_v{self.version}.json"
        with open(mp, "w") as f:
            json.dump(manifest, f, indent=4)
        saved.append(mp)

        rp = f"{self.artefact_dir}/evaluation_results_v{self.version}.json"
        with open(rp, "w") as f:
            json.dump(self.results, f, indent=4, default=str)
        saved.append(rp)

        zp = f"{self.artefact_dir}/optimaai_models_v{self.version}.zip"
        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as zf:
            for fp in saved:
                zf.write(fp, arcname=os.path.basename(fp))
        log(f"  ZIP bundle: optimaai_models_v{self.version}.zip", "SUCCESS")

    # ══════════════════════════════════════════════════
    #  FASTAPI INFERENCE WRAPPER
    # ══════════════════════════════════════════════════
    def get_inference(self):
        """Return a ready-to-use inference object after training."""
        return OptimaAiInferenceV3(
            artefact_dir = self.artefact_dir,
            version      = self.version,
        )

    # ══════════════════════════════════════════════════
    #  HELPERS
    # ══════════════════════════════════════════════════
    def _load_csv(self, key):
        path = self.paths[key]
        if not os.path.exists(path):
            log(f"File not found: {path}", "ERROR")
            sys.exit(1)
        ext = Path(path).suffix.lower()
        if ext == ".csv":
            return pd.read_csv(path)
        else:
            return pd.read_excel(path)

    def _load_report(self, csv_key, report_name):
        """
        Read a transparency report sitting next to the model's CSV.

        Looks for `<report_name>` in the same folder as `self.paths[csv_key]`.
        Returns the parsed dict if found, or None (so existing pipelines that
        haven't run the profilers/labeller yet still work).
        """
        report_path = Path(self.paths[csv_key]).with_name(report_name)
        if not report_path.exists():
            log(f"  No transparency report at {report_path} — proceeding without one.", "WARNING")
            return None
        try:
            with open(report_path) as f:
                report = json.load(f)
            log(f"  Loaded report: strategy='{report.get('strategy')}' "
                f"confidence='{report.get('confidence')}'", "INFO")
            return report
        except Exception as e:
            log(f"  Could not parse {report_path}: {e}", "WARNING")
            return None

    def _reg(self, **kw):
        d = dict(n_estimators=300, max_depth=5, learning_rate=0.1,
                 subsample=0.8, colsample_bytree=0.8, random_state=42,
                 objective="reg:squarederror", eval_metric="rmse", verbosity=0)
        if HAS_XGB:
            return xgb.XGBRegressor(**{**d,**kw})
        return GradientBoostingRegressor(
            n_estimators=kw.get("n_estimators", d["n_estimators"]),
            max_depth=kw.get("max_depth", d["max_depth"]),
            learning_rate=kw.get("learning_rate", d["learning_rate"]),
            subsample=kw.get("subsample", d["subsample"]),
            random_state=42
        )

    def _clf(self, spw=1.0):
        d = dict(n_estimators=300, max_depth=4, learning_rate=0.1,
                 subsample=0.8, random_state=42)
        if HAS_XGB:
            return xgb.XGBClassifier(**d, scale_pos_weight=spw,
                                     colsample_bytree=0.8,
                                     objective="binary:logistic",
                                     eval_metric="auc", verbosity=0)
        return GradientBoostingClassifier(**d)

    @staticmethod
    def _mape(yt, yp):
        yt,yp = np.array(yt),np.array(yp)
        m = yt != 0
        if m.sum() == 0:
            return 0.0
        return float(np.mean(np.abs((yt[m]-yp[m])/yt[m]))*100)

    @staticmethod
    def _smape(yt, yp):
        yt,yp = np.array(yt),np.array(yp)
        d = (np.abs(yt) + np.abs(yp)) / 2
        m = d != 0
        if m.sum() == 0:
            return 0.0
        return float(np.mean(np.abs(yt[m]-yp[m])/d[m])*100)

    def _top_features(self, model, feats, n=10):
        imp = sorted(zip(feats, model.feature_importances_), key=lambda x:-x[1])[:n]
        print(f"  Top {n} features:")
        for f,i in imp:
            bar_length = int(i * 50)
            bar = "█" * bar_length if bar_length > 0 else ""
            print(f"    {f:<35} {bar}  {i:.4f}")
        return [{"feature":f,"importance":round(i,4)} for f,i in imp]

    def _print_summary(self):
        self._banner("TRAINING COMPLETE — v3.1 SUMMARY")
        rows = []
        for k, v in self.results.items():
            if not isinstance(v, dict):
                continue
            if v.get("status", "").startswith("skipped"):
                rows.append(f"  {k:<25} SKIPPED  ({v.get('status')})")
            elif "RMSE" in v:
                rows.append(f"  {k:<25} RMSE={v.get('RMSE','-'):>12}  "
                            f"MAPE={v.get('MAPE_pct','-'):>6}%  "
                            f"R²={v.get('R2','-'):>8}")
            elif "ROC_AUC" in v:
                rows.append(f"  {k:<25} ROC-AUC={v['ROC_AUC']:.4f}  "
                            f"F1={v['F1_Score']:.4f}  "
                            f"Precision={v['Precision']:.4f}  "
                            f"Recall={v['Recall']:.4f}")
        for r in rows:
            print(r)
        print()
        log("All available models trained. Artefacts saved. Ready for FastAPI inference.", "SUCCESS")

    def _banner(self, t):
        b = "═" * (len(t) + 4)
        print(f"\n{C.BOLD}{C.B}╔{b}╗\n║  {t}  ║\n╚{b}╝{C.END}\n")

    def _section(self, t):
        print()
        log(t, "STEP")
        print("  " + "─" * 62)


# ══════════════════════════════════════════════════════
#  FASTAPI INFERENCE CLASS
# ══════════════════════════════════════════════════════
class OptimaAiInferenceV3:
    """
    Drop into FastAPI endpoints:

        from optimaai_ml_trainer_v3 import OptimaAiInferenceV3
        inf = OptimaAiInferenceV3("optimaai_artefacts")

        # Revenue prediction for one order
        revenue = inf.predict_revenue({
            "quantity_sold": 3, "price": 299, "discount_percent": 10,
            "seasonal_index": 1.25, "gross_margin_pct": 0.45, ...
        })

        # Churn probability for one customer
        churn_prob = inf.predict_churn({
            "total_orders": 2, "avg_days_between_orders": 60, ...
        })

        # 3-month revenue growth forecast
        growth = inf.predict_growth({
            "monthly_revenue": 1300000, "mom_growth": 0.03,
            "lag_1_rev": 1260000, "sin_m": 0.5, "cos_m": 0.866, ...
        })
    """

    def __init__(self, artefact_dir="optimaai_artefacts", version=None):
        self.artefact_dir = artefact_dir
        self.version = version or self._latest()
        self._load()

    def _latest(self):
        ms = sorted(Path(self.artefact_dir).glob("manifest_v*.json"))
        if not ms:
            raise FileNotFoundError("No manifest in artefact_dir.")
        return ms[-1].stem.replace("manifest_v", "")

    def _load(self):
        ext = "joblib" if HAS_JOBLIB else "pkl"
        
        def ld(name):
            path = f"{self.artefact_dir}/{name}_v{self.version}.{ext}"
            if HAS_JOBLIB:
                return joblib.load(path)
            else:
                with open(path, "rb") as f:
                    return pickle.load(f)
        
        for attr, name in [
            ("rev_model", "revenue_model"), ("rev_scaler", "revenue_scaler"),
            ("rev_feats", "revenue_feature_list"),
            ("churn_model", "churn_model"), ("churn_scaler", "churn_scaler"),
            ("churn_feats", "churn_feature_list"),
            ("growth_model", "growth_model"), ("growth_scaler", "growth_scaler"),
            ("growth_feats", "growth_feature_list"),
        ]:
            try:
                setattr(self, attr, ld(name))
            except:
                setattr(self, attr, None)

    def predict_revenue(self, feat: dict) -> float:
        if not self.rev_model:
            raise RuntimeError("Revenue model not loaded.")
        X = pd.DataFrame([feat])[self.rev_feats].fillna(0)
        return float(self.rev_model.predict(self.rev_scaler.transform(X))[0])

    def predict_churn(self, feat: dict) -> float:
        if not self.churn_model:
            raise RuntimeError("Churn model not loaded.")
        X = pd.DataFrame([feat])[self.churn_feats].fillna(0)
        return float(self.churn_model.predict_proba(self.churn_scaler.transform(X))[0][1])

    def predict_growth(self, feat: dict) -> float:
        if not self.growth_model:
            raise RuntimeError("Growth model not loaded.")
        X = pd.DataFrame([feat])[self.growth_feats].fillna(0)
        return float(self.growth_model.predict(self.growth_scaler.transform(X))[0])


# ══════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="OptimaAi ML Trainer v3")
    parser.add_argument("--revenue",  default="data/amazon_revenue_forecasting.csv")
    parser.add_argument("--churn",    default="data/amazon_churn_prediction.csv")
    parser.add_argument("--growth",   default="data/amazon_growth_projection.csv")
    parser.add_argument("--out",      default="optimaai_artefacts")
    parser.add_argument("--gridsearch", action="store_true")
    args = parser.parse_args()

    trainer = OptimaAiMLTrainerV3(
        artefact_dir  = args.out,
        data_paths    = {"revenue":args.revenue, "churn":args.churn, "growth":args.growth},
        run_gridsearch= args.gridsearch,
    )
    trainer.run()