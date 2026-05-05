"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              OPTIMAAI CHURN LABELER  v1                                      ║
║                                                                              ║
║  Purpose ─ produce dataset-adaptive, transparent churn labels for ANY        ║
║            transactional dataset, without hard-coded business assumptions.   ║
║                                                                              ║
║  Why this exists                                                             ║
║  ───────────────                                                             ║
║  Every dataset (SaaS, retail, B2B, services) has its own definition of      ║
║  "inactive". A 90-day rule that fits a SaaS product produces a 65% false    ║
║  churn rate on Amazon retail data — exactly the bug we hit in v3 reports.   ║
║                                                                              ║
║  This module replaces the static rule with a strategy that adapts to the    ║
║  data itself, picks the strongest available method, and writes a plain-     ║
║  English definition into the report so users always know what "churned"     ║
║  meant for THEIR upload.                                                    ║
║                                                                              ║
║  Three strategies, picked automatically:                                    ║
║    A. forward_holdout    — gold standard, needs ≥ 18 months of history     ║
║    B. adaptive_threshold — strong heuristic, needs ≥ 6 months              ║
║    C. insufficient_data  — refuse honestly; report says so                 ║
║                                                                              ║
║  Inputs  : a DataFrame of transactions with customer_id + order_date        ║
║            (optional: amount, category for richer RFM features)             ║
║  Outputs : (labelled_customers_df, LabellingReport)                         ║
║                                                                              ║
║  CLI:                                                                        ║
║    python churn_labeler.py --in data/transactions.csv \\                   ║
║                            --out data/amazon_churn_prediction.csv \\       ║
║                            --report data/churn_label_report.json           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────
#  CONFIG — defendable defaults; tune only with a clear reason
# ─────────────────────────────────────────────────────────────
MIN_DATA_DAYS_FORWARD   = 540      # 18 months → enables forward-holdout
MIN_DATA_DAYS_ADAPTIVE  = 180      # 6 months  → minimum for adaptive
MIN_ELIGIBLE_CUSTOMERS  = 50       # below this any churn model is fragile
ADAPTIVE_THRESHOLD_K    = 3        # X = k × personal_avg_inter_purchase_interval
ADAPTIVE_HARD_CAP_DAYS  = 365      # never wait > 1 year to call someone churned
PLAUSIBILITY_BAND_PCT   = (2, 60)  # warn if churn rate falls outside this %


# ─────────────────────────────────────────────────────────────
#  REPORT — flows into the executive summary so users see
#           the EXACT definition that was applied to their data
# ─────────────────────────────────────────────────────────────
@dataclass
class LabellingReport:
    strategy:           str            # 'forward_holdout' | 'adaptive_threshold' | 'insufficient_data'
    confidence:         str            # 'high' | 'medium' | 'low'
    definition:         str            # plain-English, ready to paste into a report

    data_span_days:     int
    total_customers:    int
    one_time_buyers:    int            # excluded — separate population
    repeat_customers:   int
    new_customers:      int            # excluded — too recent to label
    eligible_customers: int            # the cohort actually labelled
    churned_customers:  int
    churn_rate_pct:     float

    # Strategy-specific (only filled when relevant)
    cutoff_date:          Optional[str]   = None
    holdout_window_days:  Optional[int]   = None
    median_ipi_days:      Optional[float] = None
    threshold_multiplier: Optional[float] = None
    threshold_max_days:   Optional[int]   = None

    warnings_: list = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["warnings"] = d.pop("warnings_")  # cosmetic: drop the trailing _
        return d


# ─────────────────────────────────────────────────────────────
#  THE LABELER
# ─────────────────────────────────────────────────────────────
class ChurnLabeler:
    """
    Generate transparent, dataset-adaptive churn labels.

    Usage
    -----
        labeler = ChurnLabeler(
            customer_col = "customer_id",
            date_col     = "order_date",
            amount_col   = "net_revenue",       # optional — enables RFM features
            category_col = "product_category",  # optional — enables unique_categories
        )
        customers_df, report = labeler.label(transactions_df)

        customers_df.to_csv("data/amazon_churn_prediction.csv", index=False)
        with open("data/churn_label_report.json", "w") as f:
            json.dump(report.to_dict(), f, indent=2)
    """

    def __init__(
        self,
        customer_col: str = "customer_id",
        date_col:     str = "order_date",
        amount_col:   Optional[str] = None,
        category_col: Optional[str] = None,
    ):
        self.customer_col = customer_col
        self.date_col     = date_col
        self.amount_col   = amount_col
        self.category_col = category_col

    # ─── public ──────────────────────────────────────────────
    def label(self, df: pd.DataFrame) -> tuple[pd.DataFrame, LabellingReport]:
        df = self._validate(df)
        if df.empty:
            return self._refuse(0, df)

        span_days = (df[self.date_col].max() - df[self.date_col].min()).days

        if span_days >= MIN_DATA_DAYS_FORWARD:
            return self._forward_holdout(df, span_days)
        if span_days >= MIN_DATA_DAYS_ADAPTIVE:
            return self._adaptive_threshold(df, span_days)
        return self._refuse(span_days, df)

    # ─── input hygiene ───────────────────────────────────────
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in (self.customer_col, self.date_col):
            if col not in df.columns:
                raise ValueError(f"Required column missing: '{col}'")
        df = df.copy()
        df[self.date_col] = pd.to_datetime(df[self.date_col], errors="coerce")
        df = df.dropna(subset=[self.customer_col, self.date_col])
        return df.sort_values(self.date_col).reset_index(drop=True)

    # ─── customer-level RFM scaffolding ──────────────────────
    def _customer_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """One row per customer with the RFM features the trainer expects."""
        agg = {self.date_col: ["min", "max", "count"]}
        if self.amount_col and self.amount_col in df.columns:
            agg[self.amount_col] = ["sum", "mean"]
        if self.category_col and self.category_col in df.columns:
            agg[self.category_col] = pd.Series.nunique

        out = df.groupby(self.customer_col).agg(agg)
        out.columns = ["_".join([c for c in col if c]).strip("_") for col in out.columns]
        out = out.reset_index().rename(columns={
            f"{self.date_col}_min":   "first_order",
            f"{self.date_col}_max":   "last_order",
            f"{self.date_col}_count": "total_orders",
        })

        if self.amount_col and self.amount_col in df.columns:
            out = out.rename(columns={
                f"{self.amount_col}_sum":  "total_spent",
                f"{self.amount_col}_mean": "avg_order_value",
            })
        if self.category_col and self.category_col in df.columns:
            out = out.rename(columns={
                f"{self.category_col}_nunique": "unique_categories",
            })

        out["customer_tenure_days"] = (out["last_order"] - out["first_order"]).dt.days
        out["avg_days_between_orders"] = np.where(
            out["total_orders"] > 1,
            out["customer_tenure_days"] / (out["total_orders"] - 1).clip(lower=1),
            np.nan,
        )
        return out

    # ─── STRATEGY A: forward-looking holdout (gold standard) ─
    def _forward_holdout(self, df, span_days):
        max_date = df[self.date_col].max()
        # Holdout window scales with available data: 90–180 days
        holdout_days = int(min(180, max(90, span_days // 6)))
        cutoff_date  = max_date - pd.Timedelta(days=holdout_days)

        cust       = self._customer_summary(df)
        one_timers = cust["total_orders"] == 1
        # "Too new" — first order so close to cutoff that absence isn't meaningful
        too_new    = cust["first_order"] > (cutoff_date - pd.Timedelta(days=holdout_days))
        eligible   = ~one_timers & ~too_new

        bought_after = (
            df[df[self.date_col] > cutoff_date]
              .groupby(self.customer_col).size().rename("orders_after")
        )
        cust = cust.merge(bought_after, left_on=self.customer_col,
                          right_index=True, how="left")
        cust["orders_after"] = cust["orders_after"].fillna(0)
        cust["days_since_last_order"] = (max_date - cust["last_order"]).dt.days
        cust["is_churned"] = np.nan
        cust.loc[eligible, "is_churned"] = (cust.loc[eligible, "orders_after"] == 0).astype(int)

        labelled   = cust[eligible].copy()
        churn_rate = float(labelled["is_churned"].mean() * 100) if len(labelled) else 0.0

        warns = self._plausibility_warnings(len(labelled), churn_rate)

        report = LabellingReport(
            strategy="forward_holdout",
            confidence="high",
            definition=(
                f"A customer is labelled 'churned' if they had at least one "
                f"order before {cutoff_date.date()} and made no orders during "
                f"the {holdout_days}-day observation window from "
                f"{cutoff_date.date()} to {max_date.date()}. One-time buyers "
                f"and customers acquired too recently to observe a full holdout "
                f"window are excluded from the trained population."
            ),
            data_span_days=span_days,
            total_customers=len(cust),
            one_time_buyers=int(one_timers.sum()),
            repeat_customers=int((~one_timers).sum()),
            new_customers=int((too_new & ~one_timers).sum()),
            eligible_customers=len(labelled),
            churned_customers=int(labelled["is_churned"].sum()),
            churn_rate_pct=round(churn_rate, 2),
            cutoff_date=str(cutoff_date.date()),
            holdout_window_days=holdout_days,
            warnings_=warns,
        )
        return self._finalise(labelled), report

    # ─── STRATEGY B: customer-adaptive threshold ─────────────
    def _adaptive_threshold(self, df, span_days):
        max_date = df[self.date_col].max()
        cust     = self._customer_summary(df)

        one_timers = cust["total_orders"] == 1
        repeats    = cust[~one_timers].copy()

        # Avoid divide-by-zero / unrealistically tight thresholds
        repeats["avg_days_between_orders"] = repeats["avg_days_between_orders"].clip(lower=1)
        repeats["personal_threshold"] = (
            repeats["avg_days_between_orders"] * ADAPTIVE_THRESHOLD_K
        ).clip(upper=ADAPTIVE_HARD_CAP_DAYS)

        repeats["days_since_last_order"] = (max_date - repeats["last_order"]).dt.days

        # Only label customers whose threshold is actually observable in the data
        observable_window = (max_date - repeats["first_order"]).dt.days
        eligible = repeats["personal_threshold"] <= observable_window

        repeats["is_churned"] = np.nan
        repeats.loc[eligible, "is_churned"] = (
            repeats.loc[eligible, "days_since_last_order"]
            > repeats.loc[eligible, "personal_threshold"]
        ).astype(int)

        labelled   = repeats[eligible].copy()
        median_ipi = float(repeats["avg_days_between_orders"].median()) if len(repeats) else 0.0
        churn_rate = float(labelled["is_churned"].mean() * 100) if len(labelled) else 0.0

        warns = self._plausibility_warnings(len(labelled), churn_rate)
        warns.append(
            "Adaptive threshold is a heuristic. Re-train with forward-holdout "
            "as soon as 18+ months of data are available."
        )

        report = LabellingReport(
            strategy="adaptive_threshold",
            confidence="medium",
            definition=(
                f"A customer with multiple orders is labelled 'churned' if their "
                f"days-since-last-order exceeds {ADAPTIVE_THRESHOLD_K}× their "
                f"personal average inter-purchase interval, capped at "
                f"{ADAPTIVE_HARD_CAP_DAYS} days. The median interval across "
                f"all repeat customers in this dataset is {median_ipi:.0f} days. "
                f"One-time buyers and customers whose threshold exceeds the "
                f"observable history are excluded."
            ),
            data_span_days=span_days,
            total_customers=len(cust),
            one_time_buyers=int(one_timers.sum()),
            repeat_customers=len(repeats),
            new_customers=int(len(repeats) - len(labelled)),
            eligible_customers=len(labelled),
            churned_customers=int(labelled["is_churned"].sum()),
            churn_rate_pct=round(churn_rate, 2),
            median_ipi_days=round(median_ipi, 1),
            threshold_multiplier=ADAPTIVE_THRESHOLD_K,
            threshold_max_days=ADAPTIVE_HARD_CAP_DAYS,
            warnings_=warns,
        )
        return self._finalise(labelled), report

    # ─── STRATEGY C: refuse with honesty ─────────────────────
    def _refuse(self, span_days: int, df: pd.DataFrame) -> tuple[pd.DataFrame, LabellingReport]:
        cust = self._customer_summary(df) if not df.empty else pd.DataFrame()
        report = LabellingReport(
            strategy="insufficient_data",
            confidence="low",
            definition=(
                f"Dataset spans only {span_days} days. A defensible churn label "
                f"requires at least {MIN_DATA_DAYS_ADAPTIVE} days of transactional "
                f"history. No labels were produced; the churn model should be "
                f"skipped for this dataset and the report should disclose this clearly."
            ),
            data_span_days=span_days,
            total_customers=len(cust),
            one_time_buyers=int((cust["total_orders"] == 1).sum()) if len(cust) else 0,
            repeat_customers=int((cust["total_orders"] > 1).sum()) if len(cust) else 0,
            new_customers=0,
            eligible_customers=0,
            churned_customers=0,
            churn_rate_pct=0.0,
            warnings_=[
                f"Need ≥ {MIN_DATA_DAYS_ADAPTIVE} days of data; got {span_days}.",
                "Churn model NOT trained for this dataset.",
            ],
        )
        return pd.DataFrame(), report

    # ─── shared helpers ──────────────────────────────────────
    def _plausibility_warnings(self, n_eligible, churn_rate):
        warns = []
        if n_eligible < MIN_ELIGIBLE_CUSTOMERS:
            warns.append(
                f"Eligible cohort is {n_eligible} — below the "
                f"{MIN_ELIGIBLE_CUSTOMERS}-customer minimum for a stable model. "
                "Treat outputs as exploratory."
            )
        lo, hi = PLAUSIBILITY_BAND_PCT
        if churn_rate < lo or churn_rate > hi:
            warns.append(
                f"Churn rate of {churn_rate:.1f}% is outside the typical "
                f"{lo}–{hi}% band. Investigate the data before acting on results."
            )
        return warns

    def _finalise(self, labelled: pd.DataFrame) -> pd.DataFrame:
        """Canonical column name + correct dtype for the trainer."""
        out = labelled.rename(columns={self.customer_col: "customer_id"})
        out["is_churned"] = out["is_churned"].astype(int)
        return out


# ─────────────────────────────────────────────────────────────
#  CLI — drop-in replacement for the upstream label generation
# ─────────────────────────────────────────────────────────────
def _cli():
    p = argparse.ArgumentParser(
        description="Generate transparent, dataset-adaptive churn labels."
    )
    p.add_argument("--in",  dest="src",    required=True, help="Input transactions CSV")
    p.add_argument("--out", dest="dst",    required=True, help="Output customer-level CSV")
    p.add_argument("--report",             default=None,  help="Path for JSON labelling report")
    p.add_argument("--customer-col",       default="customer_id")
    p.add_argument("--date-col",           default="order_date")
    p.add_argument("--amount-col",         default=None)
    p.add_argument("--category-col",       default=None)
    args = p.parse_args()

    src_ext = Path(args.src).suffix.lower()
    if src_ext in (".xlsx", ".xls"):
        df = pd.read_excel(args.src)
    else:
        df = pd.read_csv(args.src)
    labeler = ChurnLabeler(
        customer_col=args.customer_col,
        date_col=args.date_col,
        amount_col=args.amount_col,
        category_col=args.category_col,
    )
    customers, report = labeler.label(df)

    rep_dict = report.to_dict()
    print("\n=== LABELLING REPORT ===")
    print(json.dumps(rep_dict, indent=2))

    if report.strategy == "insufficient_data":
        print("\n[REFUSED] No customer-level CSV written. See report above.",
              file=sys.stderr)
        if args.report:
            Path(args.report).parent.mkdir(parents=True, exist_ok=True)
            with open(args.report, "w") as f:
                json.dump(rep_dict, f, indent=2)
            print(f"[OK] Wrote labelling report → {args.report}", file=sys.stderr)
        sys.exit(2)

    Path(args.dst).parent.mkdir(parents=True, exist_ok=True)
    customers.to_csv(args.dst, index=False)
    print(f"\n[OK] Wrote {len(customers):,} labelled customers → {args.dst}")

    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w") as f:
            json.dump(rep_dict, f, indent=2)
        print(f"[OK] Wrote labelling report → {args.report}")


if __name__ == "__main__":
    _cli()