"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              OPTIMAAI GROWTH PROFILER  v1                                    ║
║                                                                              ║
║  Purpose ─ profile monthly-aggregate revenue data and emit a transparency   ║
║            report explaining how far ahead the growth model can responsibly  ║
║            forecast given the history available.                            ║
║                                                                              ║
║  Why this exists                                                             ║
║  ───────────────                                                             ║
║  The v3 trainer ran a 12-month forward forecast on 20 months of training    ║
║  data — extrapolating beyond what the data could possibly support. The      ║
║  report flagged it as low-confidence after the fact. This profiler stops    ║
║  it BEFORE training: it counts months, decides the safe horizon, and the    ║
║  trainer/report respect that ceiling.                                       ║
║                                                                              ║
║  Three strategies, picked automatically:                                    ║
║    A. full_forecasting   — ≥ 24 months, full year + holdout, horizon 6–12  ║
║    B. directional_only   — 6–23 months, short horizon (1–6), no YoY claims ║
║    C. insufficient_data  — < 6 months, refuse                               ║
║                                                                              ║
║  Inputs  : DataFrame with month_label + monthly_revenue columns             ║
║            (CLI also accepts transactional data and auto-aggregates)        ║
║  Outputs : GrowthProfileReport                                              ║
║                                                                              ║
║  CLI:                                                                        ║
║    python growth_profiler.py --in data/amazon_growth_projection.csv \\      ║
║                              --report data/growth_data_report.json          ║
║                                                                              ║
║    # or aggregate from transactional data first:                            ║
║    python growth_profiler.py --in data/amazon_revenue_forecasting.csv \\    ║
║                              --from-transactions \\                          ║
║                              --report data/growth_data_report.json          ║
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
#  CONFIG
# ─────────────────────────────────────────────────────────────
MIN_MONTHS_FORECAST = 6      # below this: refuse
MIN_MONTHS_DIRECT   = 12     # 6-11 months → directional only, very short horizon
MIN_MONTHS_FULL     = 24     # below this: no YoY, no full forecasting
MAX_HORIZON_MONTHS  = 12     # never forecast farther than this regardless of data


# ─────────────────────────────────────────────────────────────
#  REPORT
# ─────────────────────────────────────────────────────────────
@dataclass
class GrowthProfileReport:
    strategy:    str           # 'full_forecasting' | 'directional_only' | 'insufficient_data'
    confidence:  str           # 'high' | 'medium' | 'low'
    definition:  str

    suitable_for:     list = field(default_factory=list)
    not_suitable_for: list = field(default_factory=list)

    # Data shape
    months_of_history:      int = 0
    months_with_revenue:    int = 0
    months_zero_or_missing: int = 0
    first_period:           Optional[str] = None
    last_period:            Optional[str] = None

    # Revenue distribution
    total_revenue:          float = 0.0
    median_monthly_revenue: float = 0.0
    min_monthly_revenue:    float = 0.0
    max_monthly_revenue:    float = 0.0

    # Capability flags
    has_full_seasonal_cycle: bool = False   # >= 12 months
    has_yoy_capability:      bool = False   # >= 24 months

    # Recommended training setup
    safe_horizon_months:     int = 0
    recommended_train_months: int = 0
    recommended_test_months:  int = 0

    warnings_: list = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["warnings"] = d.pop("warnings_")
        return d


# ─────────────────────────────────────────────────────────────
#  THE PROFILER
# ─────────────────────────────────────────────────────────────
class GrowthProfiler:
    """Profile monthly-aggregate revenue data; emit a transparent report."""

    def __init__(
        self,
        period_col:  str = "month_label",
        revenue_col: str = "monthly_revenue",
    ):
        self.period_col  = period_col
        self.revenue_col = revenue_col

    # ─── public ──────────────────────────────────────────────
    def profile(self, df: pd.DataFrame) -> GrowthProfileReport:
        df = self._validate(df)
        if df.empty:
            return self._refuse(0, "No valid rows after parsing periods.")

        n_months = len(df)
        n_with_rev = int((df[self.revenue_col] > 0).sum())
        n_zero     = n_months - n_with_rev

        if n_months < MIN_MONTHS_FORECAST:
            return self._refuse(n_months,
                f"Only {n_months} months of history; need at least {MIN_MONTHS_FORECAST}.")

        if n_months >= MIN_MONTHS_FULL:
            return self._full_forecasting(df, n_months, n_with_rev, n_zero)
        return self._directional_only(df, n_months, n_with_rev, n_zero)

    # ─── internals ───────────────────────────────────────────
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in (self.period_col, self.revenue_col):
            if col not in df.columns:
                raise ValueError(f"Required column missing: '{col}'")
        df = df.copy()
        df[self.period_col]  = pd.to_datetime(df[self.period_col], errors="coerce")
        df[self.revenue_col] = pd.to_numeric(df[self.revenue_col], errors="coerce")
        df = df.dropna(subset=[self.period_col])
        df[self.revenue_col] = df[self.revenue_col].fillna(0)
        return df.sort_values(self.period_col).reset_index(drop=True)

    def _shared_stats(self, df, n_months, n_with_rev, n_zero):
        rev = df[self.revenue_col]
        return dict(
            months_of_history      = n_months,
            months_with_revenue    = n_with_rev,
            months_zero_or_missing = n_zero,
            first_period           = str(df[self.period_col].min().date())[:7],
            last_period            = str(df[self.period_col].max().date())[:7],
            total_revenue          = round(float(rev.sum()), 2),
            median_monthly_revenue = round(float(rev.median()), 2),
            min_monthly_revenue    = round(float(rev.min()), 2),
            max_monthly_revenue    = round(float(rev.max()), 2),
        )

    # ─── STRATEGY A: full forecasting (24+ months) ───────────
    def _full_forecasting(self, df, n_months, n_with_rev, n_zero):
        # Train/test split — keep last 4–6 months for test
        test_months = min(6, max(4, n_months // 6))
        train_months = n_months - test_months

        # Safe horizon: cap at 12, and at one-third of training data
        horizon = min(MAX_HORIZON_MONTHS, max(3, train_months // 3))

        warns = []
        if n_zero > n_months * 0.1:
            warns.append(
                f"{n_zero} of {n_months} months have zero or missing revenue — "
                "investigate before trusting the forecast."
            )

        confidence = "high" if n_months >= 36 else "medium"

        definition = (
            f"This dataset has {n_months} months of history "
            f"({df[self.period_col].min().date():%Y-%m} to "
            f"{df[self.period_col].max().date():%Y-%m}), enough for full "
            f"forecasting with year-over-year comparisons. Recommended split: "
            f"{train_months} months for training, {test_months} for test. "
            f"Forecasts up to {horizon} months ahead are defensible; beyond "
            f"that the model would be extrapolating."
        )

        return GrowthProfileReport(
            strategy="full_forecasting",
            confidence=confidence,
            definition=definition,
            suitable_for=[
                f"Forecasting up to {horizon} months ahead",
                "Year-over-year growth claims",
                "Seasonal pattern identification",
                "Budget and capacity planning conversations",
            ],
            not_suitable_for=[
                f"Forecasts beyond {horizon} months (extrapolation, not prediction)",
                "Hiring or contract commitments without human review",
            ],
            has_full_seasonal_cycle=True,
            has_yoy_capability=True,
            safe_horizon_months=horizon,
            recommended_train_months=train_months,
            recommended_test_months=test_months,
            warnings_=warns,
            **self._shared_stats(df, n_months, n_with_rev, n_zero),
        )

    # ─── STRATEGY B: directional only (6-23 months) ──────────
    def _directional_only(self, df, n_months, n_with_rev, n_zero):
        test_months  = min(4, max(2, n_months // 5))
        train_months = n_months - test_months

        # Tighter horizon — no full year of history
        if n_months >= 18:
            horizon = 6
        elif n_months >= 12:
            horizon = 3
        else:
            horizon = 2

        warns = [
            f"Only {n_months} months of history — forecasts are directional, "
            f"not committal numbers.",
            "Year-over-year claims are NOT supported (need 24+ months).",
        ]
        if n_zero > n_months * 0.1:
            warns.append(
                f"{n_zero} of {n_months} months have zero or missing revenue."
            )

        has_cycle = n_months >= 12

        definition = (
            f"This dataset has {n_months} months of history — enough for "
            f"directional forecasting only. Recommended split: {train_months} "
            f"months train, {test_months} test. Forecasts up to {horizon} "
            f"months ahead are usable for trajectory (up/flat/down), but the "
            f"specific values should NOT be used as targets for budgets, "
            f"hiring, or contracts. Re-train as full-forecasting once "
            f"{MIN_MONTHS_FULL} months of history are available."
        )

        return GrowthProfileReport(
            strategy="directional_only",
            confidence="low",
            definition=definition,
            suitable_for=[
                f"Trajectory direction (up/flat/down) {horizon} months ahead",
                "Internal sanity-check forecasts",
            ],
            not_suitable_for=[
                "Specific monthly targets quoted to stakeholders",
                "Year-over-year growth claims",
                "Budget commitments, hiring decisions, supplier contracts",
                f"Forecasts beyond {horizon} months",
            ],
            has_full_seasonal_cycle=has_cycle,
            has_yoy_capability=False,
            safe_horizon_months=horizon,
            recommended_train_months=train_months,
            recommended_test_months=test_months,
            warnings_=warns,
            **self._shared_stats(df, n_months, n_with_rev, n_zero),
        )

    # ─── STRATEGY C: refuse ──────────────────────────────────
    def _refuse(self, n_months: int, reason: str) -> GrowthProfileReport:
        return GrowthProfileReport(
            strategy="insufficient_data",
            confidence="low",
            definition=(
                f"Growth dataset cannot be modelled: {reason} The growth model "
                f"should be skipped for this dataset and the report should "
                f"disclose this clearly."
            ),
            suitable_for=[],
            not_suitable_for=["Any forward-looking growth claims"],
            months_of_history=n_months,
            warnings_=[reason, "Growth model NOT recommended for this dataset."],
        )


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────
def _aggregate_transactions(df, date_col, amount_col):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
    df = df.dropna(subset=[date_col, amount_col])
    out = (
        df.set_index(date_col)[amount_col]
          .resample("MS").sum()
          .reset_index()
          .rename(columns={date_col: "month_label", amount_col: "monthly_revenue"})
    )
    return out


def _cli():
    p = argparse.ArgumentParser(
        description="Profile monthly growth data and emit a transparency report."
    )
    p.add_argument("--in", dest="src",  required=True, help="Input CSV")
    p.add_argument("--report",          required=True, help="Path for JSON report")
    p.add_argument("--from-transactions", action="store_true",
                   help="Treat input as transactional and aggregate to monthly first")
    p.add_argument("--period-col",      default="month_label")
    p.add_argument("--revenue-col",     default="monthly_revenue")
    p.add_argument("--date-col",        default="order_date",
                   help="Used only with --from-transactions")
    p.add_argument("--amount-col",      default="net_revenue",
                   help="Used only with --from-transactions")
    args = p.parse_args()

    src_ext = Path(args.src).suffix.lower()
    if src_ext in (".xlsx", ".xls"):
        df = pd.read_excel(args.src)
    else:
        df = pd.read_csv(args.src)

    if args.from_transactions:
        df = _aggregate_transactions(df, args.date_col, args.amount_col)
        period_col, revenue_col = "month_label", "monthly_revenue"
    else:
        period_col, revenue_col = args.period_col, args.revenue_col

    profiler = GrowthProfiler(period_col=period_col, revenue_col=revenue_col)
    report = profiler.profile(df)

    rep_dict = report.to_dict()
    print("\n=== GROWTH PROFILE REPORT ===")
    print(json.dumps(rep_dict, indent=2))

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w") as f:
        json.dump(rep_dict, f, indent=2)
    print(f"\n[OK] Wrote report → {args.report}")

    if report.strategy == "insufficient_data":
        sys.exit(2)


if __name__ == "__main__":
    _cli()