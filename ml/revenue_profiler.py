"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              OPTIMAAI REVENUE PROFILER  v1                                   ║
║                                                                              ║
║  Purpose ─ profile any transactional revenue dataset and emit a              ║
║            transparency report explaining what the revenue model can and    ║
║            cannot reliably be used for.                                     ║
║                                                                              ║
║  Why this exists                                                             ║
║  ───────────────                                                             ║
║  The v3 trainer reports a per-transaction MAPE of 74% alongside an R² of    ║
║  0.85 — both correct, both misleading without context. High MAPE on mixed   ║
║  order sizes is a property of the data, not a model failure. This profiler  ║
║  measures that property up-front (coefficient of variation), tells the      ║
║  executive summary how to interpret the result, and recommends an           ║
║  aggregation grain so monthly forecasts can be produced cleanly.            ║
║                                                                              ║
║  Three strategies, picked automatically:                                    ║
║    A. acceptable          — span ≥ 90 days, ≥ 500 transactions, ≥ 30%       ║
║                              day coverage. Profile is reliable.              ║
║    B. acceptable_minimum  — meets minimums but flagged for caution.          ║
║    C. insufficient_data   — refuse honestly; revenue model should skip.     ║
║                                                                              ║
║  Inputs  : DataFrame of transactions with order_date + revenue column       ║
║  Outputs : (RevenueProfileReport, optional aggregated DataFrame)            ║
║                                                                              ║
║  CLI:                                                                        ║
║    python revenue_profiler.py --in data/amazon_revenue_forecasting.csv \\   ║
║                               --report data/revenue_data_report.json \\     ║
║                               --aggregate-out data/revenue_monthly.csv      ║
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
#  CONFIG — tune only with a clear reason
# ─────────────────────────────────────────────────────────────
MIN_DATA_DAYS         = 90        # below this: refuse
MIN_TRANSACTIONS      = 500       # below this: refuse
MIN_DAY_COVERAGE_PCT  = 30.0      # % of days in span that have any transaction
HIGH_COV_THRESHOLD    = 1.5       # CoV above which per-transaction error is naturally high


# ─────────────────────────────────────────────────────────────
#  REPORT
# ─────────────────────────────────────────────────────────────
@dataclass
class RevenueProfileReport:
    strategy:      str        # 'acceptable' | 'acceptable_minimum' | 'insufficient_data'
    confidence:    str        # 'high' | 'medium' | 'low'
    definition:    str        # plain-English summary for the executive report

    suitable_for:     list = field(default_factory=list)
    not_suitable_for: list = field(default_factory=list)

    # Data shape
    data_span_days:   int = 0
    first_date:       Optional[str] = None
    last_date:        Optional[str] = None
    total_transactions: int = 0
    total_revenue:    float = 0.0

    # Time coverage
    days_with_data:   int = 0
    coverage_pct:     float = 0.0
    avg_transactions_per_day: float = 0.0
    avg_revenue_per_day: float = 0.0

    # Per-transaction distribution — drives MAPE expectations
    transaction_p10:  Optional[float] = None
    transaction_p50:  Optional[float] = None
    transaction_p90:  Optional[float] = None
    transaction_p99:  Optional[float] = None
    transaction_max:  Optional[float] = None
    coefficient_of_variation: Optional[float] = None

    # Recommendations for the trainer
    recommended_aggregation:   Optional[str] = None   # 'daily' | 'weekly' | 'monthly'
    monthly_periods_available: Optional[int] = None
    weekly_periods_available:  Optional[int] = None
    train_test_split_ratio:    Optional[float] = None

    warnings_: list = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["warnings"] = d.pop("warnings_")
        return d


# ─────────────────────────────────────────────────────────────
#  THE PROFILER
# ─────────────────────────────────────────────────────────────
class RevenueProfiler:
    """Profile transactional revenue data; emit a transparent report."""

    def __init__(self, date_col: str = "order_date", amount_col: str = "net_revenue"):
        self.date_col = date_col
        self.amount_col = amount_col

    # ─── public ──────────────────────────────────────────────
    def profile(self, df: pd.DataFrame) -> RevenueProfileReport:
        df = self._validate(df)
        if df.empty:
            return self._refuse(0, 0, "No valid rows after parsing dates and dropping null amounts.")

        span_days      = (df[self.date_col].max() - df[self.date_col].min()).days
        n_tx           = len(df)
        days_with_data = df[self.date_col].dt.normalize().nunique()
        # +1 because span counts gaps between dates; days_with_data counts dates themselves
        days_in_span   = span_days + 1
        coverage_pct   = min(100.0, (days_with_data / max(1, days_in_span)) * 100)

        if span_days < MIN_DATA_DAYS:
            return self._refuse(span_days, n_tx,
                f"Span of {span_days} days is below minimum {MIN_DATA_DAYS}.")
        if n_tx < MIN_TRANSACTIONS:
            return self._refuse(span_days, n_tx,
                f"Only {n_tx:,} transactions; need at least {MIN_TRANSACTIONS}.")
        if coverage_pct < MIN_DAY_COVERAGE_PCT:
            return self._refuse(span_days, n_tx,
                f"Day coverage of {coverage_pct:.1f}% is below {MIN_DAY_COVERAGE_PCT}%.")

        return self._build_report(df, span_days, n_tx, days_with_data, coverage_pct)

    def aggregate(self, df: pd.DataFrame, grain: str = "monthly") -> pd.DataFrame:
        """
        Roll up transactional data to the chosen grain.
        Returns: period | revenue | transactions | avg_transaction_value
        """
        df = self._validate(df)
        if df.empty:
            return pd.DataFrame(columns=["period", "revenue", "transactions", "avg_transaction_value"])

        freq_map = {"daily": "D", "weekly": "W-MON", "monthly": "MS"}
        if grain not in freq_map:
            raise ValueError(f"grain must be one of {list(freq_map)}")

        agg = (
            df.set_index(self.date_col)[self.amount_col]
              .resample(freq_map[grain])
              .agg(["sum", "count", "mean"])
              .reset_index()
              .rename(columns={
                  self.date_col: "period",
                  "sum":   "revenue",
                  "count": "transactions",
                  "mean":  "avg_transaction_value",
              })
        )
        # drop empty trailing periods
        agg = agg[agg["transactions"] > 0].reset_index(drop=True)
        return agg

    # ─── internals ───────────────────────────────────────────
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in (self.date_col, self.amount_col):
            if col not in df.columns:
                raise ValueError(f"Required column missing: '{col}'")
        df = df.copy()
        df[self.date_col]   = pd.to_datetime(df[self.date_col], errors="coerce")
        df[self.amount_col] = pd.to_numeric(df[self.amount_col], errors="coerce")
        df = df.dropna(subset=[self.date_col, self.amount_col])
        return df.sort_values(self.date_col).reset_index(drop=True)

    def _build_report(self, df, span_days, n_tx, days_with_data, coverage_pct):
        amounts   = df[self.amount_col]
        total_rev = float(amounts.sum())
        mean_amt  = float(amounts.mean())
        cov       = float(amounts.std() / max(mean_amt, 1e-9))

        first_date = df[self.date_col].min()
        last_date  = df[self.date_col].max()

        # Recommend aggregation grain based on span
        if span_days >= 540:
            grain = "monthly"
        elif span_days >= 180:
            grain = "weekly"
        else:
            grain = "daily"

        monthly_periods = (last_date.to_period("M") - first_date.to_period("M")).n + 1
        weekly_periods  = (last_date - first_date).days // 7 + 1

        # Train/test split — looser when data is small
        split = 0.80 if span_days >= 365 else 0.70

        warns = []
        if cov > HIGH_COV_THRESHOLD:
            warns.append(
                f"Order sizes vary widely (coefficient of variation = {cov:.2f}). "
                f"Per-transaction MAPE will be naturally high — only aggregate "
                f"({grain}) forecasts will look clean on dashboards."
            )
        if monthly_periods < 12:
            warns.append(
                f"Only {monthly_periods} months of data — yearly seasonality "
                "cannot be modelled reliably yet."
            )
        if coverage_pct < 70:
            warns.append(
                f"Day coverage is {coverage_pct:.0f}% — there are gaps in the "
                "transactional history. Aggregate to weekly or monthly to smooth them."
            )

        # Strategy + confidence
        if span_days >= 540 and cov <= HIGH_COV_THRESHOLD and coverage_pct >= 70:
            strategy   = "acceptable"
            confidence = "high"
        elif span_days >= 365:
            strategy   = "acceptable"
            confidence = "medium"
        else:
            strategy   = "acceptable_minimum"
            confidence = "medium"

        suitable_for = [
            f"{grain.capitalize()} aggregate revenue forecasting (use the aggregator helper)",
            "Identifying revenue drivers via feature importance",
            "Detecting seasonality and trend changes over time",
        ]
        not_suitable_for = [
            f"Quoting individual order values precisely (CoV={cov:.2f} → noisy at row level)",
            "Forecasting categories or regions absent from the training data",
        ]
        if monthly_periods < 24:
            not_suitable_for.append(
                "Year-over-year growth claims (less than 24 months of history)"
            )

        definition = (
            f"This dataset spans {span_days} days "
            f"({first_date.date()} to {last_date.date()}) with {n_tx:,} "
            f"transactions and ${total_rev:,.0f} total revenue. Order sizes "
            f"have a coefficient of variation of {cov:.2f}, meaning per-"
            f"transaction predictions are naturally noisy. The model is best "
            f"used at the '{grain}' aggregation grain for executive reporting; "
            f"per-row predictions remain valid for relative ranking but should "
            f"not be quoted as absolute order values."
        )

        return RevenueProfileReport(
            strategy=strategy,
            confidence=confidence,
            definition=definition,
            suitable_for=suitable_for,
            not_suitable_for=not_suitable_for,
            data_span_days=span_days,
            first_date=str(first_date.date()),
            last_date=str(last_date.date()),
            total_transactions=n_tx,
            total_revenue=round(total_rev, 2),
            days_with_data=days_with_data,
            coverage_pct=round(coverage_pct, 2),
            avg_transactions_per_day=round(n_tx / max(1, days_with_data), 2),
            avg_revenue_per_day=round(total_rev / max(1, days_with_data), 2),
            transaction_p10=round(float(amounts.quantile(0.10)), 2),
            transaction_p50=round(float(amounts.quantile(0.50)), 2),
            transaction_p90=round(float(amounts.quantile(0.90)), 2),
            transaction_p99=round(float(amounts.quantile(0.99)), 2),
            transaction_max=round(float(amounts.max()), 2),
            coefficient_of_variation=round(cov, 4),
            recommended_aggregation=grain,
            monthly_periods_available=int(monthly_periods),
            weekly_periods_available=int(weekly_periods),
            train_test_split_ratio=split,
            warnings_=warns,
        )

    def _refuse(self, span_days, n_tx, reason):
        return RevenueProfileReport(
            strategy="insufficient_data",
            confidence="low",
            definition=(
                f"Revenue dataset cannot be profiled reliably: {reason} "
                f"The revenue model should be skipped for this dataset and "
                f"the report should disclose this clearly."
            ),
            suitable_for=[],
            not_suitable_for=["Any revenue forecasting until more data accumulates"],
            data_span_days=span_days,
            total_transactions=n_tx,
            warnings_=[reason, "Revenue model NOT recommended for this dataset."],
        )


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────
def _cli():
    p = argparse.ArgumentParser(
        description="Profile transactional revenue data and emit a transparency report."
    )
    p.add_argument("--in",  dest="src",     required=True, help="Input transactions CSV")
    p.add_argument("--report",              required=True, help="Path for JSON report")
    p.add_argument("--aggregate-out",       default=None,
                   help="Optional: path to write the aggregated time-series CSV")
    p.add_argument("--date-col",            default="order_date")
    p.add_argument("--amount-col",          default="net_revenue")
    p.add_argument("--grain",               default=None,
                   choices=["daily", "weekly", "monthly"],
                   help="Override the recommended aggregation grain")
    args = p.parse_args()

    src_ext = Path(args.src).suffix.lower()
    if src_ext in (".xlsx", ".xls"):
        df = pd.read_excel(args.src)
    else:
        df = pd.read_csv(args.src)
    profiler = RevenueProfiler(date_col=args.date_col, amount_col=args.amount_col)
    report = profiler.profile(df)

    rep_dict = report.to_dict()
    print("\n=== REVENUE PROFILE REPORT ===")
    print(json.dumps(rep_dict, indent=2))

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w") as f:
        json.dump(rep_dict, f, indent=2)
    print(f"\n[OK] Wrote report → {args.report}")

    if report.strategy == "insufficient_data":
        sys.exit(2)

    if args.aggregate_out:
        grain = args.grain or report.recommended_aggregation
        agg = profiler.aggregate(df, grain=grain)
        Path(args.aggregate_out).parent.mkdir(parents=True, exist_ok=True)
        agg.to_csv(args.aggregate_out, index=False)
        print(f"[OK] Wrote {grain} aggregate ({len(agg)} periods) → {args.aggregate_out}")


if __name__ == "__main__":
    _cli()