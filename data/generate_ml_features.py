"""
Amazon Sales Dataset - ML Feature Generator
=============================================
Generates missing features for:
  1. Revenue Forecasting
  2. Churn Prediction
  3. Growth Projection

Input:  amazon_sales_dataset.xlsx
Output: amazon_sales_ml_ready.xlsx  (enriched dataset)
        amazon_revenue_forecasting.csv
        amazon_churn_prediction.csv
        amazon_growth_projection.csv
"""

import pandas as pd
import numpy as np
from datetime import timedelta

np.random.seed(42)

# ─────────────────────────────────────────────
# 0. LOAD
# ─────────────────────────────────────────────
df = pd.read_excel("amazon_sales_dataset.xlsx")
df["order_date"] = pd.to_datetime(df["order_date"])
df = df.sort_values("order_date").reset_index(drop=True)

print(f"Loaded {len(df):,} rows  |  {df['order_date'].min().date()} → {df['order_date'].max().date()}")

# ─────────────────────────────────────────────
# 1. SYNTHETIC CUSTOMER IDs
#    (realistic: ~12 000 repeat buyers from 50 000 orders)
# ─────────────────────────────────────────────
n_customers = 12_000
customer_pool = np.arange(1, n_customers + 1)

# heavier-tailed distribution: loyal customers order more
weights = np.random.pareto(1.5, n_customers) + 1
weights /= weights.sum()

df["customer_id"] = np.random.choice(customer_pool, size=len(df), p=weights)

# ─────────────────────────────────────────────
# 2. DATE FEATURES  (Revenue Forecasting + Growth)
# ─────────────────────────────────────────────
df["year"]            = df["order_date"].dt.year
df["month"]           = df["order_date"].dt.month
df["quarter"]         = df["order_date"].dt.quarter
df["week_of_year"]    = df["order_date"].dt.isocalendar().week.astype(int)
df["day_of_week"]     = df["order_date"].dt.dayofweek          # 0=Mon
df["is_weekend"]      = (df["day_of_week"] >= 5).astype(int)

# Seasonal index per month (Jan=low, Nov/Dec=high)
seasonal_base = {1:0.75, 2:0.78, 3:0.85, 4:0.90, 5:0.92, 6:0.88,
                 7:0.87, 8:0.91, 9:0.95, 10:1.00, 11:1.25, 12:1.30}
df["seasonal_index"] = df["month"].map(seasonal_base)

# ─────────────────────────────────────────────
# 3. COST & MARGIN  (Revenue Forecasting)
# ─────────────────────────────────────────────
# Cost-to-price ratio varies by category
category_cogs = {
    "Electronics":   0.65,
    "Fashion":        0.45,
    "Books":          0.35,
    "Sports":         0.50,
    "Beauty":         0.40,
    "Home & Kitchen": 0.55,
}
df["cogs_ratio"]       = df["product_category"].map(category_cogs)
df["cost_of_goods"]    = (df["price"] * df["cogs_ratio"] * df["quantity_sold"]).round(2)
df["gross_profit"]     = (df["total_revenue"] - df["cost_of_goods"]).round(2)
df["gross_margin_pct"] = (df["gross_profit"] / df["total_revenue"].replace(0, np.nan)).round(4)

# ─────────────────────────────────────────────
# 4. RETURN & REFUND FLAGS  (Revenue Forecasting + Churn)
# ─────────────────────────────────────────────
# Higher discount → slightly higher return rate; lower rating → higher return
return_prob = (
    0.04
    + (df["discount_percent"] / 100) * 0.08
    + ((5 - df["rating"]) / 4)       * 0.06
)
df["is_returned"]   = np.random.binomial(1, return_prob.clip(0, 0.25))
df["refund_amount"] = (df["is_returned"] * df["total_revenue"] * 0.90).round(2)
df["net_revenue"]   = (df["total_revenue"] - df["refund_amount"]).round(2)

# ─────────────────────────────────────────────
# 5. CUSTOMER-LEVEL AGGREGATES  (Churn + Growth)
# ─────────────────────────────────────────────
snapshot_date = df["order_date"].max() + timedelta(days=1)

cust = (
    df.groupby("customer_id")
    .agg(
        first_order_date  = ("order_date", "min"),
        last_order_date   = ("order_date", "max"),
        total_orders      = ("order_id",   "count"),
        total_spent       = ("net_revenue", "sum"),
        avg_order_value   = ("net_revenue", "mean"),
        total_items       = ("quantity_sold","sum"),
        avg_rating_given  = ("rating",       "mean"),
        return_count      = ("is_returned",  "sum"),
        unique_categories = ("product_category","nunique"),
        preferred_region  = ("customer_region","first"),
    )
    .reset_index()
)

cust["customer_tenure_days"] = (snapshot_date - cust["first_order_date"]).dt.days
cust["days_since_last_order"] = (snapshot_date - cust["last_order_date"]).dt.days
cust["avg_days_between_orders"] = (
    cust["customer_tenure_days"] / cust["total_orders"].clip(lower=1)
).round(1)
cust["return_rate"] = (cust["return_count"] / cust["total_orders"]).round(4)
cust["spend_per_day"] = (
    cust["total_spent"] / cust["customer_tenure_days"].replace(0, 1)
).round(4)

# ─────────────────────────────────────────────
# 6. CHURN LABEL
#    Definition: no order in the last 90 days
# ─────────────────────────────────────────────
CHURN_WINDOW = 90   # days

cust["is_churned"] = (cust["days_since_last_order"] > CHURN_WINDOW).astype(int)

# Churn probability score (rule-based, useful as a soft target / sanity check)
churn_score = (
    0.30 * (cust["days_since_last_order"] / 180).clip(0, 1)
    + 0.25 * (1 - (cust["total_orders"] / cust["total_orders"].max()))
    + 0.20 * cust["return_rate"]
    + 0.15 * (1 - (cust["avg_rating_given"] / 5))
    + 0.10 * (cust["avg_days_between_orders"] / 60).clip(0, 1)
)
cust["churn_risk_score"] = churn_score.round(4).clip(0, 1)

# ─────────────────────────────────────────────
# 7. CLV & GROWTH FEATURES
# ─────────────────────────────────────────────
DISCOUNT_RATE  = 0.10   # annual
AVG_LIFESPAN   = 3      # years assumed

cust["predicted_clv"] = (
    cust["avg_order_value"]
    * (365 / cust["avg_days_between_orders"].replace(0, 365))
    * AVG_LIFESPAN
    / (1 + DISCOUNT_RATE)
).round(2)

# Engagement tier
conditions = [
    cust["total_orders"] >= 15,
    cust["total_orders"] >= 8,
    cust["total_orders"] >= 3,
]
choices = ["VIP", "Loyal", "Regular"]
cust["customer_segment"] = np.select(conditions, choices, default="New")

# ─────────────────────────────────────────────
# 8. TIME-SERIES ROLLING FEATURES  (Revenue Forecasting)
# ─────────────────────────────────────────────
daily = (
    df.groupby("order_date")
    .agg(daily_revenue=("net_revenue","sum"),
         daily_orders =("order_id",   "count"),
         daily_items  =("quantity_sold","sum"))
    .reset_index()
    .sort_values("order_date")
)

daily["revenue_lag_7d"]    = daily["daily_revenue"].shift(7)
daily["revenue_lag_30d"]   = daily["daily_revenue"].shift(30)
daily["revenue_roll_7d"]   = daily["daily_revenue"].rolling(7,  min_periods=1).mean().round(2)
daily["revenue_roll_30d"]  = daily["daily_revenue"].rolling(30, min_periods=1).mean().round(2)
daily["revenue_roll_90d"]  = daily["daily_revenue"].rolling(90, min_periods=1).mean().round(2)
daily["orders_roll_7d"]    = daily["daily_orders"].rolling(7,   min_periods=1).mean().round(2)
daily["revenue_yoy_growth"] = (
    (daily["daily_revenue"] - daily["daily_revenue"].shift(365))
    / daily["daily_revenue"].shift(365).replace(0, np.nan)
).round(4)

# Monthly aggregation for Growth Projection
monthly = (
    df.groupby(["year", "month"])
    .agg(monthly_revenue=("net_revenue","sum"),
         monthly_orders  =("order_id",  "count"),
         new_customers   =("customer_id","nunique"),
         avg_order_value =("net_revenue","mean"))
    .reset_index()
)
monthly["month_label"] = pd.to_datetime(
    monthly[["year","month"]].assign(day=1)
)
monthly = monthly.sort_values("month_label")
monthly["mom_growth"] = (
    monthly["monthly_revenue"].pct_change().round(4)
)
monthly["yoy_growth"] = (
    monthly["monthly_revenue"].pct_change(12).round(4)
)
monthly["revenue_3m_avg"] = (
    monthly["monthly_revenue"].rolling(3, min_periods=1).mean().round(2)
)
# 3-month forward revenue projection (simple trend extrapolation)
monthly["revenue_3m_forecast"] = (
    monthly["revenue_3m_avg"] * (1 + monthly["mom_growth"].fillna(0))
).round(2)

# ─────────────────────────────────────────────
# 9. MERGE CUSTOMER FEATURES BACK TO MAIN DF
# ─────────────────────────────────────────────
df = df.merge(
    cust[[
        "customer_id","customer_tenure_days","days_since_last_order",
        "total_orders","avg_order_value","return_rate","customer_segment",
        "predicted_clv","churn_risk_score","is_churned",
    ]],
    on="customer_id", how="left"
)

# ─────────────────────────────────────────────
# 10. EXPORT — THREE TASK-SPECIFIC DATASETS
# ─────────────────────────────────────────────

# ── 10A. Revenue Forecasting ──────────────────
revenue_cols = [
    "order_date","year","month","quarter","week_of_year",
    "day_of_week","is_weekend","seasonal_index",
    "product_id","product_category","customer_region",
    "price","discount_percent","quantity_sold",
    "discounted_price","total_revenue","net_revenue",
    "cost_of_goods","gross_profit","gross_margin_pct",
    "is_returned","refund_amount","payment_method",
    "rating","review_count",
]
df_revenue = df[revenue_cols].copy()
df_revenue = df_revenue.merge(
    daily[["order_date","revenue_lag_7d","revenue_lag_30d",
           "revenue_roll_7d","revenue_roll_30d","revenue_roll_90d",
           "orders_roll_7d","revenue_yoy_growth"]],
    on="order_date", how="left"
)
df_revenue.to_csv("amazon_revenue_forecasting.csv", index=False)

# ── 10B. Churn Prediction ─────────────────────
churn_cols = [
    "customer_id","preferred_region","customer_tenure_days",
    "total_orders","total_spent","avg_order_value",
    "avg_days_between_orders","days_since_last_order",
    "return_rate","total_items","avg_rating_given",
    "unique_categories","spend_per_day","churn_risk_score",
    "customer_segment","predicted_clv","is_churned",
]
df_churn = cust[churn_cols].copy()
df_churn.to_csv("amazon_churn_prediction.csv", index=False)

# ── 10C. Growth Projection ────────────────────
monthly.to_csv("amazon_growth_projection.csv", index=False)

# ── 10D. Full enriched dataset ────────────────
df.to_excel("amazon_sales_ml_ready.xlsx", index=False)

# ─────────────────────────────────────────────
# 11. SUMMARY
# ─────────────────────────────────────────────
print("\n✅  Done!\n")
print("─" * 55)
print(f"  amazon_sales_ml_ready.xlsx       {len(df):>7,} rows  {len(df.columns)} cols")
print(f"  amazon_revenue_forecasting.csv   {len(df_revenue):>7,} rows  {len(df_revenue.columns)} cols")
print(f"  amazon_churn_prediction.csv      {len(df_churn):>7,} rows  {len(df_churn.columns)} cols")
print(f"  amazon_growth_projection.csv     {len(monthly):>7,} rows  {len(monthly.columns)} cols")
print("─" * 55)

print("\n📌  New columns per ML task:")
print("\n  [Revenue Forecasting]")
for c in ["net_revenue","cost_of_goods","gross_profit","gross_margin_pct",
          "is_returned","refund_amount","seasonal_index","is_weekend",
          "revenue_lag_7d","revenue_lag_30d","revenue_roll_7d",
          "revenue_roll_30d","revenue_roll_90d","revenue_yoy_growth"]:
    print(f"     + {c}")

print("\n  [Churn Prediction]")
for c in ["customer_id","customer_tenure_days","days_since_last_order",
          "total_orders","avg_days_between_orders","return_rate",
          "spend_per_day","churn_risk_score","customer_segment",
          "predicted_clv","is_churned"]:
    print(f"     + {c}")

print("\n  [Growth Projection]")
for c in ["mom_growth","yoy_growth","revenue_3m_avg",
          "revenue_3m_forecast","new_customers"]:
    print(f"     + {c}")

print("\n  TARGET LABELS:")
print("     → is_churned          (binary 0/1, Churn Prediction)")
print("     → net_revenue         (continuous, Revenue Forecasting)")
print("     → revenue_3m_forecast (continuous, Growth Projection)")
