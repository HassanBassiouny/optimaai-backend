# OptimaAi Backend

AI-powered business intelligence API for SMEs. Delivers real-time revenue forecasting, churn prediction, growth projections, LLM-generated recommendations, and Business Model Canvas generation — all backed by trained ML models.

---

## Table of Contents

- [Overview](#overview)
- [Documentation](#Documentation)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Environment Variables](#environment-variables)
- [Training the ML Models](#training-the-ml-models)
- [API Reference](#api-reference)
- [Caching Strategy](#caching-strategy)
- [Database Schema](#database-schema)

---

## Overview

OptimaAi combines machine learning and large language models to give business users (Sales Managers, Finance Controllers, Executives) actionable intelligence from their ERP data.

## Documentation
Full platform docs: [optima-ai-documentation.vercel.app](https://optima-ai-documentation.vercel.app)

**Core capabilities:**

- **Revenue prediction** — per-order net revenue using an XGBoost/GBM regressor
- **Churn prediction** — customer churn probability with risk-level classification
- **Growth forecasting** — 3-month forward revenue projection
- **KPI snapshots** — aggregated model metrics persisted to PostgreSQL and cached in Redis
- **LLM recommendations** — role-aware business advice generated from live KPIs
- **Business Model Canvas** — AI-generated 9-block BMC derived from ML evaluation results

---

## Tech Stack

| Layer | Technology |
|---|---|
| API framework | FastAPI |
| ML models | XGBoost / GBM, Facebook Prophet |
| Database | PostgreSQL (SQLAlchemy ORM) |
| Cache | Redis |
| LLM provider | OpenRouter |
| Data preprocessing | Custom dynamic pipeline |
| Config | python-dotenv |

---

## Project Structure

```
optimaai-backend/
├── app/
│   ├── api.py                        # All API route definitions
│   ├── database.py                   # SQLAlchemy models & DB init
│   ├── cache.py                      # Redis caching layer
│   ├── routes/
│   │   ├── datasets_routes.py        # Dataset management endpoints
│   │   └── kb_routes.py              # Knowledge base endpoints
│   └── services/
│       ├── inference_service.py      # ML model inference
│       ├── recommendation_service.py # LLM recommendation generation
│       ├── bmc_service.py            # Business Model Canvas generation
│       ├── ml_bridge.py              # Loads latest evaluation artefacts
│       ├── knowledge_base.py         # Knowledge base logic
│       └── improved_prompts.py       # Prompt templates
├── ml/
│   ├── train.py                      # Training entry point
│   ├── optimaai_ml_trainer_v3.py     # Full ML training pipeline
│   └── dynamic_preprocessing_pipeline.py
├── data/
│   ├── amazon_revenue_forecasting.csv
│   ├── amazon_churn_prediction.csv
│   └── amazon_growth_projection.csv
└── optimaai_artefacts/               # Saved models & evaluation JSONs (auto-generated)
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL
- Redis

### Installation

```bash
git clone https://github.com/your-org/optimaai-backend.git
cd optimaai-backend

python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### Database Setup

```bash
# Create the database
psql -U postgres -c "CREATE DATABASE optimaai_db;"
psql -U postgres -c "CREATE USER optimaai WITH PASSWORD 'optimaai123';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE optimaai_db TO optimaai;"

# Tables are created automatically on first startup via init_db()
```

### Run the API

```bash
uvicorn app.main:app --reload
```

Interactive docs available at `http://localhost:8000/docs`.

---

## Environment Variables

Create a `.env` file in the project root:

```env
# PostgreSQL
DATABASE_URL=postgresql://optimaai:optimaai123@localhost:5432/optimaai_db

# Redis
REDIS_URL=redis://localhost:6379/0

# LLM (OpenRouter)
OPENROUTER_API_KEY=your_openrouter_key_here
```

---

## Training the ML Models

Before the prediction endpoints can respond, you must train the models at least once. Training reads from the `data/` CSVs and writes versioned artefacts (`.pkl` models + `evaluation_results_v<timestamp>.json`) to `optimaai_artefacts/`.

```bash
python ml/train.py
```

The API's `ml_bridge` automatically picks up the latest artefact file on each restart — no manual path configuration needed.

---

## API Reference

All endpoints are prefixed with `/api/v1`.

### Health Check

```
GET /api/v1/health
```

Returns service status and current UTC timestamp.

---

### Predict Revenue

```
POST /api/v1/predict/revenue
```

Predicts net revenue for a single order.

**Request body:**
```json
{
  "features": {
    "quantity_sold": 3,
    "price": 299.0,
    "discount_percent": 10,
    "seasonal_index": 1.0,
    "gross_margin_pct": 0.45,
    "month": 6,
    "quarter": 2
  }
}
```

**Response:**
```json
{
  "prediction": 807.30,
  "unit": "USD",
  "model": "XGBoost/GBM Revenue Regressor",
  "timestamp": "2026-05-11T10:00:00"
}
```

---

### Predict Churn

```
POST /api/v1/predict/churn
```

Returns churn probability and risk level for a customer.

**Request body:**
```json
{
  "features": {
    "days_since_last_order": 95,
    "total_orders": 2,
    "avg_days_between_orders": 60,
    "churn_risk_score": 0.72,
    "total_spent": 1200.0,
    "avg_order_value": 600.0
  }
}
```

**Response:**
```json
{
  "churn_probability": 0.7841,
  "risk_level": "high",
  "model": "XGBoost/GBM Churn Classifier",
  "timestamp": "2026-05-11T10:00:00"
}
```

Risk levels: `low` (< 0.4), `medium` (0.4–0.69), `high` (≥ 0.7).

---

### Predict Growth

```
POST /api/v1/predict/growth
```

Predicts 3-month forward revenue.

**Request body:**
```json
{
  "features": { ... }
}
```

**Response:**
```json
{
  "forecast_3m": 45200.00,
  "unit": "USD",
  "model": "XGBoost/GBM Growth Regressor",
  "timestamp": "2026-05-11T10:00:00"
}
```

---

### Get KPI Snapshot

```
GET /api/v1/kpis
```

Returns the latest aggregated ML model metrics. Checks Redis first (5-minute TTL), then loads from artefacts and persists a snapshot to PostgreSQL.

**Response includes:** MAPE, R², MAE, forecast bias, ROC-AUC, F1 score, churn rate, and 12-period forecasts for revenue and growth models.

---

### Get AI Recommendation

```
POST /api/v1/recommend
```

Generates a role-tailored business recommendation from live KPIs via LLM. Cached for 1 hour per role.

**Request body:**
```json
{
  "role": "Sales Manager"
}
```

Supported roles: `Sales Manager`, `Finance Controller`, `Executive` (or any custom string).

---

### Generate Business Model Canvas

```
POST /api/v1/bmc
```

Generates a full 9-block Business Model Canvas using live ML KPIs. Cached for 24 hours.

**Request body:**
```json
{
  "platform_name": "OptimaAi",
  "data_source": "Odoo ERP",
  "target_users": "SME Sales Managers, Finance Controllers, Executives"
}
```

**Response includes:** parsed `bmc_blocks` dict (9 canvas sections) and raw `bmc_text` markdown.

---

### Get Latest BMC

```
GET /api/v1/bmc/latest
```

Returns the most recently generated BMC from the database.

---

## Caching Strategy

| Data | Cache key | TTL |
|---|---|---|
| KPI snapshots | `optimaai:latest_kpi` | 5 minutes |
| Forecast results | `optimaai:latest_forecast` | 10 minutes |
| LLM recommendations | `optimaai:recommend:<role>` | 1 hour |
| Business Model Canvas | `optimaai:latest_bmc` | 24 hours |

Redis is optional — if unavailable, the API falls back gracefully to live computation on every request.

---

## Database Schema

Three tables are auto-created on startup:

**`kpi_snapshots`** — timestamped model metric snapshots (MAPE, R², AUC, F1, churn rate, 12-period forecasts)

**`recommendations`** — LLM-generated recommendations with role, model used, and linked KPI snapshot

**`bmc_results`** — generated Business Model Canvases with raw markdown and parsed 9-block JSON

---

## License

MIT
