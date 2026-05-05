"""
OptimaAi — training entry point  (v3)
Run: python ml/train.py
"""
import sys, os
sys.path.append(os.path.dirname(__file__))

from optimaai_ml_trainer_v3 import OptimaAiMLTrainerV3

BASE         = os.path.join(os.path.dirname(__file__), "..")
ARTEFACT_DIR = os.path.join(BASE, "optimaai_artefacts")

DATA_PATHS = {
    "revenue" : os.path.join(BASE, "data", "amazon_revenue_forecasting.csv"),
    "churn"   : os.path.join(BASE, "data", "amazon_churn_prediction.csv"),
    "growth"  : os.path.join(BASE, "data", "amazon_growth_projection.csv"),
}

trainer = OptimaAiMLTrainerV3(artefact_dir=ARTEFACT_DIR, data_paths=DATA_PATHS)
results = trainer.run()

# ─────────────────────────────────────────────────────────────────────────
#  Auto-sync results into the RAG knowledge base.
#  This means every time you re-train, the Insights page / LLM immediately
#  knows the new MAPE, R², churn rate, top features, forecasts, etc.
# ─────────────────────────────────────────────────────────────────────────
try:
    # The project layout is:  <root>/ml/train.py   +   <root>/app/services/...
    # so add the repo root to sys.path before importing from `app`.
    REPO_ROOT = os.path.abspath(BASE)
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    from app.services.knowledge_base import ingest_kpi_snapshot
    snapshot = ingest_kpi_snapshot(results)
    print(f"  [kb] Synced training results into RAG knowledge base "
          f"({snapshot.get('chunks', '?')} chunks)")
except Exception as e:
    print(f"  [kb] KPI sync skipped: {e}")