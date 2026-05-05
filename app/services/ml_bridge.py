"""
╔══════════════════════════════════════════════════════╗
║   OPTIMAAI — ML BRIDGE                              ║
║   Reads your trained ML results and passes them     ║
║   directly to the LLM API (no manual hardcoding)    ║
╚══════════════════════════════════════════════════════╝

Place this file at:
    optimaai-backend/app/services/ml_bridge.py
"""

import os
import json
import glob
from pathlib import Path


# ── Default artefacts folder (relative to project root) ───────────────────
DEFAULT_ARTEFACT_DIR = os.path.join(
    os.path.dirname(__file__),   # app/services/
    "..", "..",                   # back to project root
    "optimaai_artefacts"
)


def load_latest_evaluation(artefact_dir: str = None) -> dict:
    """
    Automatically finds and loads the LATEST evaluation_results JSON
    from your optimaai_artefacts/ folder.

    Returns the full kpis dict ready to pass to generate_bmc() or
    generate_recommendation().
    """
    folder = artefact_dir or DEFAULT_ARTEFACT_DIR
    folder = os.path.abspath(folder)

    # Find all evaluation result files
    pattern = os.path.join(folder, "evaluation_results_v*.json")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No evaluation_results JSON found in: {folder}\n"
            f"Make sure you have run train.py at least once."
        )

    # Pick the most recent one (sorted alphabetically = chronologically
    # because version is a timestamp like 20260406_194103)
    latest = files[-1]
    print(f"  [ml_bridge] Loading: {os.path.basename(latest)}")

    with open(latest, "r", encoding="utf-8") as f:
        kpis = json.load(f)

    # Print a quick summary of what was loaded
    _print_summary(kpis)
    return kpis


def _print_summary(kpis: dict):
    """Print a quick one-line summary of the loaded metrics."""
    rev   = kpis.get("revenue_model", {})
    churn = kpis.get("churn_model",   {})
    grow  = kpis.get("growth_model",  {})
    prop  = kpis.get("prophet_model", {})

    print(f"  [ml_bridge] Revenue  → MAPE={rev.get('MAPE_pct','?')}%  "
          f"R²={rev.get('R2','?')}")

    if "ROC_AUC" in churn:
        print(f"  [ml_bridge] Churn    → ROC-AUC={churn.get('ROC_AUC','?')}  "
              f"churn_rate={churn.get('churn_rate_pct','?')}%")
    else:
        print(f"  [ml_bridge] Churn    → {churn.get('status','no data')}")

    if "R2" in grow:
        print(f"  [ml_bridge] Growth   → MAPE={grow.get('MAPE_pct','?')}%  "
              f"R²={grow.get('R2','?')}")

    if "MAPE_pct" in prop:
        print(f"  [ml_bridge] Prophet  → MAPE={prop.get('MAPE_pct','?')}%")
