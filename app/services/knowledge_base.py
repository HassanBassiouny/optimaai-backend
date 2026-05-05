"""
services/knowledge_base.py  —  v8 (free-forever, dual-provider)

Single file, no patches. Replace your entire knowledge_base.py with this.

Features:
  1. OpenRouter primary (DeepSeek first — most stable upstream)
  2. Groq fallback (14,400 req/day free tier, no credit card)
  3. Auto-discovers currently-valid model slugs from OpenRouter
  4. 6-hour Redis cache — repeat questions = zero LLM calls
  5. Clear error messages telling you what to do when things fail
"""

import os
import json
import time
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List

import requests
import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

load_dotenv()

CHROMA_DIR  = os.getenv("CHROMA_DIR", "knowledge_base/chroma")
COLLECTION  = "optimaai_knowledge"
EMBED_MODEL = "all-MiniLM-L6-v2"

# ── Cache config ──────────────────────────────────────────────────────────
TTL_RAG_SECONDS = int(os.getenv("RAG_CACHE_TTL", "21600"))   # 6 hours

# ── Free model pools ──────────────────────────────────────────────────────
# Order matters: non-reasoning instruction-followers first, reasoning-style
# models LAST. Reasoning models like DeepSeek-V3 burn the token budget on
# visible chain-of-thought ("We need to..." / "Let me think...") and on
# long sections (Headline, Priority Actions) they get cut off before
# producing the actual answer. Putting them last means we only fall back
# to them when everything else is rate-limited.
'''OPENROUTER_MODELS = [
    # Tier 1 — clean, terse instruction-following (preferred)
    "meta-llama/llama-3.3-70b-instruct:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "google/gemma-3-27b-it:free",
    "google/gemma-4-31b-it:free",
    "google/gemma-4-26b-a4b-it:free",
    # Tier 2 — capable but reasoning-style; keep as last-resort fallbacks
    "nvidia/nemotron-3-super-120b-a12b:free",
    "deepseek/deepseek-chat-v3-0324:free",
]'''

# Groq — 14,400 req/day per model free tier, different infrastructure
GROQ_MODELS = [
    "meta-llama/llama-4-scout-17b-16e-instruct",  # newest, ~750 t/s, instruction-tuned
    "llama-3.3-70b-versatile",                     # quality flagship, ~280 t/s
    "llama-3.1-8b-instant",                        # fastest fallback, ~560 t/s
]

# ── Live OpenRouter model list cache ──────────────────────────────────────
_available_models: Optional[set] = None
_available_models_fetched_at: float = 0
_MODEL_LIST_TTL = 3600

os.makedirs(CHROMA_DIR, exist_ok=True)

_client     = None
_collection = None


def _get_collection():
    global _client, _collection
    if _collection is None:
        _client = chromadb.PersistentClient(path=CHROMA_DIR)
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=EMBED_MODEL
        )
        _collection = _client.get_or_create_collection(
            name=COLLECTION,
            embedding_function=ef,
            metadata={"hnsw:space": "cosine"},
        )
        print(f"  [kb] Collection ready. Docs: {_collection.count()}")
    return _collection


# ══════════════════════════════════════════════════════
#  INGESTION
# ══════════════════════════════════════════════════════

def ingest_text(text: str, source: str = "manual", category: str = "general",
                chunk_size: int = 400, chunk_overlap: int = 80) -> dict:
    col    = _get_collection()
    chunks = _split_text(text, chunk_size, chunk_overlap)
    if not chunks:
        return {"status": "error", "message": "No content to ingest"}

    ids, docs, metas = [], [], []
    for i, chunk in enumerate(chunks):
        doc_id = _make_id(source, i, chunk)
        ids.append(doc_id)
        docs.append(chunk)
        metas.append({
            "source":    source,
            "category":  category,
            "chunk_idx": i,
            "ingested":  datetime.utcnow().isoformat(),
        })
    col.upsert(ids=ids, documents=docs, metadatas=metas)
    print(f"  [kb] Ingested {len(chunks)} chunks from '{source}' (category={category})")
    return {"status": "ok", "source": source, "chunks": len(chunks), "category": category}


def ingest_file(
    file_path: str,
    category: str = "general",
    source: Optional[str] = None,
) -> dict:
    """
    Ingest a file into the knowledge base.

    Args:
        file_path: Path to the file on disk (may be a temp path).
        category: KB category tag.
        source:   Display name for the source. If None, falls back to the
                  file's basename. Pass the original upload filename here
                  to avoid storing temp filenames (e.g. tmp3ib4kp72.csv).
    """
    path = Path(file_path)
    if not path.exists():
        return {"status": "error", "message": f"File not found: {file_path}"}
    display_name = source or path.name
    ext  = path.suffix.lower()
    text = ""
    if ext in (".txt", ".md"):
        text = path.read_text(encoding="utf-8", errors="ignore")
    elif ext == ".csv":
        import pandas as pd
        df   = pd.read_csv(file_path)
        text = _dataframe_to_text(df, display_name)
    elif ext in (".xlsx", ".xls"):
        import pandas as pd
        sheets = pd.read_excel(file_path, sheet_name=None)
        parts  = []
        for name, df in sheets.items():
            parts.append(f"=== Sheet: {name} ===\n" + _dataframe_to_text(df, name))
        text = "\n\n".join(parts)
    elif ext == ".pdf":
        try:
            from pypdf import PdfReader
            reader = PdfReader(file_path)
            text   = "\n".join(p.extract_text() or "" for p in reader.pages)
        except ImportError:
            return {"status": "error", "message": "Run: pip install pypdf"}
    elif ext == ".json":
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        text = json.dumps(data, indent=2, default=str)
    else:
        return {"status": "error", "message": f"Unsupported: {ext}"}
    if not text.strip():
        return {"status": "error", "message": "File is empty"}
    return ingest_text(text, source=display_name, category=category)


def ingest_dataframe(df, source: str, category: str = "sales") -> dict:
    return ingest_text(_dataframe_to_text(df, source), source=source, category=category)


def ingest_kpi_snapshot(kpis: dict, label: str = "latest_ml_results") -> dict:
    """
    Ingest the latest ML evaluation snapshot into the KB.

    IMPORTANT: This first deletes any previous chunks tagged with the same
    label, so the KB only ever contains the most recent training run.
    Without this dedup step, every retrain leaves a fresh snapshot AND
    keeps the stale one, which causes the LLM to retrieve both and report
    contradictory numbers (e.g. "model A predicts $843, model B predicts
    $691"  -- they are the same model, just different training runs).
    """
    # Step 1 -- purge any stale snapshot under this label
    try:
        deleted = delete_source(label)
        if deleted.get("deleted", 0) > 0:
            print(f"  [kb] Removed {deleted['deleted']} stale '{label}' chunk(s) before re-ingesting")
    except Exception as e:
        print(f"  [kb] Could not pre-clean '{label}' (continuing): {e}")

    # Step 2 -- compose the fresh snapshot text
    text  = f"OptimaAi ML Performance -- {datetime.utcnow().date()}\n\n"
    rev   = kpis.get("revenue_model", {})
    churn = kpis.get("churn_model",   {})
    grow  = kpis.get("growth_model",  {})
    prop  = kpis.get("prophet_model", {})
    text += f"REVENUE: MAPE={rev.get('MAPE_pct')}%  R2={rev.get('R2')}  Bias={rev.get('forecast_bias')}\n"
    feats = rev.get("top_features", [])
    if feats:
        text += "Revenue drivers: " + ", ".join(
            f['feature'] if isinstance(f, dict) else str(f) for f in feats[:5]) + "\n"
    fc = rev.get("12_period_forecast", [])
    if fc:
        text += f"12-period avg forecast: ${sum(fc)/len(fc):,.2f}\n"
    text += f"\nCHURN: ROC-AUC={churn.get('ROC_AUC')}  Rate={churn.get('churn_rate_pct')}%\n"
    cf = churn.get("top_features", [])
    if cf:
        text += "Churn predictors: " + ", ".join(
            f['feature'] if isinstance(f, dict) else str(f) for f in cf[:5]) + "\n"
    text += f"\nGROWTH: MAPE={grow.get('MAPE_pct')}%  R2={grow.get('R2')}\n"
    text += f"PROPHET: MAPE={prop.get('MAPE_pct')}%\n"
    return ingest_text(text, source=label, category="ml_results")


# ══════════════════════════════════════════════════════
#  QUERY
# ══════════════════════════════════════════════════════

def query_knowledge_base(question: str, category: Optional[str] = None,
                         n_results: int = 5, min_relevance: float = 0.25) -> dict:
    col = _get_collection()
    total = col.count()
    if total == 0:
        return {"chunks": [], "context": "", "count": 0, "status": "empty_kb"}

    where = {"category": {"$eq": category}} if category else None
    try:
        results = col.query(
            query_texts=[question], n_results=min(n_results, total),
            where=where, include=["documents", "metadatas", "distances"],
        )
    except Exception as e:
        print(f"  [kb] Query failed ({e}) -- retrying without category filter")
        results = col.query(
            query_texts=[question], n_results=min(n_results, total),
            include=["documents", "metadatas", "distances"],
        )

    chunks    = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]
    if not chunks:
        return {"chunks": [], "context": "", "count": 0, "status": "no_match"}

    MAX_CHUNK_CHARS = 1200
    context_parts, sources_used = [], []
    for chunk, meta, dist in zip(chunks, metadatas, distances):
        relevance = 1 - dist
        if relevance < min_relevance:
            continue
        trimmed = _trim_at_sentence(chunk, MAX_CHUNK_CHARS)
        context_parts.append(
            f"[Source: {meta.get('source','?')} | "
            f"Relevance: {round(relevance*100,1)}%]\n{trimmed}"
        )
        sources_used.append(meta.get('source', '?'))

    if not context_parts:
        return {"chunks": [], "context": "", "count": 0, "status": "no_relevant_match"}

    return {
        "chunks":  chunks, "sources": sources_used,
        "context": "\n\n---\n\n".join(context_parts),
        "count":   len(context_parts), "status": "ok",
    }


# ══════════════════════════════════════════════════════
#  ML INTENT ROUTER  --  serves concrete numbers from models
# ══════════════════════════════════════════════════════

# Keyword lists tuned to the evaluation_results.json shape
_ML_INTENT_KEYWORDS = {
    "churn": [
        "churn", "retention", "attrition", "cancel", "leave",
        "churn rate", "churn risk",
    ],
    "revenue_forecast": [
        "revenue forecast", "forecasted revenue", "predict revenue",
        "revenue prediction", "sales forecast", "next month revenue",
        "next period revenue", "12 month revenue", "12 period revenue",
    ],
    "growth": [
        "growth forecast", "growth projection", "growth rate",
        "growth prediction", "next year growth", "monthly growth",
        "revenue next year", "next 12 months",
    ],
    "model_accuracy": [
        "how accurate", "model performance", "mape", "r2", "r-squared",
        "how reliable", "how good is the model", "roc-auc", "f1 score",
        "precision", "recall",
    ],
    "top_features": [
        "top feature", "most important feature", "what drives",
        "main driver", "key predictor", "feature importance",
        "what influences", "most predictive",
    ],
}


def _detect_ml_intent(question: str) -> Optional[str]:
    """Return the ML intent category, or None if this is a general question."""
    q = question.lower()
    # Revenue forecast vs growth — growth wins if both words appear with "month"/"year"
    for intent, keywords in _ML_INTENT_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return intent
    return None


def _format_provenance_block(report: dict, kind: str) -> str:
    """
    Render the relevant fields from a transparency report (data profile or
    churn-label report) as plain-English context for the LLM.

    `kind` is 'profile' (revenue/growth profilers) or 'labelling' (churn labeller).
    Returns an empty string when no report is present, so callers can prepend
    the result unconditionally.
    """
    if not report:
        return ""
    lines = ["DATA QUALITY CONTEXT (the LLM should use these EXACT terms when "
             "describing how the metric was computed):"]
    strat = report.get("strategy")
    conf  = report.get("confidence")
    if strat or conf:
        lines.append(f"- Strategy: {strat}  (data confidence: {conf})")
    if report.get("definition"):
        lines.append(f"- Definition: {report['definition']}")

    if kind == "labelling":
        # Cohort segmentation — explains why total_customers != trained-on count
        for k, label in [
            ("total_customers",    "Total customers"),
            ("one_time_buyers",    "One-time buyers (excluded)"),
            ("new_customers",      "Too new to label (excluded)"),
            ("eligible_customers", "Eligible cohort labelled"),
            ("churned_customers",  "Labelled as churned"),
        ]:
            if k in report and report[k] is not None:
                lines.append(f"- {label}: {report[k]:,}")
        if report.get("cutoff_date"):
            lines.append(f"- Holdout cutoff date: {report['cutoff_date']}  "
                         f"(observation window: {report.get('holdout_window_days', '?')} days)")
        if report.get("median_ipi_days") is not None:
            lines.append(f"- Median inter-purchase interval: {report['median_ipi_days']} days")

    if kind == "profile":
        if report.get("recommended_aggregation"):
            lines.append(f"- Recommended aggregation grain: {report['recommended_aggregation']}")
        if report.get("safe_horizon_months") is not None:
            lines.append(f"- Defensible forecast horizon: {report['safe_horizon_months']} months")
        if report.get("coefficient_of_variation") is not None:
            lines.append(f"- Per-row coefficient of variation: {report['coefficient_of_variation']}  "
                         "(higher = noisier per-row predictions, normal for mixed order sizes)")

    if report.get("suitable_for"):
        lines.append("- SUITABLE FOR: " + "; ".join(report["suitable_for"]))
    if report.get("not_suitable_for"):
        lines.append("- NOT SUITABLE FOR: " + "; ".join(report["not_suitable_for"]))
    return "\n".join(lines)


def _format_revenue_ml(kpis: dict) -> str:
    rev = kpis.get("revenue_model", {})
    if not rev:
        return ""
    fc = rev.get("12_period_forecast", [])
    avg_fc = (sum(fc) / len(fc)) if fc else None
    top = ", ".join(
        f"{f['feature']} ({f['importance']})"
        for f in rev.get("top_features", [])[:5]
    )
    lines = [
        "REVENUE FORECAST MODEL (gradient-boosted regression):",
        f"- Target: {rev.get('target', 'revenue')}",
        f"- MAPE: {rev.get('MAPE_pct', 'N/A')}% (lower is better; <5% is excellent)",
        f"- R squared: {rev.get('R2', 'N/A')}",
        f"- RMSE: {rev.get('RMSE', 'N/A')}",
        f"- Forecast bias: {rev.get('forecast_bias', 'N/A')}",
        f"- Trained on {rev.get('train_rows', '?'):,} rows, tested on {rev.get('test_rows', '?'):,}",
    ]
    if avg_fc is not None:
        lines.append(
            f"- Next 12 periods forecast: avg ${avg_fc:,.2f}/period, "
            f"range ${min(fc):,.2f}-${max(fc):,.2f}"
        )
    if top:
        lines.append(f"- Top 5 revenue drivers: {top}")

    # Prepend the profiler's data-quality context so the LLM grounds its
    # narrative in HOW the metrics were produced, not just the metrics themselves.
    provenance = _format_provenance_block(rev.get("data_profile"), kind="profile")
    if provenance:
        return provenance + "\n\n" + "\n".join(lines)
    return "\n".join(lines)


def _format_churn_ml(kpis: dict) -> str:
    churn = kpis.get("churn_model", {})
    if not churn:
        return ""
    top = ", ".join(
        f"{f['feature']} ({f['importance']})"
        for f in churn.get("top_features", [])[:5]
    )
    lines = [
        "CUSTOMER CHURN MODEL (classification):",
        f"- Dataset churn rate: {churn.get('churn_rate_pct', 'N/A')}%",
        f"- ROC-AUC: {churn.get('ROC_AUC', 'N/A')} (>0.8 is strong)",
        f"- F1 score: {churn.get('F1_Score', 'N/A')}",
        f"- Precision: {churn.get('Precision', 'N/A')}",
        f"- Recall: {churn.get('Recall', 'N/A')}",
        f"- Trained on {churn.get('train_rows', '?'):,} customers, tested on {churn.get('test_rows', '?'):,}",
    ]
    if top:
        lines.append(f"- Top 5 churn predictors: {top}")

    # The labelling report tells the LLM HOW the rate was calculated — without
    # this, a 50% rate looks alarming; with the cutoff/holdout context it's
    # an observed business fact about a defined cohort.
    provenance = _format_provenance_block(churn.get("labelling"), kind="labelling")
    if provenance:
        return provenance + "\n\n" + "\n".join(lines)
    return "\n".join(lines)


def _format_growth_ml(kpis: dict) -> str:
    grow = kpis.get("growth_model", {})
    if not grow:
        return ""
    # Prefer the new horizon-correct 'forecast' key; fall back to legacy alias.
    fc = grow.get("forecast") or grow.get("12_month_forecast", [])
    horizon = grow.get("forecast_horizon_months") or len(fc)
    top = ", ".join(
        f"{f['feature']} ({f['importance']})"
        for f in grow.get("top_features", [])[:5]
    )
    lines = [
        "GROWTH FORECAST MODEL:",
        f"- Target: {grow.get('target', 'growth')}",
        f"- MAPE: {grow.get('MAPE_pct', 'N/A')}%",
        f"- R squared: {grow.get('R2', 'N/A')}",
        f"- RMSE: {grow.get('RMSE', 'N/A')}",
        f"- Trained on {grow.get('train_months', '?')} months, tested on {grow.get('test_months', '?')}",
    ]
    if fc:
        forecast_strs = [
            f"{p.get('period')}: ${p.get('forecast', 0):,.0f}"
            for p in fc[:horizon]
        ]
        lines.append(f"- Next {len(forecast_strs)} months forecast:")
        lines.extend(f"    {s}" for s in forecast_strs)
    if top:
        lines.append(f"- Top 5 growth drivers: {top}")

    provenance = _format_provenance_block(grow.get("data_profile"), kind="profile")
    if provenance:
        return provenance + "\n\n" + "\n".join(lines)
    return "\n".join(lines)


def _answer_from_ml(question: str, intent: str, role: str) -> Optional[dict]:
    """Build an answer from the latest evaluation_results.json."""
    try:
        from app.services.ml_bridge import load_latest_evaluation
        kpis = load_latest_evaluation()
    except Exception as e:
        print(f"  [ml-router] No ML artifact available ({e}) -- falling back to RAG")
        return None

    sections = []
    if intent == "churn":
        sections.append(_format_churn_ml(kpis))
    elif intent == "revenue_forecast":
        sections.append(_format_revenue_ml(kpis))
    elif intent == "growth":
        sections.append(_format_growth_ml(kpis))
    elif intent == "model_accuracy":
        # Give the user all three models' performance
        sections.append(_format_revenue_ml(kpis))
        sections.append(_format_churn_ml(kpis))
        sections.append(_format_growth_ml(kpis))
    elif intent == "top_features":
        # Return feature importances for all three models
        sections.append(_format_revenue_ml(kpis))
        sections.append(_format_churn_ml(kpis))
        sections.append(_format_growth_ml(kpis))

    sections = [s for s in sections if s]
    if not sections:
        return None

    context = "\n\n".join(sections)
    prompt = f"""You are a {role} for OptimaAi, an ML-powered business analytics platform.

Answer the user's question using the trained ML model results below.
Quote specific numbers from the results -- do not be vague. If the question
asks for a recommendation, base it on the actual metrics.

Format in three sections:
**Finding** -- the direct answer with specific numbers from the models.
**Evidence** -- which model the numbers come from (revenue, churn, or growth).
**Recommendation** -- one concrete business action based on the numbers.

Keep each section to 2-4 sentences. Do NOT stop mid-sentence.

-- ML MODEL RESULTS -----------------------------------------
{context}
-------------------------------------------------------------

QUESTION: {question}
"""
    answer, model_used, err = _call_llm(prompt)
    if answer is None:
        return None
    return {
        "status":      "success",
        "answer":      answer.strip(),
        "sources":     ["ML Model Results"],
        "chunks_used": len(sections),
        "model":       model_used,
        "question":    question,
        "cached":      False,
        "ml_intent":   intent,
    }


# ══════════════════════════════════════════════════════
#  CASUAL MESSAGE DETECTOR + CONVERSATIONAL REPLY
# ══════════════════════════════════════════════════════
#
# When the user types a greeting or small-talk message (rather than a
# question about their data), we want a brief friendly reply -- not the
# full RAG pipeline with disclaimers about general knowledge. These
# helpers detect such messages and produce a short reply.

_CASUAL_PATTERNS = [
    r"^\s*(hi|hello|hey|yo|sup|howdy|hola|salam|salaam)\b",
    r"^\s*good\s*(morning|afternoon|evening|night)",
    r"\bhow\s+are\s+you\b",
    r"\bhow's?\s+it\s+going\b",
    r"\bwhat'?s?\s+up\b",
    r"^\s*thanks?\b",
    r"^\s*thank\s+you\b",
    r"^\s*(ok|okay|cool|nice|great|awesome|got\s+it)\s*$",
    r"^\s*who\s+are\s+you\b",
    r"^\s*what\s+can\s+you\s+do\b",
    r"^\s*help\s*$",
]

import re as _casual_re
_CASUAL_RE = _casual_re.compile("|".join(_CASUAL_PATTERNS), _casual_re.IGNORECASE)


def _is_casual_message(question: str) -> bool:
    """True if the user is making conversation rather than asking a real
    data question. We keep the length cap tight so a long question that
    happens to contain 'how are you' as part of a clause doesn't get
    misclassified."""
    q = (question or "").strip()
    if not q:
        return False
    if len(q) >= 80:
        return False
    return bool(_CASUAL_RE.search(q))


def _conversational_reply(question: str, role: str) -> dict:
    """Short, friendly reply for casual messages. Goes through the same
    LLM provider chain as RAG so we benefit from existing retries and
    fallbacks, but with a tiny prompt and natural tone -- and zero
    pretence that this is a data answer."""
    prompt = (
        f"You are OptimaAi, a friendly business analytics assistant. "
        f"The user just said: \"{question}\"\n\n"
        "This is a casual greeting or small-talk message, NOT a data "
        "question. Reply briefly (1-2 sentences) and warmly. Then gently "
        "remind them you can analyse their uploaded business data when "
        "they're ready -- only if it fits naturally. Use plain prose, "
        "no markdown headers, no bullet lists, no preamble like 'Sure!'. "
        "Just a normal conversational reply."
    )

    answer, model_used, _err = _call_llm(prompt)
    if not answer:
        # LLM unavailable -- canned fallback so the user always gets a reply
        answer = (
            "Hi! I'm OptimaAi. I'm here to help you analyse your uploaded "
            "business data — ask me anything about revenue, customers, "
            "churn, or trends whenever you're ready."
        )

    return {
        "status":      "general_knowledge",
        "answer":      answer.strip(),
        "sources":     [],
        "chunks_used": 0,
        "model":       model_used,
        "question":    question,
        "cached":      False,
    }


# ══════════════════════════════════════════════════════
#  RAG ANSWER  --  Cache -> OpenRouter -> Groq
# ══════════════════════════════════════════════════════

def rag_answer(question: str, category: Optional[str] = None,
               role: str = "business analyst", n_results: int = 5) -> dict:

    # 0. Casual / off-topic message? Reply naturally without going through
    #    cache, ML routing, or RAG. Avoids the awkward "Note: this is based
    #    on general knowledge..." treatment when the user just says "hi".
    if _is_casual_message(question):
        return _conversational_reply(question, role)

    # 1. Cache lookup
    cached = _cache_lookup(question, role, category)
    if cached:
        cached["cached"] = True
        return cached

    # 1.5 ML intent routing -- if the user is asking about model outputs,
    #     serve concrete numbers from the latest trained models.
    intent = _detect_ml_intent(question)
    if intent:
        ml_result = _answer_from_ml(question, intent, role)
        if ml_result is not None:
            _cache_store(question, role, category, ml_result)
            return ml_result
        # If no evaluation artifact exists yet, fall through to RAG/general.

    # 2. Retrieve chunks
    retrieval = query_knowledge_base(question, category, n_results)
    no_ctx = retrieval["count"] == 0

    # 3. Build prompt -- two modes:
    #    a) grounded: context found, answer strictly from it
    #    b) fallback: nothing matched, answer from general knowledge
    if no_ctx:
        prompt = f"""You are a {role} for OptimaAi, an ML-powered business analytics platform.

The user asked a question, but there is no matching information in their
uploaded knowledge base. Answer using your general knowledge as an expert
{role}, and be helpful and concrete.

IMPORTANT: Start your answer with a short note that the response is based
on general knowledge, not on the user's data. Keep the total answer under
200 words, clear and practical. Use plain prose -- no markdown headings
required.

QUESTION: {question}
"""
    else:
        prompt = f"""You are a {role} for OptimaAi, an ML-powered business analytics platform.

Answer the user's question using the context below. The context contains
information from MULTIPLE data files -- read all of them and pick the most
relevant one for this specific question, even if the user mentioned a
different filename.

For example: if the user asks about "churn risk in the orders dataset",
but the orders dataset has no churn columns while another file
(e.g. a churn prediction file) does, answer from the file that has the
information, and explicitly state which file you used.

If NO file in the context has the needed information, say so plainly and
suggest which file type would help -- do not invent facts.

How to write the answer:
- Reply naturally and conversationally, like a smart colleague answering in
  chat. Address the user directly ("your data shows...", "looking at your
  customers...").
- Ground every claim in something specific from the context (a number, a
  column, a customer/product, a model metric). If you can't ground a claim,
  don't make it.
- Use **bold** for key numbers and findings. Use bullet points (with `- `)
  only when you genuinely have 3+ parallel items to list.
- Keep it concise -- 2 to 4 short paragraphs is usually right.
- If the context is thin or doesn't fully answer the question, say so honestly:
  "I don't have enough data to say X, but here's what I can tell from what's
  uploaded..."
- Do NOT use rigid section headers like "Finding:" / "Evidence:" /
  "Recommendation:". Just write prose.
- Do NOT add a "Note:" prefix or any disclaimer about general knowledge --
  if the data is in the context, use it; if it isn't, say so directly.

-- CONTEXT --------------------------------------------------
{retrieval["context"]}
-------------------------------------------------------------

QUESTION: {question}
"""

    # 4. Call LLM providers in order
    answer, model_used, err = _call_llm(prompt)
    if answer is None:
        return {
            "status":   "llm_error",
            "answer":   (f"All LLM providers are unreachable or rate-limited. "
                         f"Try again in a few minutes. Last error: {err}"),
            "sources":  retrieval.get("sources", []),
            "question": question,
            "context_preview": retrieval.get("context", "")[:500],
        }

    # 5. Cache + return
    result = {
        "status":      "success" if not no_ctx else "general_knowledge",
        "answer":      answer.strip(),
        "sources":     retrieval.get("sources", []),
        "chunks_used": retrieval["count"],
        "model":       model_used,
        "question":    question,
        "cached":      False,
    }
    _cache_store(question, role, category, result)
    return result


# ══════════════════════════════════════════════════════
#  LLM ROUTER  --  OpenRouter first, Groq fallback
# ══════════════════════════════════════════════════════

# Phrases that almost-always indicate the model is showing its work rather
# than producing a finished answer. If a response STARTS with one of these,
# the model is leaking its chain-of-thought into the section content and
# we should treat the response as a failure so the next model in the chain
# gets tried. This is what previously caused the Headline and Priority
# Actions sections of generated reports to fill with "We need to write..."
# instead of an actual headline.
_REASONING_LEAK_PREFIXES = (
    "we need to", "we must", "we should", "we have",
    "let me", "let's", "i need to", "i should", "i'll",
    "first, ", "first i", "okay,", "okay ",
    "sentence 1:", "sentence 1 ", "step 1:",
    "thinking:", "thought:", "reasoning:",
    "the user wants", "the user is asking", "the task",
    "to begin", "to start, ",
)

# Patterns that, even mid-response, indicate visible self-talk worth flagging
_REASONING_LEAK_DENSITY_TRIGGERS = (
    "we need to ", "we must ", "let's ", "let me ",
    "sentence 1:", "step 1:", "okay so ", "actually ",
)


def _strip_reasoning(text: str) -> str:
    """
    Strip <think> / <thinking> tags some reasoning models emit.
    Conservative: only removes content INSIDE the tags, leaves the rest
    untouched. Returns the cleaned text.
    """
    if not text:
        return text
    import re
    # Remove <think>...</think> and <thinking>...</thinking> blocks
    text = re.sub(r"<think(?:ing)?>[\s\S]*?</think(?:ing)?>", "", text,
                   flags=re.IGNORECASE)
    # Some models output a closing </think> with no opening one (output
    # got cut off mid-think); drop everything before it.
    if "</think>" in text.lower():
        idx = text.lower().rfind("</think>")
        text = text[idx + len("</think>"):]
    return text.strip()


def _looks_like_reasoning_leak(text: str) -> bool:
    """
    Decide whether `text` is the model's chain-of-thought leaking into
    the answer rather than the answer itself. Used as a quality gate
    BEFORE accepting a model's response.
    """
    if not text:
        return False
    head = text.lstrip().lower()[:300]
    if any(head.startswith(p) for p in _REASONING_LEAK_PREFIXES):
        return True
    # Density check: if the first 600 chars contain 3+ chain-of-thought
    # markers, the model is thinking out loud throughout
    snippet = text[:600].lower()
    hits = sum(1 for p in _REASONING_LEAK_DENSITY_TRIGGERS if p in snippet)
    return hits >= 3


def _accept_or_reject(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Apply tag stripping + reasoning-leak detection. Returns
    (clean_text, None) on accept, or (None, reason_string) on reject.
    The router uses the reject reason to log why it skipped a model.
    """
    if not text or not text.strip():
        return None, "empty response"
    cleaned = _strip_reasoning(text)
    if not cleaned or len(cleaned) < 30:
        return None, "response too short after stripping reasoning tags"
    if _looks_like_reasoning_leak(cleaned):
        return None, "response is chain-of-thought, not finished prose"
    return cleaned, None


def _call_llm(prompt: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    last_err = None

    if os.getenv("OPENROUTER_API_KEY"):
        ans, model, err = _call_openrouter(prompt)
        if ans:
            return ans, model, None
        last_err = err
        print(f"  [kb] OpenRouter exhausted -- falling to Groq")

    if os.getenv("GROQ_API_KEY"):
        ans, model, err = _call_groq(prompt)
        if ans:
            return ans, model, None
        last_err = err

    return None, None, last_err or "no LLM provider configured"


# ── OpenRouter ────────────────────────────────────────────────────────────

def _refresh_available_models() -> set:
    global _available_models, _available_models_fetched_at
    try:
        r = requests.get("https://openrouter.ai/api/v1/models", timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [])
            _available_models = {m["id"] for m in data}
            _available_models_fetched_at = time.time()
            print(f"  [kb] Cached {len(_available_models)} OpenRouter models")
            return _available_models
    except Exception as e:
        print(f"  [kb] Could not fetch model list: {e}")
    return set()


def _get_live_models() -> set:
    global _available_models, _available_models_fetched_at
    if (_available_models is None or
        time.time() - _available_models_fetched_at > _MODEL_LIST_TTL):
        return _refresh_available_models()
    return _available_models


def _call_openrouter(prompt: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return None, None, "OPENROUTER_API_KEY not set"

    live = _get_live_models()
    models_to_try: List[str] = (
        [m for m in OPENROUTER_MODELS if not live or m in live]
        or OPENROUTER_MODELS
    )
    if not models_to_try:
        return None, None, "All OpenRouter models deprecated"

    last_err = None
    for model in models_to_try:
        try:
            r = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "HTTP-Referer":  "https://optimaai.app",
                    "X-Title":       "OptimaAi RAG",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       model,
                    "messages":    [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                    "max_tokens":  2000,
                },
                timeout=45,
            )
            if r.status_code == 200:
                data = r.json()
                choices = data.get("choices") or []
                if choices and choices[0].get("message", {}).get("content"):
                    out = choices[0]["message"]["content"].strip()
                    cleaned, reject_reason = _accept_or_reject(out)
                    if cleaned:
                        print(f"  [kb] OpenRouter {model} OK")
                        return cleaned, model, None
                    last_err = f"{model}: {reject_reason}"
                    print(f"  [kb] {last_err} -- rotating")
                    continue
                last_err = f"{model}: empty response"
            elif r.status_code == 404:
                last_err = f"{model}: slug deprecated"
                print(f"  [kb] {last_err} -- skipping")
                continue
            elif r.status_code == 429:
                last_err = f"{model}: rate-limited"
                print(f"  [kb] {last_err} -- rotating")
                continue
            elif r.status_code == 402:
                return None, None, "OpenRouter needs a credit balance. Load $5 at openrouter.ai/credits"
            elif r.status_code == 401:
                return None, None, "OpenRouter API key invalid"
            else:
                last_err = f"{model}: HTTP {r.status_code} {r.text[:150]}"
                print(f"  [kb] {last_err}")
        except requests.Timeout:
            last_err = f"{model}: timeout"
        except Exception as e:
            last_err = f"{model}: {e}"

    return None, None, last_err or "all OpenRouter models failed"


# ── Groq ──────────────────────────────────────────────────────────────────

def _call_groq(prompt: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None, None, "GROQ_API_KEY not set"

    last_err = None
    for model in GROQ_MODELS:
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       model,
                    "messages":    [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                    "max_tokens":  2000,
                },
                timeout=45,
            )
            if r.status_code == 200:
                data = r.json()
                choices = data.get("choices") or []
                if choices and choices[0].get("message", {}).get("content"):
                    out = choices[0]["message"]["content"].strip()
                    cleaned, reject_reason = _accept_or_reject(out)
                    if cleaned:
                        print(f"  [kb] Groq {model} OK")
                        return cleaned, f"groq/{model}", None
                    last_err = f"groq/{model}: {reject_reason}"
                    print(f"  [kb] {last_err} -- rotating")
                    continue
                last_err = f"groq/{model}: empty response"
            elif r.status_code == 429:
                last_err = f"groq/{model}: rate-limited"
                print(f"  [kb] {last_err} -- rotating")
                continue
            elif r.status_code == 401:
                return None, None, "Groq API key invalid"
            else:
                last_err = f"groq/{model}: HTTP {r.status_code} {r.text[:150]}"
                print(f"  [kb] {last_err}")
        except requests.Timeout:
            last_err = f"groq/{model}: timeout"
        except Exception as e:
            last_err = f"groq/{model}: {e}"

    return None, None, last_err or "all Groq models failed"


# ══════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════

def _cache_key(question: str, role: str, category: Optional[str]) -> str:
    raw = f"{question}|{role}|{category or '_'}".encode()
    return f"optimaai:rag:{hashlib.md5(raw).hexdigest()}"


def _cache_lookup(question: str, role: str, category: Optional[str]) -> Optional[dict]:
    try:
        from app.cache import cache_get
        return cache_get(_cache_key(question, role, category))
    except Exception:
        return None


def _cache_store(question: str, role: str, category: Optional[str], result: dict) -> None:
    try:
        from app.cache import cache_set
        cache_set(_cache_key(question, role, category), result, TTL_RAG_SECONDS)
    except Exception:
        pass


# ══════════════════════════════════════════════════════
#  MANAGEMENT + HELPERS
# ══════════════════════════════════════════════════════

def kb_stats() -> dict:
    col   = _get_collection()
    count = col.count()
    if count == 0:
        return {"total_chunks": 0, "sources": [], "categories": []}
    all_items = col.get(include=["metadatas"])
    sources   = list({m.get("source", "?")   for m in all_items["metadatas"]})
    cats      = list({m.get("category", "?") for m in all_items["metadatas"]})
    return {"total_chunks": count, "sources": sorted(sources), "categories": sorted(cats)}


def delete_source(source_name: str) -> dict:
    _get_collection().delete(where={"source": {"$eq": source_name}})
    return {"status": "deleted", "source": source_name}


def clear_knowledge_base() -> dict:
    global _collection, _client
    try:
        if _client is None:
            _get_collection()
        _client.delete_collection(COLLECTION)
    except Exception:
        pass
    _collection = None
    _client = None
    _get_collection()
    return {"status": "cleared"}


def _split_text(text: str, chunk_size: int, overlap: int) -> list:
    words = text.split()
    if not words:
        return []
    chunks = []
    step = max(1, chunk_size - overlap)
    for i in range(0, len(words), step):
        chunk = " ".join(words[i:i + chunk_size])
        if len(chunk.strip()) > 30:
            chunks.append(chunk)
        if i + chunk_size >= len(words):
            break
    return chunks


def _make_id(source: str, idx: int, text: str) -> str:
    h = hashlib.md5(f"{source}_{idx}_{text[:50]}".encode()).hexdigest()[:8]
    return f"{source}_{idx}_{h}"


def _trim_at_sentence(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    for sep in (". ", ".\n", "! ", "? "):
        pos = cut.rfind(sep)
        if pos > max_chars * 0.6:
            return cut[:pos + 1].strip() + " ..."
    return cut.strip() + " ..."


def _empty_message(status: str, category: Optional[str]) -> str:
    if status == "empty_kb":
        return "The knowledge base is empty. Upload a CSV/PDF or run ML training first."
    if status == "no_match" and category:
        return (f"No documents tagged '{category}' match your question. "
                f"Try removing the category filter or uploading data in that category.")
    if status == "no_relevant_match":
        return ("Found documents but none are relevant enough to this question. "
                "Try rephrasing or adding more specific source data.")
    return "No relevant context found."


def _dataframe_to_text(df, name: str) -> str:
    columns_str = ", ".join(str(c) for c in df.columns[:20])

    # ── Automatic topic detection from column names ────────────────────
    # Helps embedding search match queries like "churn risk" to the right
    # file, even if the query doesn't mention the filename.
    cols_lower = " ".join(str(c).lower() for c in df.columns)
    topics = []
    TOPIC_KEYWORDS = {
        "customer churn and retention risk":
            ["churn", "retention", "cancel", "attrition"],
        "revenue forecasting and sales projections":
            ["revenue", "forecast", "projection", "sales"],
        "growth metrics and subscription KPIs":
            ["growth", "mrr", "arr", "subscription"],
        "order transactions and ecommerce activity":
            ["order", "purchase", "transaction", "cart"],
        "customer demographics and profiles":
            ["age", "gender", "country", "segment", "customer"],
        "product catalogue and pricing":
            ["product", "sku", "price", "category"],
        "payment and billing":
            ["payment", "invoice", "billing", "method"],
    }
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw in cols_lower for kw in keywords):
            topics.append(topic)
    topic_line = (
        f"Topics: {'; '.join(topics)}." if topics
        else "Topics: general business data."
    )

    lines = [
        f"Dataset: {name}",
        f"This file contains data about: {', '.join(topics) if topics else 'general business activity'}.",
        topic_line,
        f"Shape: {len(df):,} rows x {len(df.columns)} columns",
        f"Columns: {columns_str}",
        "",
    ]
    num_cols = df.select_dtypes(include=["number"]).columns
    if len(num_cols):
        lines.append("NUMERIC SUMMARY:")
        for col in num_cols[:8]:
            try:
                lines.append(
                    f"  {col}: min={df[col].min():.2f} "
                    f"max={df[col].max():.2f} "
                    f"mean={df[col].mean():.2f}"
                )
            except Exception:
                lines.append(f"  {col}: numeric column")
        lines.append("")
    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols[:3]:
        top = df[col].value_counts().head(3)
        lines.append(f"TOP {col}: " + ", ".join(f"{v}({c})" for v, c in top.items()))
    lines.append("")
    lines.append("SAMPLE (first 10 rows):")
    lines.append(df.head(10).to_string(index=False))
    return "\n".join(lines)