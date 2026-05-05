"""
app/services/recommendation_service.py  —  v2

Generates a brief, role-targeted recommendation from the latest ML KPIs.

WHAT CHANGED vs v1:
  - Routes through knowledge_base._call_llm instead of a one-shot LangChain
    call. That gives this endpoint the same resilience the report service
    already has:
        * Multi-model fallback inside Groq (llama-4-scout → llama-3.3-70b
          → llama-3.1-8b-instant)
        * Cross-provider fallback (OpenRouter → Groq when both keys set)
        * Chain-of-thought leak detection (rejects responses that start
          with "We need to..." / "Let me think..." and rotates to the
          next model automatically)
        * 2000-token output budget — enough for the longer recommendation
          structures without truncation
  - Uses the new transparency-aware ML formatters from knowledge_base
    (_format_revenue_ml / _format_churn_ml / _format_growth_ml). When
    the trainer attached `data_profile` and `labelling` blocks to the
    evaluation results, the prompt now includes the labeller's plain-
    English churn definition, the profilers' suitable-for / not-suitable-
    for lists, and the safe forecast horizon — same context the report
    layer uses, so recommendations stay consistent with reports.
  - Drops LangChain dependency entirely. The prompt is now a plain
    f-string. langchain_openai pulled in a lot of transitive packages
    just to make one HTTP call.
  - Adds an explicit OUTPUT CONTRACT block forbidding chain-of-thought
    spillage — the same anti-reasoning guard that fixed the report's
    Headline / Priority Actions sections.

BACKWARD COMPAT:
  generate_recommendation(role, kpis) keeps its signature. It accepts
  EITHER the new full kpis dict (with `revenue_model`, `churn_model`,
  `growth_model` keys) OR v1's flat dict (with `mape`, `churn_rate`,
  etc.). It auto-detects which one was passed, so api.py keeps working
  whether you migrate it or not.

ENV CONFIG:
  Set GROQ_API_KEY for Groq-only mode. If you also set OPENROUTER_API_KEY,
  the router tries OpenRouter first (and falls back to Groq). To force
  Groq-only, comment out OPENROUTER_API_KEY in .env.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

from app.services.knowledge_base import (
    _call_llm,
    _format_revenue_ml,
    _format_churn_ml,
    _format_growth_ml,
)

load_dotenv()


# ── Role-aware framing ─────────────────────────────────────────────────────
# Used to tilt the prompt toward the audience without re-templating the
# whole thing. Keys match the role strings the API layer typically sends.
ROLE_GUIDANCE = {
    "executive":
        "Focus on outcomes leadership cares about: customer attrition, "
        "revenue trajectory, and decisions that need to be made this quarter.",
    "Chief Executive Officer":
        "Focus on outcomes leadership cares about: customer attrition, "
        "revenue trajectory, and decisions that need to be made this quarter.",
    "Sales Manager":
        "Focus on what frontline sales managers can act on today: "
        "pricing decisions, account-level retention, and pipeline health.",
    "Operations":
        "Focus on operational implications: capacity planning, fulfilment "
        "risk, and how staff/inventory should respond to the forecast.",
    "Marketing":
        "Focus on acquisition and retention spend: where to push, where "
        "to pull back, and which customer segments are most at risk.",
}


# ═══════════════════════════════════════════════════════════════════════════
#   PROMPT ASSEMBLY
# ═══════════════════════════════════════════════════════════════════════════

def _build_ml_context(kpis: dict) -> str:
    """
    Build the ML data block embedded in the prompt.

    Auto-detects whether `kpis` is the new full dict (preferred — has
    `revenue_model` / `churn_model` / `growth_model` keys with rich
    transparency reports inside) or v1's flat dict (legacy from older
    api.py code that flattened metrics before passing in).
    """
    is_full = any(
        k in kpis for k in ("revenue_model", "churn_model", "growth_model")
    )
    if is_full:
        parts: list[str] = []
        for fn in (_format_revenue_ml, _format_churn_ml, _format_growth_ml):
            block = fn(kpis)
            if block:
                parts.append(block)
        return "\n\n".join(parts)

    # Legacy flat dict — keep older api.py paths working
    return (
        f"- Forecast accuracy (MAPE): {kpis.get('mape', 'N/A')}%\n"
        f"- Forecast bias: {kpis.get('forecast_bias', 'N/A')}\n"
        f"- Customer churn rate: {kpis.get('churn_rate', 'N/A')}%\n"
        f"- Revenue variance vs forecast: {kpis.get('revenue_variance', 'N/A')}\n"
        f"- Growth trend: {kpis.get('growth_trend', 'N/A')}"
    )


def _build_prompt(role: str, ml_context: str) -> str:
    role_focus = ROLE_GUIDANCE.get(role, ROLE_GUIDANCE["executive"])

    return f"""You are an AI advisor for OptimaAi, an analytics platform.
You are writing a brief recommendation for a {role}.

OUTPUT CONTRACT:
- Output ONLY the finished recommendation. Nothing else.
- Do NOT show your reasoning, planning, or working-out.
- Do NOT begin with phrases like 'We need to', 'Let me', 'I need to',
  'Let's', 'First, I'll', 'Sentence 1:', 'Step 1:', or 'Thinking:'.
- Begin DIRECTLY with the heading 'RISK SUMMARY'.
- Do NOT label your own output ('Here is the recommendation:'). Just write it.

ROLE FOCUS:
{role_focus}

DATA (the only facts you may cite):
{ml_context}

REQUIRED STRUCTURE (use these exact headings):

RISK SUMMARY
[Two sentences. Identify the single biggest risk and quantify it using
an actual number from the DATA section above.]

PRIORITY ACTIONS
1. [Verb] [specific action] -- because [metric name] shows [value from data].
2. [Verb] [specific action] -- because [metric name] shows [value from data].
3. [Verb] [specific action] -- because [metric name] shows [value from data].

POSITIVE SIGNAL
[One sentence. Cite one strong metric from the DATA section and explain
what it means for the business.]

STYLE RULES:
- Plain business language. No jargon. No technical terms unless the role
  is technical.
- Anchor every claim to a specific number from the DATA section. If a
  number is not present, do not state it.
- Do NOT invent target percentages or deadlines. "Reduce churn" is fine;
  "reduce churn by 10% in 90 days" is forbidden unless 10% appears in
  the DATA.
- Maximum 250 words total.
"""


# ═══════════════════════════════════════════════════════════════════════════
#   PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════

def generate_recommendation(role: str, kpis: dict) -> dict:
    """
    Generate a role-targeted recommendation from the latest ML KPIs.

    Args:
        role  : audience, e.g. "Chief Executive Officer", "Sales Manager"
        kpis  : either the full ml-bridge dict (preferred) or v1's flat dict

    Returns:
        {
          "status":         "success" | "failed",
          "recommendation": str,            # only on success
          "role":           str,            # echoed back
          "model":          str,            # which model produced the answer
          "error":          str,            # only on failed
          "hint":           str,            # operator-facing remediation
        }
    """
    try:
        ml_context = _build_ml_context(kpis)
        if not ml_context.strip():
            return {
                "status": "failed",
                "error":  "no ML KPIs available",
                "hint":   "Run the trainer first so evaluation_results.json exists.",
            }

        prompt = _build_prompt(role, ml_context)
        answer, model, err = _call_llm(prompt)

        if not answer:
            return {
                "status": "failed",
                "error":  err or "LLM returned no usable answer",
                "hint":   ("Check GROQ_API_KEY (and OPENROUTER_API_KEY if used) "
                           "in .env. Verify internet connectivity. If reasoning-"
                           "leak detection rejected every response, the model "
                           "list in knowledge_base.GROQ_MODELS may need tuning."),
            }

        return {
            "status":         "success",
            "recommendation": answer,
            "role":           role,
            "model":          model,
        }
    except Exception as e:
        return {
            "status": "failed",
            "error":  str(e),
            "hint":   "Check GROQ_API_KEY in .env and internet connection.",
        }