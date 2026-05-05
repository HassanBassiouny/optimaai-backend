"""
app/routes/kb_routes.py
Knowledge base API endpoints:
  POST /api/v1/kb/ingest/text   ← ingest raw text
  POST /api/v1/kb/ingest/file   ← upload any file
  POST /api/v1/kb/ingest/kpis   ← auto-ingest ML results
  POST /api/v1/kb/ask           ← RAG: ask any question
  GET  /api/v1/kb/stats         ← how many docs, sources
  DELETE /api/v1/kb/source      ← remove a source
"""
import os
import shutil
import tempfile
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.services.knowledge_base import (
    ingest_text, ingest_file, ingest_kpi_snapshot,
    rag_answer, kb_stats, delete_source, clear_knowledge_base,
)
from app.services.ml_bridge import load_latest_evaluation

router = APIRouter(prefix="/api/v1/kb", tags=["Knowledge Base"])


# ── Schemas ────────────────────────────────────────────────────────────────

class IngestTextRequest(BaseModel):
    text:      str
    source:    str = "manual_entry"
    category:  str = "general"
    class Config:
        json_schema_extra = {"example": {
            "text": "Q3 revenue dropped 12% due to supply chain disruptions in Asia region.",
            "source": "quarterly_review_2024",
            "category": "finance",
        }}

class AskRequest(BaseModel):
    question:  str
    category:  Optional[str] = None
    role:      str = "business analyst"
    class Config:
        json_schema_extra = {"example": {
            "question": "What caused the revenue drop in Q3?",
            "category": "finance",
            "role": "Finance Controller",
        }}

class DeleteSourceRequest(BaseModel):
    source: str


# ── Routes ─────────────────────────────────────────────────────────────────

@router.post("/ingest/text")
def ingest_text_route(req: IngestTextRequest):
    """Ingest a plain text string into the knowledge base."""
    result = ingest_text(req.text, req.source, req.category)
    if result.get("status") != "ok":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.post("/ingest/file")
async def ingest_file_route(
    file:     UploadFile = File(...),
    category: str = "general",
):
    """
    Upload any file (CSV, Excel, PDF, TXT) to the knowledge base.
    Supports: .csv  .xlsx  .xls  .pdf  .txt  .md  .json
    """
    suffix = os.path.splitext(file.filename)[1].lower()
    allowed = {".csv", ".xlsx", ".xls", ".pdf", ".txt", ".md", ".json"}
    if suffix not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {suffix}. Allowed: {allowed}"
        )

    # Save to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    try:
        result = ingest_file(tmp_path, category=category)
        result["filename"] = file.filename
        return result
    finally:
        os.unlink(tmp_path)


@router.post("/ingest/kpis")
def ingest_kpis_route():
    """
    Auto-ingest the latest ML evaluation results into the knowledge base.
    Call this after every training run so the LLM knows your latest metrics.
    """
    try:
        kpis   = load_latest_evaluation()
        result = ingest_kpi_snapshot(kpis)
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/ask")
def ask_route(req: AskRequest):
    """
    Ask any business question — the RAG pipeline retrieves relevant
    context from the knowledge base and answers using the LLM.

    Returns a successful response (HTTP 200) for any of these statuses:
        - "success"            -- answered using uploaded data
        - "general_knowledge"  -- answered using general knowledge
                                  (data didn't cover the question, OR
                                  it was a casual/conversational message)
        - "no_context"         -- KB is empty or had nothing relevant

    Only LLM provider failures or unexpected statuses raise HTTP errors.
    """
    result = rag_answer(
        question  = req.question,
        category  = req.category,
        role      = req.role,
        n_results = 5,
    )

    # All these statuses are valid responses, not errors. The frontend
    # uses the status field to style the bubble appropriately.
    valid_statuses = {"success", "general_knowledge", "no_context"}
    status = result.get("status")
    if status in valid_statuses:
        return result

    # Anything else (e.g. "llm_error") is a real failure -- propagate
    # as a 502 with the error message in the detail.
    raise HTTPException(
        status_code=502,
        detail=result.get("answer") or f"Unexpected status: {status}",
    )


@router.get("/stats")
def stats_route():
    """Return knowledge base statistics — total chunks, sources, categories."""
    return kb_stats()


@router.delete("/source")
def delete_source_route(req: DeleteSourceRequest):
    """Remove all chunks from a specific source."""
    return delete_source(req.source)


@router.delete("/clear")
def clear_route():
    """Clear the entire knowledge base. Irreversible."""
    return clear_knowledge_base()