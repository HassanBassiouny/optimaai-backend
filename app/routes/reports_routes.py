"""
app/routes/reports_routes.py
═══════════════════════════════════════════════════════════════════════════
REPORTS API
═══════════════════════════════════════════════════════════════════════════

Endpoints:
  GET    /api/v1/reports/templates                   list available templates
  POST   /api/v1/reports/generate                    generate a new report
  GET    /api/v1/reports/{report_id}/file/{fmt}      download docx/pdf/md
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional

from app.services.report_service import (
    list_templates,
    generate_report,
    get_report_file,
)

router = APIRouter(prefix="/api/v1/reports", tags=["Reports"])


# ── Schemas ───────────────────────────────────────────────────────────────

class GenerateReportRequest(BaseModel):
    template_id: str
    role:        Optional[str] = "executive"

    class Config:
        json_schema_extra = {"example": {
            "template_id": "executive_summary",
            "role":        "Chief Executive Officer",
        }}


# ── Routes ────────────────────────────────────────────────────────────────

@router.get("/templates")
def templates_route():
    """Return all available report templates with metadata."""
    return {"templates": list_templates()}


@router.post("/generate")
def generate_route(req: GenerateReportRequest):
    """
    Generate a multi-section report from one of the registered templates.

    The report is rendered as markdown (for the UI) and persisted as both
    .docx and .pdf for download.
    """
    try:
        result = generate_report(req.template_id, role=req.role or "executive")
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {e}")


@router.get("/{report_id}/file/{fmt}")
def download_report(report_id: str, fmt: str):
    """
    Download a generated report file.

    fmt may be 'docx', 'pdf' or 'md'.
    """
    try:
        path = get_report_file(report_id, fmt)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    media_types = {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf":  "application/pdf",
        "md":   "text/markdown",
    }
    return FileResponse(
        path,
        media_type = media_types[fmt],
        filename   = f"OptimaAi_Report_{report_id}.{fmt}",
    )
