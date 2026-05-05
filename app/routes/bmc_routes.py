"""
app/routes/bmc_routes.py
═══════════════════════════════════════════════════════════════════════════
BMC API
═══════════════════════════════════════════════════════════════════════════

Endpoints:
  POST  /api/v1/bmc/generate                generate a new BMC
  GET   /api/v1/bmc/{bmc_id}/file/{fmt}     download docx or pdf
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional

from app.services.bmc_service import generate_bmc, get_bmc_file

router = APIRouter(prefix="/api/v1/bmc", tags=["Business Model Canvas"])


class GenerateBmcRequest(BaseModel):
    business_name: Optional[str] = "Your Business"

    class Config:
        json_schema_extra = {"example": {"business_name": "Egypt E-commerce"}}


@router.post("/generate")
def generate_route(req: GenerateBmcRequest):
    """
    Generate a 9-block Business Model Canvas inferred from the customer's
    uploaded data in the knowledge base.
    """
    try:
        return generate_bmc(business_name=req.business_name or "Your Business")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BMC generation failed: {e}")


@router.get("/{bmc_id}/file/{fmt}")
def download_route(bmc_id: str, fmt: str):
    """Download the BMC as docx or pdf."""
    try:
        path = get_bmc_file(bmc_id, fmt)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    media = {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf":  "application/pdf",
    }
    return FileResponse(
        path,
        media_type=media[fmt],
        filename=f"OptimaAi_BMC_{bmc_id}.{fmt}",
    )
