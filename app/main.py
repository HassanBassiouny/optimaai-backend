"""
app/main.py — OptimaAi FastAPI entry point.
"""
import os
import warnings
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

warnings.filterwarnings("ignore", message=".*trapped.*bcrypt.*")

from app.database import init_db
from app.routes.auth_routes       import router as auth_router
from app.routes.api               import router as api_router
from app.routes.kb_routes         import router as kb_router
from app.routes.datasets_routes   import router as datasets_router
from app.routes.dashboard_routes  import router as dashboard_router
from app.routes.misc_routes       import router as misc_router
from app.routes.mapping_routes    import router as mapping_router
from app.routes.reports_routes    import router as reports_router
from app.routes.calibrate_routes  import router as calibrate_router
from app.routes.bmc_routes        import router as bmc_router
from app.routes.odoo_routes       import router as odoo_router

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("  [startup] Initialising DB…")
    init_db()
    print("  [startup] Ready.")
    yield


app = FastAPI(
    title="OptimaAi API",
    description="ML-powered business analytics backend (auth + ML + RAG + BMC + mapping + Odoo).",
    version="1.2.0",
    lifespan=lifespan,
)

FRONTEND_ORIGINS = os.getenv(
    "FRONTEND_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins     = [o.strip() for o in FRONTEND_ORIGINS if o.strip()],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

app.include_router(auth_router)
app.include_router(api_router)
app.include_router(kb_router)
app.include_router(datasets_router)
app.include_router(dashboard_router)
app.include_router(misc_router)
app.include_router(mapping_router)
app.include_router(reports_router)
app.include_router(calibrate_router)
app.include_router(bmc_router)
app.include_router(odoo_router)

@app.get("/")
def root():
    return {"service": "OptimaAi API", "status": "ok", "docs": "/docs"}