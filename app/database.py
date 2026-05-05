"""
app/database.py  —  UNIFIED OptimaAi schema (v2 — with column_mappings)
"""
import os
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Boolean,
    Text, DateTime, JSON, ForeignKey, Index, LargeBinary
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://optimaai:optimaai123@localhost:5432/optimaai_db"
)

engine       = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base         = declarative_base()


# ══════════════════════════════════════════════════════
#  Users
# ══════════════════════════════════════════════════════
class User(Base):
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    email           = Column(String(255), unique=True, nullable=False, index=True)
    name            = Column(String(255), nullable=False)
    hashed_password = Column(String(255), nullable=False)
    role            = Column(String(50), default="viewer")
    is_active       = Column(Boolean,  default=True)
    avatar_url      = Column(String(500), nullable=True)
    department_id   = Column(String(100), nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    uploads         = relationship("Upload",         back_populates="user", cascade="all, delete-orphan")
    kpi_snapshots   = relationship("KPISnapshot",    back_populates="user")
    recommendations = relationship("Recommendation", back_populates="user")
    bmcs            = relationship("BMCResult",      back_populates="user")
    rag_queries     = relationship("RagQuery",       back_populates="user", cascade="all, delete-orphan")


# ══════════════════════════════════════════════════════
#  Uploads
# ══════════════════════════════════════════════════════
class Upload(Base):
    __tablename__ = "uploads"

    upload_id          = Column(Integer, primary_key=True, index=True)
    user_id            = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    original_file_name = Column(String(500), nullable=False)
    table_name         = Column(String(200), nullable=True)
    category           = Column(String(100), default="general")
    rows_count         = Column(Integer, default=0)
    columns_count      = Column(Integer, default=0)
    quality_before     = Column(Float,  nullable=True)
    quality_after      = Column(Float,  nullable=True)
    kb_chunks          = Column(Integer, default=0)
    status             = Column(String(50), default="pending")
    uploaded_at        = Column(DateTime, default=datetime.utcnow, index=True)

    user     = relationship("User", back_populates="uploads")
    mappings = relationship("ColumnMapping", back_populates="upload", cascade="all, delete-orphan")

    customer_table_name = Column(String(200), nullable=True)
    monthly_table_name  = Column(String(200), nullable=True)

# ══════════════════════════════════════════════════════
#  Column mappings  — NEW
#  Links user-uploaded column names to model-expected features
# ══════════════════════════════════════════════════════
class ColumnMapping(Base):
    __tablename__ = "column_mappings"

    id            = Column(Integer, primary_key=True, index=True)
    upload_id     = Column(Integer, ForeignKey("uploads.upload_id", ondelete="CASCADE"), nullable=False, index=True)
    model_kind    = Column(String(30), nullable=False)   # revenue | churn | growth
    mapping       = Column(JSON, nullable=False)
    # Example mapping JSON:
    #   {"quantity_sold": "Qty", "price": "OrderAmount", "month": "_from_date:OrderDate"}
    created_at    = Column(DateTime, default=datetime.utcnow)
    updated_at    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active     = Column(Boolean, default=True)

    upload = relationship("Upload", back_populates="mappings")


# ══════════════════════════════════════════════════════
#  KPI snapshots
# ══════════════════════════════════════════════════════
class KPISnapshot(Base):
    __tablename__ = "kpi_snapshots"

    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    upload_id       = Column(Integer, ForeignKey("uploads.upload_id"), nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)
    model_version   = Column(String(50))

    revenue_mape    = Column(Float)
    revenue_r2      = Column(Float)
    revenue_mae     = Column(Float)
    forecast_bias   = Column(Float)

    churn_roc_auc   = Column(Float)
    churn_f1        = Column(Float)
    churn_rate_pct  = Column(Float)

    growth_mape     = Column(Float)
    growth_r2       = Column(Float)

    prophet_mape    = Column(Float)

    revenue_forecast_12 = Column(JSON)
    growth_forecast_12  = Column(JSON)

    user = relationship("User", back_populates="kpi_snapshots")


# ══════════════════════════════════════════════════════
#  Recommendations / BMC / RAG / Predictions
# ══════════════════════════════════════════════════════
class Recommendation(Base):
    __tablename__ = "recommendations"

    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    kpi_snapshot_id = Column(Integer, ForeignKey("kpi_snapshots.id"), nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)
    role            = Column(String(50))
    recommendation  = Column(Text)
    model_used      = Column(String(100))

    user = relationship("User", back_populates="recommendations")


class BMCResult(Base):
    __tablename__ = "bmc_results"

    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    kpi_snapshot_id = Column(Integer, ForeignKey("kpi_snapshots.id"), nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)
    platform_name   = Column(String(100))
    bmc_text        = Column(Text)
    bmc_blocks      = Column(JSON)
    model_used      = Column(String(100))

    user = relationship("User", back_populates="bmcs")


class RagQuery(Base):
    __tablename__ = "rag_queries"

    id          = Column(Integer, primary_key=True, index=True)
    user_id     = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    question    = Column(Text, nullable=False)
    answer      = Column(Text)
    role_used   = Column(String(50))
    category    = Column(String(100), nullable=True)
    sources     = Column(JSON)
    chunks_used = Column(Integer, default=0)
    status      = Column(String(30))
    model_used  = Column(String(100))
    created_at  = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="rag_queries")


class Prediction(Base):
    __tablename__ = "predictions"

    id         = Column(Integer, primary_key=True, index=True)
    user_id    = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    upload_id  = Column(Integer, ForeignKey("uploads.upload_id", ondelete="CASCADE"), nullable=True)
    kind       = Column(String(30))
    features   = Column(JSON)
    result     = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


Index("ix_uploads_user_uploaded", Upload.user_id, Upload.uploaded_at.desc())
Index("ix_rag_user_created",      RagQuery.user_id, RagQuery.created_at.desc())


# ══════════════════════════════════════════════════════
#  Per-user calibrators (Option C: hybrid generalization)
# ══════════════════════════════════════════════════════
class UserCalibrator(Base):
    __tablename__ = "user_calibrators"

    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),
                             nullable=False, index=True)
    upload_id       = Column(Integer, ForeignKey("uploads.upload_id", ondelete="SET NULL"),
                             nullable=True)
    model_kind      = Column(String(30), nullable=False)   # revenue | churn | growth
    calibrator_kind = Column(String(30), nullable=False)   # linear | isotonic | platt
    blob            = Column(LargeBinary, nullable=False)
    n_samples       = Column(Integer, default=0)
    metrics         = Column(JSON, nullable=True)
    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime, default=datetime.utcnow)


Index("ix_uploads_user_uploaded", Upload.user_id, Upload.uploaded_at.desc())
Index("ix_rag_user_created",      RagQuery.user_id, RagQuery.created_at.desc())
Index("ix_calibrator_user_kind_active",
      UserCalibrator.user_id, UserCalibrator.model_kind, UserCalibrator.is_active)


def init_db():
    Base.metadata.create_all(bind=engine)
    print("  [database] All tables created.")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()