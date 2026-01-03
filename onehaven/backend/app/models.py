# app/models.py
from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Column,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# -----------------------------
# Core enums
# -----------------------------
class LeadStatus(str, enum.Enum):
    new = "new"
    qualified = "qualified"
    contacted = "contacted"
    under_contract = "under_contract"
    closed = "closed"
    dead = "dead"


class LeadSource(str, enum.Enum):
    rentcast_listing = "rentcast_listing"
    wayne_auction = "wayne_auction"
    manual = "manual"
    mls_reso = "mls_reso"


class Strategy(str, enum.Enum):
    rental = "rental"
    flip = "flip"


class OutcomeType(str, enum.Enum):
    contacted = "contacted"
    responded = "responded"
    appointment_set = "appointment_set"
    under_contract = "under_contract"
    closed = "closed"
    dead = "dead"
    mls_pending = "mls_pending"
    mls_closed = "mls_closed"


class OutboxStatus(str, enum.Enum):
    pending = "pending"
    delivered = "delivered"
    failed = "failed"


class IntegrationType(str, enum.Enum):
    webhook = "webhook"


class JobRunStatus(str, enum.Enum):
    running = "running"
    success = "success"
    failed = "failed"

class EstimateKind(str, enum.Enum):
    rent_long_term = "rent_long_term"
    value = "value"

# -----------------------------
# Models
# -----------------------------

class Property(Base):
    __tablename__ = "properties"
    __table_args__ = (
        UniqueConstraint("address_line", "city", "state", "zipcode", name="uq_property_addr"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    address_line: Mapped[str] = mapped_column(String(255))
    city: Mapped[str] = mapped_column(String(80))
    state: Mapped[str] = mapped_column(String(2))
    zipcode: Mapped[str] = mapped_column(String(10))

    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon: Mapped[float | None] = mapped_column(Float, nullable=True)

    property_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    beds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    baths: Mapped[float | None] = mapped_column(Float, nullable=True)
    sqft: Mapped[int | None] = mapped_column(Integer, nullable=True)

    owner_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    owner_mailing: Mapped[str | None] = mapped_column(String(255), nullable=True)

    last_sale_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sale_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)



class Lead(Base):
    __tablename__ = "leads"
    __table_args__ = (
        UniqueConstraint("property_id", "strategy", "source", name="uq_lead_prop_strategy_source"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(Integer, index=True)

    strategy: Mapped[Strategy] = mapped_column(Enum(Strategy), index=True)
    source: Mapped[LeadSource] = mapped_column(Enum(LeadSource), index=True)

    source_ref: Mapped[str | None] = mapped_column(String(120), nullable=True)

    # Truthy inputs / enrichment outputs
    list_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    rent_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)
    arv_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)  # NEW
    rehab_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)  # optional

    deal_score: Mapped[float] = mapped_column(Float, default=0.0)
    motivation_score: Mapped[float] = mapped_column(Float, default=0.0)
    rank_score: Mapped[float] = mapped_column(Float, default=0.0)

    status: Mapped[LeadStatus] = mapped_column(Enum(LeadStatus), default=LeadStatus.new, index=True)

    score_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    explain_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class OutcomeEvent(Base):
    __tablename__ = "outcome_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(Integer, index=True)

    outcome_type: Mapped[OutcomeType] = mapped_column(Enum(OutcomeType), index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    source: Mapped[str] = mapped_column(String(50), default="manual")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    contract_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_profit: Mapped[float | None] = mapped_column(Float, nullable=True)


class Integration(Base):
    __tablename__ = "integrations"
    __table_args__ = (UniqueConstraint("name", name="uq_integration_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(80))
    type: Mapped[IntegrationType] = mapped_column(Enum(IntegrationType))

    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    config_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class OutboxEvent(Base):
    """
    Outbox pattern (reliable integration events).
    IMPORTANT: fields must match integrations/services/outbox.py
    """
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # what happened
    event_type: Mapped[str] = mapped_column(String(120), index=True)
    payload_json: Mapped[str] = mapped_column(Text)

    # delivery bookkeeping
    status: Mapped[OutboxStatus] = mapped_column(Enum(OutboxStatus), default=OutboxStatus.pending, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)

    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class EstimateCache(Base):
    """
    Hard cache for external enrichment calls.
    Keyed by (property_id, kind).
    """
    __tablename__ = "estimate_cache"
    __table_args__ = (UniqueConstraint("property_id", "kind", name="uq_estimate_cache_prop_kind"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    property_id: Mapped[int] = mapped_column(Integer, index=True)
    kind: Mapped[EstimateKind] = mapped_column(Enum(EstimateKind), index=True)

    # the numeric estimate (rent, value, etc.)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(40), default="unknown")

    # bookkeeping timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    # when the estimate was produced (used for TTL freshness checks)
    estimated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)

    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)

class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_name: Mapped[str] = mapped_column(String(80), index=True)

    status: Mapped[JobRunStatus] = mapped_column(Enum(JobRunStatus), default=JobRunStatus.running, index=True)

    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_json: Mapped[str | None] = mapped_column(Text, nullable=True)
