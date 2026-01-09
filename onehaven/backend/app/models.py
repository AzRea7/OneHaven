# app/models.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, synonym


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# -----------------------------
# Core enums
# -----------------------------
class LeadStatus(str, PyEnum):
    new = "new"
    qualified = "qualified"
    contacted = "contacted"
    under_contract = "under_contract"
    closed = "closed"
    dead = "dead"


class LeadSource(str, PyEnum):
    rentcast_listing = "rentcast_listing"
    wayne_auction = "wayne_auction"
    manual = "manual"
    mls_reso = "mls_reso"
    mls_grid = "mls_grid"


class Strategy(str, PyEnum):
    rental = "rental"
    flip = "flip"


class OutcomeType(str, PyEnum):
    contacted = "contacted"
    responded = "responded"
    appointment_set = "appointment_set"
    under_contract = "under_contract"
    closed = "closed"
    dead = "dead"
    mls_pending = "mls_pending"
    mls_closed = "mls_closed"


class OutboxStatus(str, PyEnum):
    pending = "pending"
    delivered = "delivered"
    failed = "failed"


class IntegrationType(str, PyEnum):
    webhook = "webhook"


class JobRunStatus(str, PyEnum):
    running = "running"
    success = "success"
    failed = "failed"


class EstimateKind(str, PyEnum):
    rent_long_term = "rent_long_term"
    value = "value"


# -----------------------------
# Models
# -----------------------------
class Property(Base):
    """
    Mapped to current SQLite schema. DB columns include:
      address_line, city, state, zipcode, lat, lon, property_type, beds, baths, sqft, ...
    """
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    source: Mapped[str] = mapped_column(String, default="unknown", nullable=False)
    source_listing_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    address_line1: Mapped[str] = mapped_column("address_line", String(255), nullable=False)

    city: Mapped[str] = mapped_column(String(80), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)

    zip_code: Mapped[str] = mapped_column("zipcode", String(10), nullable=False)

    latitude: Mapped[Optional[float]] = mapped_column("lat", Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column("lon", Float, nullable=True)

    property_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    bedrooms: Mapped[Optional[int]] = mapped_column("beds", Integer, nullable=True)
    bathrooms: Mapped[Optional[float]] = mapped_column("baths", Float, nullable=True)

    sqft: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    owner_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    owner_mailing: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    owner_mailing_city: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    owner_mailing_state: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_sale_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_sale_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    # Tests create tables from metadata, so this is fine there.
    # If prod DB lacks this column, you’ll need a migration for prod runs.
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    # -------------------------
    # Synonyms (critical for tests + older call sites)
    # -------------------------
    address_line = synonym("address_line1")
    zipcode = synonym("zip_code")

    # ✅ allow Property(lat=..., lon=...) in tests/fixtures
    lat = synonym("latitude")
    lon = synonym("longitude")
    beds = synonym("bedrooms")
    baths = synonym("bathrooms")


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    property_id: Mapped[int] = mapped_column(Integer, nullable=False)

    strategy: Mapped[Strategy] = mapped_column(Enum(Strategy), nullable=False, default=Strategy.rental)

    list_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rent_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    arv_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rehab_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    deal_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    motivation_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rank_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    status: Mapped[LeadStatus] = mapped_column(Enum(LeadStatus), nullable=False, default=LeadStatus.new)

    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    explain_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class OutcomeEvent(Base):
    __tablename__ = "outcome_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(Integer, index=True)

    outcome_type: Mapped[OutcomeType] = mapped_column(Enum(OutcomeType), index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)

    source: Mapped[str] = mapped_column(String(50), default="manual")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    contract_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_profit: Mapped[float | None] = mapped_column(Float, nullable=True)


class Outcome(Base):
    __tablename__ = "outcomes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_id: Mapped[int] = mapped_column(Integer, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class Integration(Base):
    __tablename__ = "integrations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    type: Mapped[str] = mapped_column(String, nullable=False, default=IntegrationType.webhook.value)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)

    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    config_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    event_type: Mapped[str] = mapped_column(String(120), index=True)
    payload_json: Mapped[str] = mapped_column(Text)

    status: Mapped[OutboxStatus] = mapped_column(Enum(OutboxStatus), default=OutboxStatus.pending, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)

    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class EstimateCache(Base):
    __tablename__ = "estimate_cache"
    __table_args__ = (UniqueConstraint("property_id", "kind", name="uq_estimate_cache_prop_kind"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    property_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    kind: Mapped[EstimateKind] = mapped_column(Enum(EstimateKind), nullable=False, index=True)

    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")

    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: utcnow() + timedelta(days=1))

    raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    estimated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_name: Mapped[str] = mapped_column(String(80), index=True)

    status: Mapped[JobRunStatus] = mapped_column(Enum(JobRunStatus), default=JobRunStatus.running, index=True)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    meta_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    summary_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
