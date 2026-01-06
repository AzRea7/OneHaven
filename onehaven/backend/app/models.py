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
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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
    IMPORTANT: This model is mapped to your *current* SQLite schema.

    Your DB table `properties` has columns:
      address_line, city, state, zipcode, lat, lon, property_type, beds, baths, sqft,
      owner_name, owner_mailing, last_sale_date, last_sale_price, created_at,
      source, source_listing_id

    So we keep the *Python attribute names* your code prefers (address_line1, zip_code, etc.)
    but map them onto the existing column names using mapped_column("<db_column_name>", ...).
    """
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # These two exist in DB (you confirmed via PRAGMA)
    source: Mapped[str] = mapped_column(String, default="unknown", nullable=False)
    source_listing_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # DB column is `address_line` (NOT NULL)
    address_line1: Mapped[str] = mapped_column("address_line", String(255), nullable=False)

    # DB has no address_line2 column
    city: Mapped[str] = mapped_column(String(80), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)

    # DB column is `zipcode` (NOT NULL)
    zip_code: Mapped[str] = mapped_column("zipcode", String(10), nullable=False)

    # DB columns are `lat` / `lon`
    latitude: Mapped[Optional[float]] = mapped_column("lat", Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column("lon", Float, nullable=True)

    property_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    # DB columns are `beds` / `baths`
    bedrooms: Mapped[Optional[int]] = mapped_column("beds", Integer, nullable=True)
    bathrooms: Mapped[Optional[float]] = mapped_column("baths", Float, nullable=True)

    sqft: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # extra columns in your DB (fine to keep)
    owner_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    owner_mailing: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    owner_mailing_city: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    owner_mailing_state: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_sale_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_sale_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # DB has created_at (NOT NULL). DB does NOT have updated_at.
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    property_id: Mapped[int] = mapped_column(Integer, nullable=False)

    strategy: Mapped[str] = mapped_column(String, nullable=False, default=Strategy.rental.value)

    list_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rent_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    arv_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    deal_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    motivation_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rank_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    status: Mapped[str] = mapped_column(String, nullable=False, default=LeadStatus.new.value)

    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    explain_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


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

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class Integration(Base):
    __tablename__ = "integrations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String, nullable=False, default=IntegrationType.webhook.value)
    name: Mapped[str] = mapped_column(String, nullable=False)

    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    secret: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

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
    """
    IMPORTANT: This model is mapped to your *current* SQLite schema.

    estimate_cache columns from PRAGMA:
      id, property_id, kind, value, source, fetched_at (NOT NULL), expires_at (NOT NULL),
      raw_json, created_at(TEXT default), updated_at(TEXT default), estimated_at
    """
    __tablename__ = "estimate_cache"
    __table_args__ = (UniqueConstraint("property_id", "kind", name="uq_estimate_cache_prop_kind"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    property_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    kind: Mapped[EstimateKind] = mapped_column(Enum(EstimateKind), nullable=False, index=True)

    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")

    # Required in DB
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: utcnow() + timedelta(days=1))

    raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # DB has these as TEXT defaults; we map them as strings to avoid sqlite/type weirdness
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

    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
