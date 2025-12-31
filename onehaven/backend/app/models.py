import enum
from datetime import datetime

from sqlalchemy import String, Column, Integer, Float, DateTime, Text, Enum, UniqueConstraint, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


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


class Strategy(str, enum.Enum):
    rental = "rental"
    flip = "flip"


class OutboxStatus(str, enum.Enum):
    pending = "pending"
    delivered = "delivered"
    failed = "failed"


class IntegrationType(str, enum.Enum):
    webhook = "webhook"
    # later: s3, email, gsheet, sftp, etc.


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

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(Integer, index=True)

    source: Mapped[LeadSource] = mapped_column(Enum(LeadSource))
    status: Mapped[LeadStatus] = mapped_column(Enum(LeadStatus), default=LeadStatus.new)
    strategy: Mapped[Strategy] = mapped_column(Enum(Strategy), default=Strategy.rental)

    source_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    provenance_json: Mapped[str] = mapped_column(Text, default="{}")

    motivation_score: Mapped[float] = mapped_column(Float, default=0.0)
    deal_score: Mapped[float] = mapped_column(Float, default=0.0)
    rank_score: Mapped[float] = mapped_column(Float, default=0.0)
    explain: Mapped[str] = mapped_column(Text, default="")

    list_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    arv_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)
    rent_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)
    rehab_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Integration(Base):
    """
    Stores integration destinations. Keep it generic.
    Example:
      type=webhook
      config_json={"url": "...", "secret": "...", "events":["lead.upserted"]}
    """
    __tablename__ = "integrations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    type: Mapped[IntegrationType] = mapped_column(Enum(IntegrationType))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    config_json: Mapped[str] = mapped_column(Text, default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class OutboxEvent(Base):
    """
    Outbox pattern: events are written transactionally with lead updates,
    then dispatched asynchronously. This gives reliable delivery + retry.
    """
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    event_type: Mapped[str] = mapped_column(String(80), index=True)  # e.g. lead.upserted
    payload_json: Mapped[str] = mapped_column(Text)

    status: Mapped[OutboxStatus] = mapped_column(Enum(OutboxStatus), default=OutboxStatus.pending)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class OutcomeType(str, enum.Enum):
    contacted = "contacted"
    responded = "responded"
    appointment_set = "appointment_set"
    under_contract = "under_contract"
    closed = "closed"
    dead = "dead"
    mls_pending = "mls_pending"
    mls_closed = "mls_closed"


class OutcomeEvent(Base):
    """
    Outcome events are the training signal source.
    Some are human pipeline events (contacted/responded),
    some are market events (mls_pending/mls_closed).
    """
    __tablename__ = "outcome_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_id: Mapped[int] = mapped_column(Integer, index=True)
    outcome_type: Mapped[OutcomeType] = mapped_column(Enum(OutcomeType), index=True)

    occurred_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Optional payload fields
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # For closed outcomes
    contract_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_profit: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Provenance: "manual", "mls_reso", "partner_csv", etc.
    source: Mapped[str] = mapped_column(String(60), default="manual")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class JobRun(Base):
    __tablename__ = "job_runs"
    id = Column(Integer, primary_key=True)
    job_name = Column(String, nullable=False)           # refresh / dispatch
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="running")  # running/success/fail
    summary_json = Column(Text, nullable=True)
    error = Column(Text, nullable=True)