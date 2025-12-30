from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal

Strategy = Literal["rental", "flip"]


class LeadOut(BaseModel):
    id: int
    property_id: int
    source: str
    status: str
    strategy: str

    rank_score: float
    deal_score: float
    motivation_score: float
    explain: str

    address_line: str
    city: str
    state: str
    zipcode: str

    list_price: float | None = None
    arv_estimate: float | None = None
    rent_estimate: float | None = None
    rehab_estimate: float | None = None

    created_at: datetime


class JobResult(BaseModel):
    created_leads: int = Field(..., ge=0)
    updated_leads: int = Field(..., ge=0)
    dropped: int = Field(..., ge=0)
    drop_reasons: dict[str, int]


class IntegrationCreate(BaseModel):
    name: str
    type: Literal["webhook"] = "webhook"
    enabled: bool = True
    url: str
    secret: str | None = None


class IntegrationOut(BaseModel):
    id: int
    name: str
    type: str
    enabled: bool
    created_at: datetime


class DispatchResult(BaseModel):
    delivered: int
    failed: int
    sinks: int | None = None
    events: int | None = None
    skipped_no_sinks: int | None = None

class LeadStatusUpdate(BaseModel):
    status: Literal["new", "qualified", "contacted", "under_contract", "closed", "dead"]
    occurred_at: datetime | None = None
    notes: str | None = None


class OutcomeCreate(BaseModel):
    lead_id: int
    outcome_type: Literal[
        "contacted",
        "responded",
        "appointment_set",
        "under_contract",
        "closed",
        "dead",
        "mls_pending",
        "mls_closed",
    ]
    occurred_at: datetime | None = None
    notes: str | None = None
    contract_price: float | None = None
    realized_profit: float | None = None
    source: str = "manual"


class OutcomeOut(BaseModel):
    id: int
    lead_id: int
    outcome_type: str
    occurred_at: datetime
    source: str
    notes: str | None = None
    contract_price: float | None = None
    realized_profit: float | None = None


class ScoreBucketMetrics(BaseModel):
    bucket: str
    count: int
    responded_rate: float
    appointment_rate: float
    contract_rate: float
    close_rate: float


class TimeToContactMetrics(BaseModel):
    bucket: str
    median_minutes_to_contact: float | None
    count_with_contact: int


class RoiMetrics(BaseModel):
    count_closed_with_profit: int
    avg_realized_profit: float | None
    median_realized_profit: float | None
