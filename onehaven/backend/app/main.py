import json
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from .db import get_session, engine
from .models import (
    Base,
    Lead, Property,
    Integration, IntegrationType,
    OutcomeType, LeadStatus
)
from .schemas import (
    LeadOut, JobResult,
    IntegrationCreate, IntegrationOut,
    DispatchResult,
    OutcomeCreate, OutcomeOut, LeadStatusUpdate,
    ScoreBucketMetrics, TimeToContactMetrics, RoiMetrics,
)
from .jobs.refresh import refresh_region
from .integrations.jobs.dispatch import run_dispatch
from .services.outcomes import add_outcome_event, update_lead_status
from .services.metrics import conversion_by_bucket, time_to_contact_by_bucket, roi_vs_realized

app = FastAPI(title="OneHaven - Lead Truth Engine")


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.post("/jobs/refresh", response_model=JobResult)
async def run_refresh(
    region: str = Query("se_michigan"),
    session: AsyncSession = Depends(get_session),
) -> JobResult:
    result = await refresh_region(session, region)
    await session.commit()
    return JobResult(**result)


@app.post("/jobs/dispatch", response_model=DispatchResult)
async def dispatch_outbox(
    batch_size: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> DispatchResult:
    result = await run_dispatch(session=session, batch_size=batch_size)
    await session.commit()
    return DispatchResult(**result)


@app.post("/integrations", response_model=IntegrationOut)
async def create_integration(
    body: IntegrationCreate,
    session: AsyncSession = Depends(get_session),
) -> IntegrationOut:
    if body.type != "webhook":
        raise HTTPException(status_code=400, detail="Only webhook integrations supported in v0")

    cfg = {"url": body.url, "secret": body.secret}
    integ = Integration(
        name=body.name,
        type=IntegrationType.webhook,
        enabled=body.enabled,
        config_json=json.dumps(cfg),
    )
    session.add(integ)
    await session.commit()

    return IntegrationOut(
        id=integ.id,
        name=integ.name,
        type=integ.type.value,
        enabled=integ.enabled,
        created_at=integ.created_at,
    )


@app.get("/integrations", response_model=list[IntegrationOut])
async def list_integrations(session: AsyncSession = Depends(get_session)) -> list[IntegrationOut]:
    rows = (await session.execute(select(Integration).order_by(Integration.id.asc()))).scalars().all()
    return [
        IntegrationOut(
            id=i.id,
            name=i.name,
            type=i.type.value,
            enabled=i.enabled,
            created_at=i.created_at,
        )
        for i in rows
    ]


@app.get("/leads/top", response_model=list[LeadOut])
async def top_leads(
    zip: str = Query(..., min_length=5, max_length=10),
    strategy: str = Query("rental"),
    limit: int = Query(25, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[LeadOut]:
    stmt = (
        select(Lead, Property)
        .join(Property, Property.id == Lead.property_id)
        .where(Property.zipcode == zip)
        .where(Lead.strategy == strategy)
        .order_by(desc(Lead.rank_score))
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()

    out: list[LeadOut] = []
    for lead, prop in rows:
        out.append(
            LeadOut(
                id=lead.id,
                property_id=lead.property_id,
                source=lead.source.value,
                status=lead.status.value,
                strategy=lead.strategy.value,
                rank_score=lead.rank_score,
                deal_score=lead.deal_score,
                motivation_score=lead.motivation_score,
                explain=lead.explain,
                address_line=prop.address_line,
                city=prop.city,
                state=prop.state,
                zipcode=prop.zipcode,
                list_price=lead.list_price,
                arv_estimate=lead.arv_estimate,
                rent_estimate=lead.rent_estimate,
                rehab_estimate=lead.rehab_estimate,
                created_at=lead.created_at,
            )
        )
    return out


# ----- Outcomes (feedback loop) -----

@app.post("/leads/{lead_id}/status", response_model=OutcomeOut)
async def set_lead_status(
    lead_id: int,
    body: LeadStatusUpdate,
    session: AsyncSession = Depends(get_session),
) -> OutcomeOut:
    ev = await update_lead_status(
        session=session,
        lead_id=lead_id,
        status=LeadStatus(body.status),
        occurred_at=body.occurred_at,
        notes=body.notes,
        source="manual",
    )
    await session.commit()
    return OutcomeOut(
        id=ev.id,
        lead_id=ev.lead_id,
        outcome_type=ev.outcome_type.value,
        occurred_at=ev.occurred_at,
        source=ev.source,
        notes=ev.notes,
        contract_price=ev.contract_price,
        realized_profit=ev.realized_profit,
    )


@app.post("/events/outcome", response_model=OutcomeOut)
async def create_outcome(
    body: OutcomeCreate,
    session: AsyncSession = Depends(get_session),
) -> OutcomeOut:
    ev = await add_outcome_event(
        session=session,
        lead_id=body.lead_id,
        outcome_type=OutcomeType(body.outcome_type),
        occurred_at=body.occurred_at,
        notes=body.notes,
        contract_price=body.contract_price,
        realized_profit=body.realized_profit,
        source=body.source,
    )
    await session.commit()
    return OutcomeOut(
        id=ev.id,
        lead_id=ev.lead_id,
        outcome_type=ev.outcome_type.value,
        occurred_at=ev.occurred_at,
        source=ev.source,
        notes=ev.notes,
        contract_price=ev.contract_price,
        realized_profit=ev.realized_profit,
    )


# ----- Evaluation (sellability) -----

@app.get("/metrics/conversion", response_model=list[ScoreBucketMetrics])
async def metrics_conversion(
    zip: str = Query(...),
    strategy: str = Query("rental"),
    session: AsyncSession = Depends(get_session),
) -> list[ScoreBucketMetrics]:
    rows = await conversion_by_bucket(session, zip=zip, strategy=strategy)
    return [ScoreBucketMetrics(**r) for r in rows]


@app.get("/metrics/time-to-contact", response_model=list[TimeToContactMetrics])
async def metrics_time_to_contact(
    zip: str = Query(...),
    strategy: str = Query("rental"),
    session: AsyncSession = Depends(get_session),
) -> list[TimeToContactMetrics]:
    rows = await time_to_contact_by_bucket(session, zip=zip, strategy=strategy)
    return [TimeToContactMetrics(**r) for r in rows]


@app.get("/metrics/roi", response_model=RoiMetrics)
async def metrics_roi(
    zip: str = Query(...),
    strategy: str = Query("rental"),
    session: AsyncSession = Depends(get_session),
) -> RoiMetrics:
    r = await roi_vs_realized(session, zip=zip, strategy=strategy)
    return RoiMetrics(**r)
