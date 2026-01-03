# app/main.py
from __future__ import annotations
import json
from fastapi import FastAPI, Depends, Query, HTTPException, Header
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from collections import defaultdict
import statistics
from typing import Any


from .config import settings
from .db import get_session, engine
from .models import (
    Base,
    Lead, Property,
    Integration, IntegrationType,
    OutcomeType, LeadStatus, LeadSource,
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
from .connectors.wayne_auction import WayneAuctionConnector
from .services.jobruns import start_job, finish_job_success, finish_job_fail

app = FastAPI(title="OneHaven - Lead Truth Engine")


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if settings.API_KEY:
        if not x_api_key or x_api_key != settings.API_KEY:
            raise HTTPException(status_code=401, detail="Invalid API key")


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.post("/jobs/refresh", dependencies=[Depends(require_api_key)])
async def jobs_refresh(
    region: str | None = Query(None, description="Named region, e.g. se_michigan"),
    zips: str | None = Query(None, description="Comma-separated zip list, e.g. 48362,48363"),
    city: str | None = Query(None, description="City name, e.g. 'lake orion' or 'clarkston'"),
    max_price: float | None = Query(None, description="Optional listing price ceiling"),
    per_zip_limit: int = Query(200, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """
    Refresh priority:
      1) zips
      2) city
      3) region
    """
    zip_list: list[str] = []
    if zips:
        zip_list = [z.strip() for z in zips.split(",") if z.strip()]

    result = await refresh_region(
        session,
        region=region,
        zips=zip_list or None,
        city=city,
        max_price=max_price,
        per_zip_limit=per_zip_limit,
    )
    await session.commit()
    return result




@app.post("/jobs/dispatch", response_model=DispatchResult, dependencies=[Depends(require_api_key)])
async def dispatch_outbox(
    batch_size: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> DispatchResult:
    jr = await start_job(session, "dispatch_api")
    try:
        result = await run_dispatch(session=session, batch_size=batch_size)
        await finish_job_success(session, jr, result)
        await session.commit()
        return DispatchResult(**result)
    except Exception as e:
        await finish_job_fail(session, jr, e)
        await session.commit()
        raise


@app.post("/integrations", response_model=IntegrationOut, dependencies=[Depends(require_api_key)])
async def create_integration(
    body: IntegrationCreate,
    session: AsyncSession = Depends(get_session),
) -> IntegrationOut:
    if body.type != "webhook":
        raise HTTPException(status_code=400, detail="Only webhook integrations supported in v0")

    # Soft guard: unique name (prevents sink spam)
    existing = (await session.execute(select(Integration).where(Integration.name == body.name))).scalars().first()
    if existing:
        raise HTTPException(status_code=409, detail="Integration name already exists. Use PATCH to update/disable.")

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


@app.patch("/integrations/{integration_id}", response_model=IntegrationOut, dependencies=[Depends(require_api_key)])
async def update_integration(
    integration_id: int,
    enabled: bool | None = None,
    url: str | None = None,
    secret: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> IntegrationOut:
    integ = (await session.execute(select(Integration).where(Integration.id == integration_id))).scalars().first()
    if not integ:
        raise HTTPException(status_code=404, detail="Integration not found")

    if enabled is not None:
        integ.enabled = bool(enabled)

    if integ.type == IntegrationType.webhook and (url is not None or secret is not None):
        cfg = json.loads(integ.config_json or "{}")
        if url is not None:
            cfg["url"] = url
        if secret is not None:
            cfg["secret"] = secret
        integ.config_json = json.dumps(cfg)

    await session.commit()

    return IntegrationOut(
        id=integ.id,
        name=integ.name,
        type=integ.type.value,
        enabled=integ.enabled,
        created_at=integ.created_at,
    )


@app.get("/integrations", response_model=list[IntegrationOut], dependencies=[Depends(require_api_key)])
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
    max_price: float | None = Query(default=None, ge=0),
    source: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> list[LeadOut]:
    stmt = (
        select(Lead, Property)
        .join(Property, Property.id == Lead.property_id)
        .where(Property.zipcode == zip)
        .where(Lead.strategy == strategy)
    )

    if max_price is not None:
        stmt = stmt.where(Lead.list_price.isnot(None)).where(Lead.list_price <= max_price)

    if source is not None:
        # Accept "rentcast_listing" or "wayne_auction"
        stmt = stmt.where(Lead.source == LeadSource(source))

    stmt = stmt.order_by(desc(Lead.rank_score)).limit(limit)

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
                explain=lead.explain_json or "",
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


@app.post("/leads/{lead_id}/status", response_model=OutcomeOut, dependencies=[Depends(require_api_key)])
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


@app.post("/events/outcome", response_model=OutcomeOut, dependencies=[Depends(require_api_key)])
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


@app.get("/metrics/conversion", response_model=list[ScoreBucketMetrics], dependencies=[Depends(require_api_key)])
async def metrics_conversion(
    zip: str = Query(...),
    strategy: str = Query("rental"),
    session: AsyncSession = Depends(get_session),
) -> list[ScoreBucketMetrics]:
    rows = await conversion_by_bucket(session, zip=zip, strategy=strategy)
    return [ScoreBucketMetrics(**r) for r in rows]


@app.get("/metrics/time-to-contact", response_model=list[TimeToContactMetrics], dependencies=[Depends(require_api_key)])
async def metrics_time_to_contact(
    zip: str = Query(...),
    strategy: str = Query("rental"),
    session: AsyncSession = Depends(get_session),
) -> list[TimeToContactMetrics]:
    rows = await time_to_contact_by_bucket(session, zip=zip, strategy=strategy)
    return [TimeToContactMetrics(**r) for r in rows]


@app.get("/metrics/roi", response_model=RoiMetrics, dependencies=[Depends(require_api_key)])
async def metrics_roi(
    zip: str = Query(...),
    strategy: str = Query("rental"),
    session: AsyncSession = Depends(get_session),
) -> RoiMetrics:
    r = await roi_vs_realized(session, zip=zip, strategy=strategy)
    return RoiMetrics(**r)


@app.get("/health", dependencies=[Depends(require_api_key)])
async def health(session: AsyncSession = Depends(get_session)):
    from .models import JobRun
    rows = (await session.execute(select(JobRun).order_by(desc(JobRun.started_at)).limit(10))).scalars().all()
    return {
        "status": "ok",
        "recent_jobs": [
            {
                "job_name": r.job_name,
                "started_at": r.started_at,
                "finished_at": r.finished_at,
                "status": r.status,
                "error": r.error,
            }
            for r in rows
        ],
    }


@app.get("/connectors/wayne/health", dependencies=[Depends(require_api_key)])
async def wayne_health():
    c = WayneAuctionConnector()
    return {
        "fetched": c.health.fetched,
        "parsed_batches": c.health.parsed_batches,
        "parsed_properties": c.health.parsed_properties,
        "leads_emitted": c.health.leads_emitted,
        "errors": c.health.errors,
        "last_error": c.health.last_error,
        "snapshots_dir": "data/wayne_snapshots/",
    }


@app.get("/connectors/wayne/test", dependencies=[Depends(require_api_key)])
async def wayne_test(zip: str = Query(...), limit: int = Query(50, ge=1, le=200)):
    c = WayneAuctionConnector()
    leads = await c.fetch_by_zip(zipcode=zip, limit=limit)
    return {
        "zip": zip,
        "returned_leads": len(leads),
        "health": {
            "fetched": c.health.fetched,
            "parsed_batches": c.health.parsed_batches,
            "parsed_properties": c.health.parsed_properties,
            "leads_emitted": c.health.leads_emitted,
            "errors": c.health.errors,
            "last_error": c.health.last_error,
        },
        "sample": [l.payload for l in leads[:3]],
        "snapshots_dir": "data/wayne_snapshots/",
    }

@app.get("/debug/leads/stats", dependencies=[Depends(require_api_key)])
async def debug_leads_stats(
    zips: str | None = Query(default=None, description="Comma-separated zips filter"),
    session: AsyncSession = Depends(get_session),
):
    zip_filter = None
    if zips:
        zip_filter = {z.strip() for z in zips.split(",") if z.strip()}

    # counts by zip + min/max
    stmt = (
        select(
            Property.zipcode,
            func.count(Lead.id),
            func.min(Lead.list_price),
            func.max(Lead.list_price),
        )
        .select_from(Lead)
        .join(Property, Property.id == Lead.property_id)
        .group_by(Property.zipcode)
    )
    rows = (await session.execute(stmt)).all()

    # list prices for median (only where price present)
    stmt_prices = (
        select(Property.zipcode, Lead.list_price)
        .select_from(Lead)
        .join(Property, Property.id == Lead.property_id)
        .where(Lead.list_price.isnot(None))
    )
    if zip_filter:
        stmt_prices = stmt_prices.where(Property.zipcode.in_(zip_filter))

    price_rows = (await session.execute(stmt_prices)).all()
    prices_by_zip: dict[str, list[float]] = defaultdict(list)
    for z, p in price_rows:
        if p is not None:
            prices_by_zip[z].append(float(p))

    # counts by source
    stmt_src = (
        select(Property.zipcode, Lead.source, func.count(Lead.id))
        .select_from(Lead)
        .join(Property, Property.id == Lead.property_id)
        .group_by(Property.zipcode, Lead.source)
    )
    if zip_filter:
        stmt_src = stmt_src.where(Property.zipcode.in_(zip_filter))
    src_rows = (await session.execute(stmt_src)).all()
    src_counts: dict[str, dict[str, int]] = defaultdict(dict)
    for z, src, c in src_rows:
        src_counts[z][src.value if hasattr(src, "value") else str(src)] = int(c)

    out = []
    for zipcode, cnt, minp, maxp in rows:
        if zip_filter and zipcode not in zip_filter:
            continue
        arr = prices_by_zip.get(zipcode, [])
        med = float(statistics.median(arr)) if arr else None
        out.append(
            {
                "zip": zipcode,
                "count": int(cnt),
                "min_list_price": float(minp) if minp is not None else None,
                "median_list_price": med,
                "max_list_price": float(maxp) if maxp is not None else None,
                "counts_by_source": src_counts.get(zipcode, {}),
            }
        )
    out.sort(key=lambda x: x["zip"])
    return out


@app.get("/debug/leads/quality", dependencies=[Depends(require_api_key)])
async def debug_leads_quality(
    zips: str | None = Query(default=None, description="Comma-separated zips filter"),
    strategy: str | None = Query(default=None, description="Optional: rental|flip"),
    session: AsyncSession = Depends(get_session),
):
    """
    Data quality gates dashboard.
    Shows missing-field rates and basic sanity failure rates by zip.

    This is what stops “garbage leads” from quietly dominating your queue.
    """
    zip_filter = None
    if zips:
        zip_filter = {z.strip() for z in zips.split(",") if z.strip()}

    stmt = (
        select(Lead, Property)
        .join(Property, Property.id == Lead.property_id)
    )
    if zip_filter:
        stmt = stmt.where(Property.zipcode.in_(zip_filter))
    if strategy:
        stmt = stmt.where(Lead.strategy == strategy)

    rows = (await session.execute(stmt)).all()
    if not rows:
        return []

    # aggregate per zip
    agg: dict[str, dict] = {}
    for lead, prop in rows:
        z = prop.zipcode
        a = agg.setdefault(
            z,
            {
                "zip": z,
                "count": 0,
                "missing_list_price": 0,
                "missing_rent_estimate": 0,
                "missing_beds": 0,
                "missing_baths": 0,
                "missing_sqft": 0,
                "missing_latlon": 0,
                "rent_sanity_bad": 0,
                "counts_by_source": {},
                "counts_by_property_type": {},
            },
        )

        a["count"] += 1

        if lead.list_price is None:
            a["missing_list_price"] += 1
        if lead.rent_estimate is None:
            a["missing_rent_estimate"] += 1
        if prop.beds is None:
            a["missing_beds"] += 1
        if prop.baths is None:
            a["missing_baths"] += 1
        if prop.sqft is None:
            a["missing_sqft"] += 1
        if prop.lat is None or prop.lon is None:
            a["missing_latlon"] += 1

        # sanity “bad” = gross_yield < 4% when we can compute
        try:
            if lead.list_price and lead.rent_estimate and lead.list_price > 0 and lead.rent_estimate > 0:
                gross_yield = (lead.rent_estimate * 12.0) / lead.list_price
                if gross_yield < 0.04:
                    a["rent_sanity_bad"] += 1
        except Exception:
            pass

        src = lead.source.value if hasattr(lead.source, "value") else str(lead.source)
        a["counts_by_source"][src] = int(a["counts_by_source"].get(src, 0)) + 1

        pt = prop.property_type or "unknown"
        a["counts_by_property_type"][pt] = int(a["counts_by_property_type"].get(pt, 0)) + 1

    # finalize rates
    out = []
    for z, a in sorted(agg.items()):
        n = max(int(a["count"]), 1)
        out.append(
            {
                "zip": z,
                "count": int(a["count"]),
                "missing_rates": {
                    "list_price": a["missing_list_price"] / n,
                    "rent_estimate": a["missing_rent_estimate"] / n,
                    "beds": a["missing_beds"] / n,
                    "baths": a["missing_baths"] / n,
                    "sqft": a["missing_sqft"] / n,
                    "latlon": a["missing_latlon"] / n,
                },
                "rent_sanity_bad_rate": a["rent_sanity_bad"] / n,
                "counts_by_source": a["counts_by_source"],
                "counts_by_property_type": a["counts_by_property_type"],
            }
        )

    return out
