from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.api.auth import CurrentTenant
from app.database import get_db
from app.models import Delivery, Event, OutboxMessage, Tenant, WebhookEndpoint
from app.schemas import EndpointCreate, EndpointRead, EndpointUpdate, EventAccepted, EventCreate

router = APIRouter(prefix="/v1")
DbSession = Annotated[Session, Depends(get_db)]


def endpoint_response(endpoint: WebhookEndpoint) -> EndpointRead:
    return EndpointRead(
        id=endpoint.id,
        name=endpoint.name,
        url=endpoint.url,
        event_types=endpoint.event_types,
        status=endpoint.status,
        created_at=endpoint.created_at,
        updated_at=endpoint.updated_at,
    )


@router.post("/endpoints", response_model=EndpointRead, status_code=status.HTTP_201_CREATED)
def create_endpoint(
    request: EndpointCreate,
    tenant: CurrentTenant,
    db: DbSession,
) -> EndpointRead:
    endpoint = WebhookEndpoint(
        tenant_id=tenant.id,
        name=request.name,
        url=str(request.url),
        event_types=request.event_types,
        status="active",
    )
    db.add(endpoint)
    db.commit()
    db.refresh(endpoint)
    return endpoint_response(endpoint)


@router.get("/endpoints", response_model=list[EndpointRead])
def list_endpoints(tenant: CurrentTenant, db: DbSession) -> list[EndpointRead]:
    endpoints = db.scalars(
        select(WebhookEndpoint)
        .where(WebhookEndpoint.tenant_id == tenant.id)
        .order_by(WebhookEndpoint.created_at.desc())
    ).all()
    return [endpoint_response(endpoint) for endpoint in endpoints]


@router.get("/endpoints/{endpoint_id}", response_model=EndpointRead)
def get_endpoint(endpoint_id: UUID, tenant: CurrentTenant, db: DbSession) -> EndpointRead:
    endpoint = db.scalar(
        select(WebhookEndpoint).where(
            WebhookEndpoint.id == endpoint_id,
            WebhookEndpoint.tenant_id == tenant.id,
        )
    )
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    return endpoint_response(endpoint)


@router.patch("/endpoints/{endpoint_id}", response_model=EndpointRead)
def update_endpoint(
    endpoint_id: UUID,
    request: EndpointUpdate,
    tenant: CurrentTenant,
    db: DbSession,
) -> EndpointRead:
    endpoint = db.scalar(
        select(WebhookEndpoint).where(
            WebhookEndpoint.id == endpoint_id,
            WebhookEndpoint.tenant_id == tenant.id,
        )
    )
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    changes = request.model_dump(exclude_unset=True)
    if any(value is None for value in changes.values()):
        raise HTTPException(status_code=422, detail="Fields cannot be null")
    if not changes:
        raise HTTPException(status_code=422, detail="Provide at least one field to update")
    if "status" in changes and changes["status"] not in {"active", "suspended", "disabled"}:
        raise HTTPException(status_code=422, detail="status must be active, suspended, or disabled")
    if "url" in changes and changes["url"] is not None:
        changes["url"] = str(changes["url"])
    for field, value in changes.items():
        setattr(endpoint, field, value)
    endpoint.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(endpoint)
    return endpoint_response(endpoint)


@router.delete("/endpoints/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
def disable_endpoint(endpoint_id: UUID, tenant: CurrentTenant, db: DbSession) -> Response:
    endpoint = db.scalar(
        select(WebhookEndpoint).where(
            WebhookEndpoint.id == endpoint_id,
            WebhookEndpoint.tenant_id == tenant.id,
        )
    )
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    endpoint.status = "disabled"
    endpoint.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    payload: EventCreate,
    request: Request,
    tenant: CurrentTenant,
    db: DbSession,
) -> EventAccepted:
    raw_body = await request.body()
    event_insert = (
        pg_insert(Event)
        .values(
            tenant_id=tenant.id,
            producer_event_id=payload.event_id,
            event_type=payload.event_type,
            payload=payload.payload,
            body_bytes=raw_body,
        )
        .on_conflict_do_nothing(constraint="uq_events_tenant_producer_event")
        .returning(Event.id)
    )

    # Event, delivery rows, and outbox rows commit or roll back as one unit.
    with db.begin():
        event_id = db.execute(event_insert).scalar_one_or_none()
        duplicate = event_id is None
        if duplicate:
            existing = db.scalar(
                select(Event).where(
                    Event.tenant_id == tenant.id,
                    Event.producer_event_id == payload.event_id,
                )
            )
            assert existing is not None
            if existing.event_type != payload.event_type or existing.payload != payload.payload:
                raise HTTPException(
                    status_code=409,
                    detail="event_id was already used with a different event body",
                )
            event_id = existing.id
            delivery_ids = list(
                db.scalars(select(Delivery.id).where(Delivery.event_id == event_id)).all()
            )
            return EventAccepted(
                event_id=event_id,
                producer_event_id=payload.event_id,
                duplicate=True,
                matched_endpoints=len(delivery_ids),
                delivery_ids=delivery_ids,
            )

        endpoints = db.scalars(
            select(WebhookEndpoint).where(
                WebhookEndpoint.tenant_id == tenant.id,
                WebhookEndpoint.status == "active",
                or_(
                    WebhookEndpoint.event_types.contains([payload.event_type]),
                    WebhookEndpoint.event_types.contains(["*"]),
                ),
            )
        ).all()

        delivery_ids: list[UUID] = []
        for endpoint in endpoints:
            delivery = Delivery(event_id=event_id, endpoint_id=endpoint.id, status="queued")
            db.add(delivery)
            db.flush()
            db.add(OutboxMessage(delivery_id=delivery.id, status="pending"))
            delivery_ids.append(delivery.id)

    return EventAccepted(
        event_id=event_id,
        producer_event_id=payload.event_id,
        duplicate=False,
        matched_endpoints=len(delivery_ids),
        delivery_ids=delivery_ids,
    )


@router.get("/events/{event_id}")
def get_event(event_id: UUID, tenant: CurrentTenant, db: DbSession) -> dict:
    event = db.scalar(
        select(Event).where(Event.id == event_id, Event.tenant_id == tenant.id)
    )
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return {
        "id": str(event.id),
        "producer_event_id": event.producer_event_id,
        "event_type": event.event_type,
        "payload": event.payload,
        "created_at": event.created_at,
    }

@router.post("/deliveries/{delivery_id}/replay", status_code=status.HTTP_202_ACCEPTED)
def replay_delivery(delivery_id: UUID, tenant: CurrentTenant, db: DbSession):
    """Start a fresh retry budget while preserving attempt history."""
    row = db.scalar(select(Delivery).join(Event, Delivery.event_id == Event.id).where(
        Delivery.id == delivery_id, Event.tenant_id == tenant.id
    ).with_for_update())
    if row is None:
        raise HTTPException(status_code=404, detail="Delivery not found")
    if row.status not in {"dead", "permanent_failure"}:
        raise HTTPException(status_code=409, detail="Only failed deliveries can be replayed")
    outbox = db.scalar(select(OutboxMessage).where(OutboxMessage.delivery_id == row.id).with_for_update())
    if outbox is None:
        raise HTTPException(status_code=500, detail="Delivery outbox record is missing")
    now = datetime.now(timezone.utc)
    row.status = "queued"
    row.attempt_count = 0
    row.next_attempt_at = now
    row.last_error = None
    row.last_status_code = None
    row.delivered_at = None
    outbox.status = "pending"
    outbox.available_at = now
    outbox.published_at = None
    outbox.last_error = None
    outbox.lease_owner = outbox.lease_token = outbox.lease_expires_at = None
    db.commit()
    return {"delivery_id": str(row.id), "status": row.status, "message": "Replay scheduled"}

from app.models import DeliveryAttempt

@router.get("/deliveries/{delivery_id}")
def get_delivery(delivery_id: UUID, tenant: CurrentTenant, db: DbSession):
    """Return tenant-scoped delivery state and its durable attempt history."""
    delivery = db.scalar(select(Delivery).join(Event, Delivery.event_id == Event.id).where(
        Delivery.id == delivery_id, Event.tenant_id == tenant.id
    ))
    if delivery is None:
        raise HTTPException(status_code=404, detail="Delivery not found")
    attempts = db.scalars(select(DeliveryAttempt).where(
        DeliveryAttempt.delivery_id == delivery.id
    ).order_by(DeliveryAttempt.attempt_number)).all()
    return {
        "delivery_id": str(delivery.id),
        "event_id": str(delivery.event_id),
        "endpoint_id": str(delivery.endpoint_id),
        "status": delivery.status,
        "attempt_count": delivery.attempt_count,
        "next_attempt_at": delivery.next_attempt_at,
        "last_status_code": delivery.last_status_code,
        "last_error": delivery.last_error,
        "delivered_at": delivery.delivered_at,
        "attempts": [{
            "attempt_number": item.attempt_number,
            "outcome": item.outcome,
            "http_status_code": item.http_status_code,
            "response_excerpt": item.response_excerpt,
            "error": item.error,
            "latency_ms": item.latency_ms,
            "started_at": item.started_at,
            "completed_at": item.completed_at,
        } for item in attempts],
    }
