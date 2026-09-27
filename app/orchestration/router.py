from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.orchestration.schemas import (
    BatchRetryRequest,
    BatchSubmit,
    NodeClaim,
    NodeFailure,
    NodeHeartbeat,
    NodeResult,
    SkipRequest,
)
from app.orchestration.service import OrchestrationService

router = APIRouter(prefix="/api/orchestration", tags=["观测分析编排"])


def service() -> OrchestrationService:
    return OrchestrationService()


@router.post("/batches", status_code=202)
def submit_batch(payload: BatchSubmit):
    return service().submit(payload.model_dump())


@router.get("/batches")
def list_batches(status: str | None = None, project_code: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_batches(status=status, project_code=project_code, limit=limit)}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int):
    return service().get_batch(batch_id)


@router.get("/batches/{batch_id}/topology")
def batch_topology(batch_id: int):
    return service().topology(batch_id)


@router.get("/batches/{batch_id}/summary")
def batch_summary(batch_id: int):
    return service().summary(batch_id)


@router.get("/batches/{batch_id}/events")
def batch_events(batch_id: int, limit: int = Query(default=500, ge=1, le=2000)):
    return {"items": service().events(batch_id, limit=limit)}


@router.post("/batches/{batch_id}/retry")
def retry_batch(batch_id: int, payload: BatchRetryRequest, principal: Principal = Depends(current_principal)):
    principal.require("orchestration.retry")
    return service().retry_batch(batch_id, principal.username, payload.reason)


@router.post("/nodes/claim")
def claim_node(payload: NodeClaim):
    return {"node": service().claim(payload.worker_id, payload.batch_id, payload.lease_seconds)}


@router.post("/nodes/{node_id}/heartbeat")
def heartbeat_node(node_id: int, payload: NodeHeartbeat):
    return service().heartbeat(node_id, payload.worker_id, payload.lease_seconds)


@router.post("/nodes/{node_id}/complete")
def complete_node(node_id: int, payload: NodeResult):
    return service().complete(node_id, payload.worker_id, payload.result, payload.metrics)


@router.post("/nodes/{node_id}/fail")
def fail_node(node_id: int, payload: NodeFailure):
    return service().fail(node_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/nodes/{node_id}/skip")
def skip_node(node_id: int, payload: SkipRequest, principal: Principal = Depends(current_principal)):
    principal.require("orchestration.skip")
    return service().skip(node_id, principal.username, payload.reason)


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)
