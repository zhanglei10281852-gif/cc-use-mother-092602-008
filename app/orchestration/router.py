"""观测分析 DAG 编排接口。

工作者接口（领取/续租/回执）与既有 compute 模块一致，使用请求体内的
worker_id + 租约令牌鉴权；人工干预接口要求会话令牌和对应权限。
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.orchestration.schemas import (
    BatchSubmit,
    CancelBatchRequest,
    ClaimRequest,
    HeartbeatRequest,
    ManualRetryRequest,
    ManualSkipRequest,
    NodeFailureRequest,
    NodeResultRequest,
)
from app.orchestration.service import OrchestrationService

router = APIRouter(prefix="/api/orchestration", tags=["观测分析 DAG 编排"])


def service() -> OrchestrationService:
    return OrchestrationService()


@router.post("/batches", status_code=202)
def submit_batch(payload: BatchSubmit):
    return service().submit_batch(payload.model_dump())


@router.get("/batches")
def list_batches(status: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return service().list_batches(status=status, limit=limit)


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int):
    return service().get_batch(batch_id)


@router.get("/batches/{batch_id}/topology")
def get_topology(batch_id: int):
    return service().topology_view(batch_id)


@router.get("/batches/{batch_id}/summary")
def get_summary(batch_id: int):
    batch = service().get_batch(batch_id)
    return {
        "batch_id": batch_id,
        "status": batch["status"],
        "summary_generated_at": batch["summary_generated_at"],
        "summary": batch["summary"],
    }


@router.get("/nodes/{node_id}")
def get_node(node_id: int):
    return service().get_node(node_id)


@router.post("/claim")
def claim(payload: ClaimRequest):
    return service().claim(
        payload.worker_id, payload.lease_seconds,
        stages=payload.stages, max_nodes=payload.max_nodes,
    )


@router.post("/nodes/{node_id}/heartbeat")
def heartbeat(node_id: int, payload: HeartbeatRequest):
    return service().heartbeat(node_id, payload.worker_id, payload.lease_token, payload.lease_seconds)


@router.post("/nodes/{node_id}/complete")
def complete(node_id: int, payload: NodeResultRequest):
    return service().complete(
        node_id, payload.worker_id, payload.lease_token, payload.result, payload.metrics
    )


@router.post("/nodes/{node_id}/fail")
def fail(node_id: int, payload: NodeFailureRequest):
    return service().fail(
        node_id, payload.worker_id, payload.lease_token,
        payload.error_code, payload.message, payload.retryable,
    )


@router.post("/nodes/{node_id}/skip")
def manual_skip(node_id: int, payload: ManualSkipRequest,
                principal: Principal = Depends(current_principal)):
    return service().manual_skip(node_id, principal, payload.reason)


@router.post("/nodes/{node_id}/retry")
def manual_retry(node_id: int, payload: ManualRetryRequest,
                 principal: Principal = Depends(current_principal)):
    return service().manual_retry_node(node_id, principal, payload.reason)


@router.post("/batches/{batch_id}/cancel")
def cancel_batch(batch_id: int, payload: CancelBatchRequest,
                 principal: Principal = Depends(current_principal)):
    return service().cancel_batch(batch_id, principal, payload.reason)


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="system:lease-recovery", min_length=1)):
    return service().recover_expired_leases(actor)
