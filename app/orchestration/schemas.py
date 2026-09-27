"""编排子系统的请求模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class NodeInput(BaseModel):
    key: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    stage: str = Field(min_length=1, max_length=60)
    depends_on: list[str] = Field(default_factory=list, max_length=500)
    resources: dict[str, int] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(
        description="节点载荷，必须包含独立的 idempotency_key",
        default_factory=dict,
    )
    priority: int = Field(default=50, ge=0, le=100)
    max_attempts: int = Field(default=3, ge=1, le=20)


class BatchSubmit(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    submitted_by: str = Field(min_length=1, max_length=120)
    # 批次幂等键：与每个节点自身的 idempotency_key 相互独立
    batch_key: str = Field(min_length=6, max_length=160)
    nodes: list[NodeInput] = Field(min_length=1, max_length=500)
    resource_limits: dict[str, int] = Field(default_factory=dict)
    priority: int = Field(default=50, ge=0, le=100)


class ClaimRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    lease_seconds: int = Field(default=60, ge=5, le=3600)
    stages: list[str] | None = Field(default=None, max_length=50)
    max_nodes: int = Field(default=10, ge=1, le=50)


class HeartbeatRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    lease_token: str = Field(min_length=8, max_length=200)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class NodeResultRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    lease_token: str = Field(min_length=8, max_length=200)
    result: dict[str, Any]
    metrics: dict[str, Any] = Field(default_factory=dict)


class NodeFailureRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    lease_token: str = Field(min_length=8, max_length=200)
    error_code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool = True


class ManualSkipRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000, description="人工跳过理由（必填，用于追溯）")


class ManualRetryRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000, description="人工重试理由（必填，用于追溯）")


class CancelBatchRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000, description="取消批次理由（必填，用于追溯）")
