from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class NodeSpec(BaseModel):
    """批次内单个编排节点的声明，node_key 即节点级幂等键。"""

    node_key: str = Field(min_length=2, max_length=80, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    stage: str = Field(min_length=1, max_length=40, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    depends_on: list[str] = Field(default_factory=list, max_length=100)
    resource_units: int = Field(default=1, ge=1, le=100000)
    max_attempts: int = Field(default=3, ge=1, le=20)
    priority: int = Field(default=50, ge=0, le=100)
    payload: dict[str, Any] = Field(default_factory=dict)


class BatchSubmit(BaseModel):
    """批次提交，batch_key 是批次级幂等键，与节点级 node_key 分属不同命名空间。"""

    batch_key: str = Field(min_length=6, max_length=160)
    name: str = Field(min_length=2, max_length=120)
    project_code: str = Field(min_length=1, max_length=80)
    requested_by: str = Field(min_length=1, max_length=80)
    resource_budget: int = Field(ge=1, le=10_000_000)
    max_parallel: int = Field(default=4, ge=1, le=1000)
    nodes: list[NodeSpec] = Field(min_length=1, max_length=500)


class NodeClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    batch_id: int | None = Field(default=None, ge=1)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class NodeHeartbeat(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class NodeResult(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    result: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)


class NodeFailure(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    error_code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool = True


class SkipRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)


class BatchRetryRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)
