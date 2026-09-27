"""DAG 编排核心服务。

状态机
------
节点：blocked -> ready -> running -> succeeded
     ready/blocked -> skipped（人工跳过或失败传播）/ cancelled
     running -> failed（终态）或 ready（可重试退避后重新领取）
批次：pending -> running -> succeeded | partial | failed
     任意时刻可由有权限的操作者请求取消，进入 cancelling，收敛后为 cancelled/partial。

传播规则
--------
* 上游 failed/cancelled 会使尚未启动的后继直接 skipped（系统传播，不消耗算力）。
* 系统传播的 skipped 会继续向下游级联。
* 人工跳过视为“有意省略该步”，与 succeeded 一样满足后继的就绪条件，
  因此已完成的分支在恢复/重试后仍可被复用。
* 人工重试根因 failed 节点时，仅复活被系统传播跳过的下游；succeeded 节点永不重跑。
"""

from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.core.security import Principal
from app.database import get_connection, transaction
from app.orchestration.store import OrchestrationRepository, ensure_schema
from app.repositories.audit import AuditRepository
from app.orchestration.topology import (
    DONE_STATES,
    TERMINAL_STATES,
    ValidatedBatch,
    validate_batch,
)

SYSTEM_ACTOR = "system:propagator"
RECOVERY_ACTOR = "system:lease-recovery"

# 人工干预所需权限
PERMISSION_SKIP = "orchestration.skip"
PERMISSION_RETRY = "orchestration.retry"
PERMISSION_CANCEL = "orchestration.cancel"

MAX_BACKOFF_SECONDS = 300
MAX_CLAIM_NODES = 50


def digest(value: Any) -> str:
    import hashlib

    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def backoff_seconds(attempt_count: int) -> int:
    return min(MAX_BACKOFF_SECONDS, 2 ** max(0, attempt_count - 1))


class OrchestrationService:
    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        ensure_schema(self.connection)

    # ------------------------------------------------------------------
    # 批次提交
    # ------------------------------------------------------------------
    def submit_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        submitted_by = payload["submitted_by"]
        batch_key = payload["batch_key"]
        topology = validate_batch(payload.get("nodes") or [], payload.get("resource_limits") or {})
        ordered = [topology.nodes[key] for key in sorted(topology.nodes, key=lambda k: topology.index_by_key[k])]
        request_digest = digest({
            "name": payload.get("name", ""),
            "priority": payload.get("priority", 50),
            "resource_limits": topology.resource_limits,
            "nodes": [
                {
                    "key": spec.key, "stage": spec.stage, "depends_on": sorted(spec.dependencies),
                    "resources": spec.resources,
                    "payload": {k: v for k, v in spec.payload.items() if k != "idempotency_key"},
                    "idempotency_key": str(spec.payload.get("idempotency_key", "")).strip(),
                    "max_attempts": spec.max_attempts,
                    "priority": spec.priority,
                }
                for spec in ordered
            ],
        })
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            existing = repository.batch_by_key(submitted_by, batch_key)
            if existing is not None:
                # 批次幂等：同键重放必须对应同一次请求
                row = connection.execute(
                    "SELECT request_digest FROM orch_batches WHERE id=?", (existing["id"],)
                ).fetchone()
                if row["request_digest"] != request_digest:
                    raise ConflictError("同一批次幂等键对应了不同的 DAG 定义")
                return self._batch_view(repository, existing["id"])

            self._ensure_node_keys_absent(connection, topology)

            batch = repository.insert_batch(
                batch_key=batch_key, name=payload.get("name", batch_key), submitted_by=submitted_by,
                request_digest=request_digest, resource_limits=topology.resource_limits,
                priority=int(payload.get("priority", 50)), now=now,
            )
            batch_id = batch["id"]
            for key in topology.topological_order():
                spec = topology.nodes[key]
                node_idempotency = str(spec.payload["idempotency_key"]).strip()
                initial = "ready" if not spec.dependencies else "blocked"
                repository.insert_node(
                    batch_id=batch_id, node_key=key, idempotency_key=node_idempotency,
                    stage=spec.stage, ordinal=topology.index_by_key[key],
                    depends_on=sorted(spec.dependencies), resources=spec.resources,
                    payload={k: v for k, v in spec.payload.items() if k != "idempotency_key"},
                    priority=spec.priority,
                    max_attempts=spec.max_attempts, status=initial, available_at=now, now=now,
                )
                repository.add_event(
                    batch_id=batch_id, node_id=None, event_type="node_declared", actor=submitted_by,
                    detail={"node_key": key, "initial": initial, "depends_on": sorted(spec.dependencies)},
                    now=now,
                )
            repository.add_event(batch_id=batch_id, node_id=None, event_type="batch_submitted",
                                 actor=submitted_by, detail={"nodes": len(topology.nodes)}, now=now)
            return self._batch_view(repository, batch_id)

    @staticmethod
    def _ensure_node_keys_absent(connection: sqlite3.Connection, topology: ValidatedBatch) -> None:
        for spec in topology.nodes.values():
            key = str(spec.payload.get("idempotency_key", "")).strip()
            if not key or len(key) > 160:
                raise ValidationError(
                    "每个节点必须提供独立于批次键的 idempotency_key（长度不超过 160）",
                    context={"node_key": spec.key},
                )
            row = connection.execute(
                "SELECT id,batch_id,node_key,status FROM orch_nodes WHERE idempotency_key=?", (key,)
            ).fetchone()
            if row is not None:
                raise ConflictError(
                    "节点幂等键已被其他节点占用，重复提交不会重新执行",
                    context={"idempotency_key": key, "node_id": row["id"], "batch_id": row["batch_id"],
                             "node_key": row["node_key"], "status": row["status"]},
                )

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_batches(self, *, status: str | None = None, limit: int = 100) -> dict[str, Any]:
        repository = OrchestrationRepository(self.connection)
        return {"items": repository.list_batches(status=status, limit=max(1, min(limit, 500)))}

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        with transaction() as connection:
            return self._batch_view(OrchestrationRepository(connection), batch_id)

    def get_node(self, node_id: int) -> dict[str, Any]:
        repository = OrchestrationRepository(self.connection)
        node = repository.node_by_id(node_id)
        if node is None:
            raise NotFoundError("编排节点不存在")
        node["attempts"] = repository.attempts_of_node(node_id)
        return node

    def topology_view(self, batch_id: int) -> dict[str, Any]:
        repository = OrchestrationRepository(self.connection)
        batch = repository.batch_by_id(batch_id)
        if batch is None:
            raise NotFoundError("批次不存在")
        nodes = repository.nodes_of_batch(batch_id)
        edges = [
            {"from": dep, "to": node["node_key"]}
            for node in nodes for dep in node["depends_on"]
        ]
        return {
            "batch_id": batch_id,
            "batch_key": batch["batch_key"],
            "status": batch["status"],
            "nodes": [
                {
                    "node_key": node["node_key"], "stage": node["stage"], "status": node["status"],
                    "depends_on": node["depends_on"], "resources": node["resources"],
                    "priority": node["priority"], "attempt_count": node["attempt_count"],
                    "max_attempts": node["max_attempts"],
                    "result_digest": node["result_digest"], "skip_reason": node["skip_reason"],
                    "last_error_code": node["last_error_code"],
                }
                for node in nodes
            ],
            "edges": edges,
            "resource_limits": batch["resource_limits"],
        }

    # ------------------------------------------------------------------
    # 领取 / 回执 / 续租
    # ------------------------------------------------------------------
    def claim(self, worker_id: str, lease_seconds: int, *, stages: list[str] | None = None,
              max_nodes: int = 10) -> dict[str, Any]:
        max_nodes = max(1, min(max_nodes, MAX_CLAIM_NODES))
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        stage_filter = tuple(sorted(set(stages))) if stages else None
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            candidates = repository.ready_candidates(now, stage_filter)
            limits = self._batch_limits(repository)
            used = self._resource_usage(repository)
            claimed: list[dict[str, Any]] = []
            # 按就绪顺序贪心领取；只领取在当前剩余容量下可启动的节点。
            for candidate in candidates:
                if len(claimed) >= max_nodes:
                    break
                if not self._fits(candidate, limits, used):
                    continue
                lease_token = secrets.token_urlsafe(16)
                cursor = connection.execute(
                    "UPDATE orch_nodes SET status='running',attempt_count=attempt_count+1,"
                    "lease_owner=?,lease_token=?,lease_expires_at=?,started_at=COALESCE(started_at,?),"
                    "updated_at=?,version=version+1 "
                    "WHERE id=? AND status='ready' AND available_at<=?",
                    (worker_id, lease_token, lease_until, now, now, candidate["id"], now),
                )
                if cursor.rowcount != 1:
                    continue  # 被并发工作者抢先
                node = repository.node_by_id(candidate["id"])
                assert node is not None
                repository.add_attempt(node_id=node["id"], attempt=node["attempt_count"],
                                       worker_id=worker_id, lease_token=lease_token,
                                       outcome="claimed", started_at=now)
                repository.add_event(batch_id=node["batch_id"], node_id=node["id"],
                                     event_type="node_claimed", actor=worker_id,
                                     from_status="ready", to_status="running",
                                     detail={"attempt": node["attempt_count"],
                                             "lease_expires_at": lease_until}, now=now)
                for name, amount in node["resources"].items():
                    if name in limits.get(node["batch_id"], {}):
                        used[(node["batch_id"], name)] = used.get((node["batch_id"], name), 0) + amount
                claimed.append({**node, "lease_token": lease_token, "lease_expires_at": lease_until})
            for node in claimed:
                self._recompute(repository, node["batch_id"], now)
            return {"worker_id": worker_id, "claimed": claimed}

    def heartbeat(self, node_id: int, worker_id: str, lease_token: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            self._require_lease(repository, node_id, worker_id, lease_token)
            connection.execute(
                "UPDATE orch_nodes SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=?",
                (expires, now, node_id),
            )
            result = repository.node_by_id(node_id)
            assert result is not None
            repository.add_event(batch_id=result["batch_id"], node_id=node_id,
                                 event_type="lease_renewed", actor=worker_id,
                                 detail={"lease_expires_at": expires}, now=now)
            return result

    def complete(self, node_id: int, worker_id: str, lease_token: str,
                 result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = self._require_lease(repository, node_id, worker_id, lease_token)
            result_digest = digest({"result": result, "metrics": metrics})
            connection.execute(
                "UPDATE orch_nodes SET status='succeeded',result_json=?,result_digest=?,"
                "lease_owner='',lease_token='',lease_expires_at='',finished_at=?,updated_at=?,"
                "version=version+1,last_error_code='',last_error_message='' WHERE id=?",
                (json.dumps({"result": result, "metrics": metrics}, ensure_ascii=False, sort_keys=True),
                 result_digest, now, now, node_id),
            )
            repository.close_attempt(node_id=node_id, attempt=node["attempt_count"],
                                     outcome="succeeded", ended_at=now, result_digest=result_digest)
            repository.add_event(batch_id=node["batch_id"], node_id=node_id,
                                 event_type="node_succeeded", actor=worker_id,
                                 from_status="running", to_status="succeeded",
                                 detail={"result_digest": result_digest}, now=now)
            return self._recompute(repository, node["batch_id"], now)

    def fail(self, node_id: int, worker_id: str, lease_token: str,
             error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = self._require_lease(repository, node_id, worker_id, lease_token)
            batch = repository.batch_by_id(node["batch_id"])
            assert batch is not None
            cancelling = batch["status"] == "cancelling"
            will_retry = retryable and node["attempt_count"] < node["max_attempts"] and not cancelling
            if will_retry:
                delay = backoff_seconds(node["attempt_count"])
                available = to_storage(now_value + timedelta(seconds=delay))
                connection.execute(
                    "UPDATE orch_nodes SET status='ready',available_at=?,lease_owner='',"
                    "lease_token='',lease_expires_at='',last_error_code=?,last_error_message=?,"
                    "updated_at=?,version=version+1 WHERE id=?",
                    (available, error_code, message, now, node_id),
                )
                repository.close_attempt(node_id=node_id, attempt=node["attempt_count"],
                                         outcome="failed", ended_at=now,
                                         error_code=error_code, message=message)
                repository.add_event(batch_id=node["batch_id"], node_id=node_id,
                                     event_type="node_retry_scheduled", actor=worker_id,
                                     from_status="running", to_status="ready",
                                     detail={"attempt": node["attempt_count"],
                                             "max_attempts": node["max_attempts"],
                                             "backoff_seconds": delay,
                                             "error_code": error_code}, now=now)
                return self._recompute(repository, node["batch_id"], now)

            connection.execute(
                "UPDATE orch_nodes SET status='failed',lease_owner='',lease_token='',"
                "lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,"
                "updated_at=?,version=version+1 WHERE id=?",
                (error_code, message, now, now, node_id),
            )
            repository.close_attempt(node_id=node_id, attempt=node["attempt_count"],
                                     outcome="failed", ended_at=now,
                                     error_code=error_code, message=message)
            repository.add_event(batch_id=node["batch_id"], node_id=node_id,
                                 event_type="node_failed", actor=worker_id,
                                 from_status="running", to_status="failed",
                                 detail={"error_code": error_code, "retryable": retryable,
                                         "attempt": node["attempt_count"]}, now=now)
            return self._recompute(repository, node["batch_id"], now)

    # ------------------------------------------------------------------
    # 人工干预
    # ------------------------------------------------------------------
    def manual_skip(self, node_id: int, principal: Principal, reason: str) -> dict[str, Any]:
        principal.require(PERMISSION_SKIP)
        if not reason or not reason.strip():
            raise ValidationError("人工跳过必须填写理由")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = self._node(repository, node_id)
            if node["status"] not in {"blocked", "ready"}:
                raise ConflictError("只有尚未开始执行的节点可以人工跳过；运行中节点请先取消")
            before = node["status"]
            self._mark_human_skipped(connection, repository, node, principal.username, reason, now)
            result = self._recompute(repository, node["batch_id"], now)
            self._audit(connection, principal, action="orchestration.node.skip",
                        resource_id=node_id, before={"status": before},
                        after={"status": "skipped", "reason": reason},
                        metadata={"batch_id": node["batch_id"], "node_key": node["node_key"]}, now=now)
            return result

    def manual_retry_node(self, node_id: int, principal: Principal, reason: str) -> dict[str, Any]:
        principal.require(PERMISSION_RETRY)
        if not reason or not reason.strip():
            raise ValidationError("人工重试必须填写理由")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = self._node(repository, node_id)
            if node["status"] != "failed":
                raise ConflictError("只有终态失败的节点可以人工重试；成功节点不会被重新执行")
            # 追加一次尝试预算，attempt_count 保留以反映真实执行次数
            connection.execute(
                "UPDATE orch_nodes SET status='ready',available_at=?,max_attempts=max_attempts+1,"
                "lease_owner='',lease_token='',lease_expires_at='',finished_at=NULL,"
                "last_error_code='',last_error_message='',updated_at=?,version=version+1 WHERE id=?",
                (now, now, node_id),
            )
            repository.add_event(batch_id=node["batch_id"], node_id=node_id,
                                 event_type="node_manual_retry", actor=principal.username,
                                 reason=reason, from_status="failed", to_status="ready", now=now)
            self._revive_propagated(connection, repository, node["batch_id"], now)
            revived_view = self._recompute(repository, node["batch_id"], now)
            revived_count = sum(
                1 for n in repository.nodes_of_batch(node["batch_id"])
                if n["status"] in {"blocked", "ready"}
            )
            self._audit(connection, principal, action="orchestration.node.retry",
                        resource_id=node_id, before={"status": "failed"},
                        after={"status": "ready", "reason": reason},
                        metadata={"batch_id": node["batch_id"], "node_key": node["node_key"],
                                  "unblocked_nodes": revived_count}, now=now)
            return revived_view

    def cancel_batch(self, batch_id: int, principal: Principal, reason: str) -> dict[str, Any]:
        principal.require(PERMISSION_CANCEL)
        if not reason or not reason.strip():
            raise ValidationError("取消批次必须填写理由")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            batch = repository.batch_by_id(batch_id)
            if batch is None:
                raise NotFoundError("批次不存在")
            if batch["status"] in {"succeeded", "partial", "failed", "cancelled"}:
                raise ConflictError("批次已经终态，无法取消")
            previous_status = batch["status"]
            repository.touch_batch(batch_id, "cancelling", now=now)
            for node in repository.nodes_of_batch(batch_id):
                if node["status"] in {"blocked", "ready"}:
                    connection.execute(
                        "UPDATE orch_nodes SET status='cancelled',finished_at=?,updated_at=?,"
                        "version=version+1 WHERE id=?",
                        (now, now, node["id"]),
                    )
                    repository.add_event(batch_id=batch_id, node_id=node["id"],
                                         event_type="node_cancelled", actor=principal.username,
                                         reason=reason, from_status=node["status"],
                                         to_status="cancelled", now=now)
                elif node["status"] == "running":
                    repository.add_event(batch_id=batch_id, node_id=node["id"],
                                         event_type="node_cancel_requested", actor=principal.username,
                                         reason=reason, from_status="running",
                                         to_status="running", now=now)
            repository.add_event(batch_id=batch_id, node_id=None, event_type="batch_cancel_requested",
                                 actor=principal.username, reason=reason,
                                 from_status=previous_status, to_status="cancelling", now=now)
            result = self._recompute(repository, batch_id, now)
            self._audit(connection, principal, action="orchestration.batch.cancel",
                        resource_id=batch_id, before={"status": previous_status},
                        after={"status": result["status"], "reason": reason},
                        metadata={"cancelled_nodes": [
                            n["node_key"] for n in result["nodes"] if n["status"] == "cancelled"
                        ]}, now=now)
            return result

    @staticmethod
    def _audit(connection: sqlite3.Connection, principal: Principal, *, action: str,
               resource_id: int, before: dict[str, Any], after: dict[str, Any],
               metadata: dict[str, Any], now: str) -> None:
        AuditRepository(connection).append(
            actor_user_id=principal.user_id, actor_name=principal.username, action=action,
            resource_type="orchestration", resource_id=resource_id, outcome="success",
            before=before, after=after, metadata=metadata, correlation_id=None, created_at=now,
        )

    # ------------------------------------------------------------------
    # 租约恢复
    # ------------------------------------------------------------------
    def recover_expired_leases(self, actor: str = RECOVERY_ACTOR) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        touched_batches: set[int] = set()
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            for node in repository.expired_running_nodes(now):
                batch = repository.batch_by_id(node["batch_id"])
                cancelling = batch is not None and batch["status"] == "cancelling"
                delay = backoff_seconds(node["attempt_count"])
                will_retry = node["attempt_count"] < node["max_attempts"] and not cancelling
                if will_retry:
                    available = to_storage(now_value + timedelta(seconds=delay))
                    connection.execute(
                        "UPDATE orch_nodes SET status='ready',available_at=?,lease_owner='',"
                        "lease_token='',lease_expires_at='',updated_at=?,version=version+1 WHERE id=?",
                        (available, now, node["id"]),
                    )
                    repository.close_attempt(node_id=node["id"], attempt=node["attempt_count"],
                                             outcome="recovered", ended_at=now,
                                             error_code="lease_expired",
                                             message="工作者租约过期，尝试重新排队")
                    repository.add_event(batch_id=node["batch_id"], node_id=node["id"],
                                         event_type="node_lease_recovered", actor=actor,
                                         from_status="running", to_status="ready",
                                         detail={"backoff_seconds": delay}, now=now)
                    recovered.append(node["id"])
                else:
                    connection.execute(
                        "UPDATE orch_nodes SET status='failed',lease_owner='',lease_token='',"
                        "lease_expires_at='',last_error_code='lease_expired',"
                        "last_error_message='工作者租约过期且尝试次数耗尽',finished_at=?,"
                        "updated_at=?,version=version+1 WHERE id=?",
                        (now, now, node["id"]),
                    )
                    repository.close_attempt(node_id=node["id"], attempt=node["attempt_count"],
                                             outcome="failed", ended_at=now,
                                             error_code="lease_expired",
                                             message="工作者租约过期且尝试次数耗尽")
                    repository.add_event(batch_id=node["batch_id"], node_id=node["id"],
                                         event_type="node_failed", actor=actor,
                                         from_status="running", to_status="failed",
                                         detail={"error_code": "lease_expired", "lease": True},
                                         now=now)
                    exhausted.append(node["id"])
                touched_batches.add(node["batch_id"])
            for batch_id in touched_batches:
                self._recompute(repository, batch_id, now)
        return {"recovered": recovered, "exhausted": exhausted}

    # ------------------------------------------------------------------
    # 内部：传播 / 收敛 / 摘要
    # ------------------------------------------------------------------
    def _recompute(self, repository: OrchestrationRepository, batch_id: int, now: str) -> dict[str, Any]:
        self._propagate(repository, batch_id, now)
        nodes = repository.nodes_of_batch(batch_id)
        batch = repository.batch_by_id(batch_id)
        assert batch is not None

        started = any(node["started_at"] for node in nodes)
        all_terminal = all(node["status"] in TERMINAL_STATES for node in nodes)
        cancel_requested = batch["status"] == "cancelling"

        if cancel_requested and not all_terminal:
            new_status = "cancelling"
        elif not all_terminal:
            new_status = "running" if started else "pending"
        else:
            succeeded = [n for n in nodes if n["status"] == "succeeded"]
            human_skipped = [n for n in nodes if n["status"] == "skipped" and not self._is_propagated(n)]
            hard_failed = [n for n in nodes if n["status"] in {"failed", "cancelled"}]
            if not hard_failed and all(
                n["status"] == "succeeded" or self._is_human_skipped(n) for n in nodes
            ):
                new_status = "succeeded"
            elif cancel_requested:
                new_status = "partial" if succeeded else "cancelled"
            elif succeeded or human_skipped:
                new_status = "partial" if hard_failed else "succeeded"
            else:
                new_status = "failed"

        finished = now if new_status in {"succeeded", "partial", "failed", "cancelled"} else None
        repository.touch_batch(batch_id, new_status, now=now, started=started, finished=finished)

        summary = self._build_summary(repository, batch_id, new_status, now)
        repository.save_summary(batch_id, summary, now)

        view = self._batch_view(repository, batch_id)
        return view

    def _propagate(self, repository: OrchestrationRepository, batch_id: int, now: str) -> None:
        """固定点传播：失败/取消/系统跳过阻断后继；全部上游完成则解除阻塞。"""
        nodes = {n["node_key"]: n for n in repository.nodes_of_batch(batch_id)}
        changed = True
        while changed:
            changed = False
            for node in nodes.values():
                if node["status"] not in {"blocked", "ready"}:
                    continue
                deps = [nodes[key] for key in node["depends_on"]]
                blocker = next(
                    (d for d in deps
                     if d["status"] in {"failed", "cancelled"} or self._is_propagated(d)),
                    None,
                )
                if blocker is not None:
                    # ready 节点理论上不会再出现阻断上游（成功态不可逆），这里做防御性处理。
                    self._mark_propagated_skipped(repository, node, blocker, now)
                    refreshed = repository.node_by_id(node["id"])
                    assert refreshed is not None
                    nodes[node["node_key"]] = refreshed
                    changed = True
                    continue
                if all(d["status"] in DONE_STATES for d in deps):
                    if node["status"] == "blocked":
                        self._flip_ready(repository, node, now)
                        refreshed = repository.node_by_id(node["id"])
                        assert refreshed is not None
                        nodes[node["node_key"]] = refreshed
                        changed = True

    def _revive_propagated(self, connection: sqlite3.Connection, repository: OrchestrationRepository,
                           batch_id: int, now: str) -> None:
        """根因节点重试后，复活仅因系统传播而跳过的下游；成功节点保持不动。"""
        nodes = {n["node_key"]: n for n in repository.nodes_of_batch(batch_id)}
        changed = True
        while changed:
            changed = False
            for node in nodes.values():
                if node["status"] != "skipped" or not self._is_propagated(node):
                    continue
                deps = [nodes[key] for key in node["depends_on"]]
                # 仍有硬失败/取消上游则不复活；若上游也是待复活的传播跳过，本轮先跳过
                if any(d["status"] in {"failed", "cancelled"} for d in deps):
                    continue
                if any(self._is_propagated(d) for d in deps):
                    continue
                connection.execute(
                    "UPDATE orch_nodes SET status='blocked',skipped_by='',skip_reason='',"
                    "finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                    (now, node["id"]),
                )
                repository.add_event(batch_id=batch_id, node_id=node["id"],
                                     event_type="node_revived", actor=SYSTEM_ACTOR,
                                     from_status="skipped", to_status="blocked", now=now)
                refreshed = repository.node_by_id(node["id"])
                assert refreshed is not None
                nodes[node["node_key"]] = refreshed
                changed = True

    @staticmethod
    def _is_propagated(node: dict[str, Any]) -> bool:
        return node["status"] == "skipped" and node["skipped_by"] == SYSTEM_ACTOR

    @staticmethod
    def _is_human_skipped(node: dict[str, Any]) -> bool:
        return node["status"] == "skipped" and node["skipped_by"] not in {"", SYSTEM_ACTOR}

    def _flip_ready(self, repository: OrchestrationRepository, node: dict[str, Any], now: str) -> None:
        repository.connection.execute(
            "UPDATE orch_nodes SET status='ready',available_at=?,updated_at=?,version=version+1 WHERE id=?",
            (now, now, node["id"]),
        )
        repository.add_event(batch_id=node["batch_id"], node_id=node["id"],
                             event_type="node_unblocked", actor=SYSTEM_ACTOR,
                             from_status="blocked", to_status="ready", now=now)

    def _mark_propagated_skipped(self, repository: OrchestrationRepository, node: dict[str, Any],
                                 blocker: dict[str, Any], now: str) -> None:
        reason = f"上游节点 {blocker['node_key']} 状态为 {blocker['status']}，后继不再调度"
        repository.connection.execute(
            "UPDATE orch_nodes SET status='skipped',skipped_by=?,skip_reason=?,finished_at=?,"
            "updated_at=?,version=version+1 WHERE id=?",
            (SYSTEM_ACTOR, reason, now, now, node["id"]),
        )
        repository.add_event(batch_id=node["batch_id"], node_id=node["id"],
                             event_type="node_skip_propagated", actor=SYSTEM_ACTOR,
                             reason=reason, from_status=node["status"], to_status="skipped",
                             detail={"blocker": blocker["node_key"],
                                     "blocker_status": blocker["status"]}, now=now)

    def _mark_human_skipped(self, connection: sqlite3.Connection, repository: OrchestrationRepository,
                            node: dict[str, Any], actor: str, reason: str, now: str) -> None:
        connection.execute(
            "UPDATE orch_nodes SET status='skipped',skipped_by=?,skip_reason=?,finished_at=?,"
            "updated_at=?,version=version+1 WHERE id=?",
            (actor, reason, now, now, node["id"]),
        )
        repository.add_event(batch_id=node["batch_id"], node_id=node["id"],
                             event_type="node_skip_manual", actor=actor, reason=reason,
                             from_status=node["status"], to_status="skipped", now=now)

    # ------------------------------------------------------------------
    # 摘要与视图
    # ------------------------------------------------------------------
    def _build_summary(self, repository: OrchestrationRepository, batch_id: int,
                       batch_status: str, now: str) -> dict[str, Any]:
        nodes = repository.nodes_of_batch(batch_id)
        by_key = {n["node_key"]: n for n in nodes}
        state_counts: dict[str, int] = {}
        for node in nodes:
            state_counts[node["status"]] = state_counts.get(node["status"], 0) + 1

        root_causes: list[dict[str, Any]] = []
        for node in nodes:
            if node["status"] in {"failed", "cancelled"}:
                root_causes.append({
                    "node_key": node["node_key"], "stage": node["stage"],
                    "status": node["status"], "error_code": node["last_error_code"],
                    "error_message": node["last_error_message"],
                    "attempt_count": node["attempt_count"],
                    "skipped_descendants": self._propagated_descendants(by_key, node["node_key"]),
                })

        completed = [
            {
                "node_key": n["node_key"], "stage": n["stage"], "status": n["status"],
                "attempt_count": n["attempt_count"], "result_digest": n["result_digest"],
                "finished_at": n["finished_at"],
                "skip_reason": n["skip_reason"] if n["status"] == "skipped" else "",
                "skipped_by": n["skipped_by"] if n["status"] == "skipped" else "",
            }
            for n in nodes
            if n["status"] == "succeeded" or self._is_human_skipped(n)
        ]
        trace = [
            {
                "node_key": n["node_key"], "stage": n["stage"], "status": n["status"],
                "depends_on": n["depends_on"], "attempt_count": n["attempt_count"],
                "max_attempts": n["max_attempts"], "result_digest": n["result_digest"],
                "error_code": n["last_error_code"], "skip_reason": n["skip_reason"],
                "lease_owner": n["lease_owner"], "started_at": n["started_at"],
                "finished_at": n["finished_at"],
            }
            for n in nodes
        ]
        summary = {
            "generated_at": now,
            "batch_status": batch_status,
            "total_nodes": len(nodes),
            "state_counts": state_counts,
            "completed_branches": completed,
            "root_causes": root_causes,
            "node_trace": trace,
        }
        summary["summary_digest"] = digest(trace)
        return summary

    def _propagated_descendants(self, by_key: dict[str, dict[str, Any]], root_key: str) -> list[str]:
        children: dict[str, list[str]] = {}
        for key, node in by_key.items():
            for dep in node["depends_on"]:
                children.setdefault(dep, []).append(key)
        result: list[str] = []
        stack = list(children.get(root_key, ()))
        seen: set[str] = set()
        while stack:
            key = stack.pop()
            if key in seen:
                continue
            seen.add(key)
            node = by_key[key]
            if node["status"] == "skipped" and node["skipped_by"] == SYSTEM_ACTOR:
                result.append(key)
                stack.extend(children.get(key, ()))
        return sorted(result)

    def _batch_view(self, repository: OrchestrationRepository, batch_id: int) -> dict[str, Any]:
        batch = repository.batch_by_id(batch_id)
        if batch is None:
            raise NotFoundError("批次不存在")
        nodes = repository.nodes_of_batch(batch_id)
        batch["nodes"] = nodes
        batch["events"] = repository.events_of_batch(batch_id)
        return batch

    # ------------------------------------------------------------------
    # 资源与租约辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _batch_limits(repository: OrchestrationRepository) -> dict[int, dict[str, int]]:
        limits: dict[int, dict[str, int]] = {}
        for row in repository.connection.execute("SELECT id,resource_limits_json FROM orch_batches"):
            limits[row["id"]] = json.loads(row["resource_limits_json"])
        return limits

    @staticmethod
    def _resource_usage(repository: OrchestrationRepository) -> dict[tuple[int, str], int]:
        """统计每个批次当前运行节点对受限资源的占用量。"""
        used: dict[tuple[int, str], int] = {}
        for node in repository.running_nodes():
            for name, amount in node["resources"].items():
                key = (node["batch_id"], name)
                used[key] = used.get(key, 0) + amount
        return used

    @staticmethod
    def _fits(node: dict[str, Any], limits: dict[int, dict[str, int]],
              used: dict[tuple[int, str], int]) -> bool:
        for name, amount in node["resources"].items():
            capacity = limits.get(node["batch_id"], {}).get(name)
            if capacity is None:
                continue  # 未声明容量的资源只记账不限量
            if used.get((node["batch_id"], name), 0) + amount > capacity:
                return False
        return True

    @staticmethod
    def _node(repository: OrchestrationRepository, node_id: int) -> dict[str, Any]:
        node = repository.node_by_id(node_id)
        if node is None:
            raise NotFoundError("编排节点不存在")
        return node

    @staticmethod
    def _require_lease(repository: OrchestrationRepository, node_id: int,
                       worker_id: str, lease_token: str) -> dict[str, Any]:
        node = repository.node_by_id(node_id)
        if node is None:
            raise NotFoundError("编排节点不存在")
        if node["status"] != "running":
            raise ConflictError(f"节点当前状态为 {node['status']}，不是运行中")
        lease = repository.connection.execute(
            "SELECT lease_owner,lease_token FROM orch_nodes WHERE id=?", (node_id,)
        ).fetchone()
        if lease["lease_owner"] != worker_id or lease["lease_token"] != lease_token:
            raise PermissionDeniedError("工作者身份或租约令牌不匹配")
        return node
