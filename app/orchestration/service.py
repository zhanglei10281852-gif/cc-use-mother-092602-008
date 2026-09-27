from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.orchestration.repository import OrchestrationRepository


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class OrchestrationService:
    """管理 DAG 任务批次：提交校验、就绪调度、失败传播、人工跳过与恢复。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = OrchestrationRepository(self.connection)

    # 提交与校验

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        nodes = payload["nodes"]
        self._validate_graph(nodes, payload["resource_budget"])
        now = to_storage(self.clock.now())
        batch_digest = digest({
            "name": payload["name"], "project_code": payload["project_code"],
            "resource_budget": payload["resource_budget"], "max_parallel": payload["max_parallel"],
            "nodes": nodes,
        })
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            existing = repository.batch_by_key(payload["requested_by"], payload["batch_key"])
            if existing is not None:
                if existing["batch_digest"] != batch_digest:
                    raise ConflictError("同一批次幂等键对应了不同的批次内容")
                return self._batch_detail(repository, existing["id"])
            reserved = sum(int(node["resource_units"]) for node in nodes)
            batch = repository.create_batch(
                batch_key=payload["batch_key"], batch_digest=batch_digest, name=payload["name"],
                project_code=payload["project_code"], requested_by=payload["requested_by"],
                resource_budget=payload["resource_budget"], reserved_units=reserved,
                max_parallel=payload["max_parallel"], node_count=len(nodes), now=now,
            )
            repository.add_event(
                batch_id=batch["id"], node_id=None, event_type="batch.created", actor=payload["requested_by"],
                detail={"name": payload["name"], "node_count": len(nodes), "reserved_units": reserved}, now=now,
            )
            id_by_key: dict[str, int] = {}
            for node in nodes:
                status = "pending" if node["depends_on"] else "ready"
                created = repository.create_node(
                    batch_id=batch["id"], node_key=node["node_key"], stage=node["stage"],
                    payload=node["payload"], resource_units=node["resource_units"],
                    max_attempts=node["max_attempts"], priority=node["priority"], status=status, now=now,
                )
                id_by_key[node["node_key"]] = created["id"]
                if status == "ready":
                    repository.add_event(
                        batch_id=batch["id"], node_id=created["id"], event_type="node.ready",
                        actor=payload["requested_by"], to_status="ready", now=now,
                    )
            for node in nodes:
                for dependency in node["depends_on"]:
                    repository.add_edge(batch_id=batch["id"], node_id=id_by_key[node["node_key"]], depends_on_id=id_by_key[dependency])
            return self._batch_detail(repository, batch["id"])

    @staticmethod
    def _validate_graph(nodes: list[dict[str, Any]], resource_budget: int) -> None:
        keys = [node["node_key"] for node in nodes]
        duplicated = sorted({key for key in keys if keys.count(key) > 1})
        if duplicated:
            raise ValidationError("节点幂等键在同一批次内重复", context={"node_keys": duplicated})
        known = set(keys)
        for node in nodes:
            unknown = sorted(set(node["depends_on"]) - known)
            if unknown:
                raise ValidationError(f"节点 {node['node_key']} 依赖了不存在的节点", context={"dependencies": unknown})
            if node["node_key"] in node["depends_on"]:
                raise ValidationError(f"节点 {node['node_key']} 不能依赖自身")
            if int(node["resource_units"]) > resource_budget:
                raise ValidationError(f"节点 {node['node_key']} 的算力需求超出批次资源预算")
        required = sum(int(node["resource_units"]) for node in nodes)
        if required > resource_budget:
            raise ValidationError("批次总算力需求超出资源预算", context={"required": required, "resource_budget": resource_budget})
        indegree = {key: 0 for key in keys}
        downstream: dict[str, list[str]] = {key: [] for key in keys}
        for node in nodes:
            for dependency in set(node["depends_on"]):
                indegree[node["node_key"]] += 1
                downstream[dependency].append(node["node_key"])
        queue = sorted(key for key, degree in indegree.items() if degree == 0)
        visited = 0
        while queue:
            current = queue.pop(0)
            visited += 1
            for follower in downstream[current]:
                indegree[follower] -= 1
                if indegree[follower] == 0:
                    queue.append(follower)
        if visited != len(keys):
            cycle = sorted(key for key, degree in indegree.items() if degree > 0)
            raise ValidationError("依赖图存在循环，无法编排", context={"nodes": cycle})

    # 查询

    def list_batches(self, *, status: str | None = None, project_code: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_batches(status=status, project_code=project_code, limit=max(1, min(limit, 500)))

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        repository = self.repository
        if repository.batch_by_id(batch_id) is None:
            raise NotFoundError("编排批次不存在")
        return self._batch_detail(repository, batch_id)

    def _batch_detail(self, repository: OrchestrationRepository, batch_id: int) -> dict[str, Any]:
        batch = dict(repository.batch_by_id(batch_id))
        nodes = repository.nodes_of_batch(batch_id)
        key_by_id = {node["id"]: node["node_key"] for node in nodes}
        dependencies: dict[int, list[str]] = {node["id"]: [] for node in nodes}
        for edge in repository.edges_of_batch(batch_id):
            dependencies[edge["node_id"]].append(key_by_id[edge["depends_on_id"]])
        for node in nodes:
            node["depends_on"] = sorted(dependencies[node["id"]])
        batch["nodes"] = nodes
        batch["counts"] = repository.status_counts(batch_id)
        return batch

    def topology(self, batch_id: int) -> dict[str, Any]:
        repository = self.repository
        batch = repository.batch_by_id(batch_id)
        if batch is None:
            raise NotFoundError("编排批次不存在")
        nodes = repository.nodes_of_batch(batch_id)
        key_by_id = {node["id"]: node["node_key"] for node in nodes}
        edges = repository.edges_of_batch(batch_id)
        indegree = {node["node_key"]: 0 for node in nodes}
        downstream: dict[str, list[str]] = {node["node_key"]: [] for node in nodes}
        for edge in edges:
            upstream, downstream_key = key_by_id[edge["depends_on_id"]], key_by_id[edge["node_id"]]
            indegree[downstream_key] += 1
            downstream[upstream].append(downstream_key)
        levels: dict[str, int] = {}
        queue = sorted(key for key, degree in indegree.items() if degree == 0)
        for key in queue:
            levels[key] = 0
        while queue:
            current = queue.pop(0)
            for follower in downstream[current]:
                indegree[follower] -= 1
                levels[follower] = max(levels.get(follower, 0), levels[current] + 1)
                if indegree[follower] == 0:
                    queue.append(follower)
        return {
            "batch": dict(batch),
            "nodes": [
                {
                    "node_key": node["node_key"], "stage": node["stage"], "status": node["status"],
                    "level": levels.get(node["node_key"], 0), "resource_units": node["resource_units"],
                    "attempt_count": node["attempt_count"],
                    "depends_on": sorted(key_by_id[edge["depends_on_id"]] for edge in edges if edge["node_id"] == node["id"]),
                }
                for node in nodes
            ],
            "edges": [
                {"from": key_by_id[edge["depends_on_id"]], "to": key_by_id[edge["node_id"]]}
                for edge in edges
            ],
        }

    def events(self, batch_id: int, limit: int = 500) -> list[dict[str, Any]]:
        if self.repository.batch_by_id(batch_id) is None:
            raise NotFoundError("编排批次不存在")
        return self.repository.events_of_batch(batch_id, limit=max(1, min(limit, 2000)))

    def summary(self, batch_id: int) -> dict[str, Any]:
        repository = self.repository
        batch = repository.batch_by_id(batch_id)
        if batch is None:
            raise NotFoundError("编排批次不存在")
        nodes = repository.nodes_of_batch(batch_id)
        events = repository.events_of_batch(batch_id)
        counts = repository.status_counts(batch_id)
        stages: dict[str, dict[str, int]] = {}
        for node in nodes:
            stage_counts = stages.setdefault(node["stage"], {})
            stage_counts[node["status"]] = stage_counts.get(node["status"], 0) + 1
        running_units = sum(int(node["resource_units"]) for node in nodes if node["status"] == "running")
        summary_digest = digest({
            "batch_id": batch_id,
            "batch_status": batch["status"],
            "nodes": [(node["node_key"], node["status"], node["attempt_count"]) for node in nodes],
            "events": [event["id"] for event in events],
        })
        return {
            "batch": dict(batch),
            "counts": counts,
            "stages": stages,
            "resource": {
                "budget": batch["resource_budget"],
                "reserved_units": batch["reserved_units"],
                "running_units": running_units,
                "max_parallel": batch["max_parallel"],
            },
            "nodes": nodes,
            "events": events,
            "digest": summary_digest,
        }

    # 调度与执行回执

    def claim(self, worker_id: str, batch_id: int | None, lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            for candidate in repository.ready_candidates(batch_id=batch_id, now=now):
                batch = repository.batch_by_id(candidate["batch_id"])
                if repository.running_count(batch["id"]) >= int(batch["max_parallel"]):
                    continue
                cursor = connection.execute(
                    "UPDATE orchestration_nodes SET status='running',attempt_count=attempt_count+1,worker_id=?,lease_expires_at=?,"
                    "started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='ready'",
                    (worker_id, lease_until, now, now, candidate["id"]),
                )
                if cursor.rowcount != 1:
                    continue
                repository.add_event(
                    batch_id=candidate["batch_id"], node_id=candidate["id"], event_type="node.claimed", actor=worker_id,
                    from_status="ready", to_status="running", detail={"lease_expires_at": lease_until}, now=now,
                )
                return dict(repository.node_by_id(candidate["id"]))
            return None

    def heartbeat(self, node_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE orchestration_nodes SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND worker_id=?",
                (expires, now, node_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("节点未由当前工作者持有")
            return dict(OrchestrationRepository(connection).node_by_id(node_id))

    def complete(self, node_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = self._require_running(repository, node_id, worker_id)
            connection.execute(
                "UPDATE orchestration_nodes SET status='succeeded',result_json=?,worker_id='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (json.dumps(result, ensure_ascii=False, sort_keys=True), now, now, node_id),
            )
            repository.add_event(
                batch_id=node["batch_id"], node_id=node_id, event_type="node.completed", actor=worker_id,
                from_status="running", to_status="succeeded", detail={"metrics": metrics}, now=now,
            )
            self._promote_downstream(connection, repository, node, now, worker_id)
            self._refresh_batch_status(connection, repository, node["batch_id"], now, worker_id)
            return dict(repository.node_by_id(node_id))

    def fail(self, node_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = self._require_running(repository, node_id, worker_id)
            can_retry = retryable and int(node["attempt_count"]) < int(node["max_attempts"])
            if can_retry:
                delay = min(300, 2 ** max(0, int(node["attempt_count"]) - 1))
                available = to_storage(now_value + timedelta(seconds=delay))
                connection.execute(
                    "UPDATE orchestration_nodes SET status='ready',available_at=?,worker_id='',lease_expires_at='',"
                    "last_error_code=?,last_error_message=?,updated_at=?,version=version+1 WHERE id=?",
                    (available, error_code, message[:2000], now, node_id),
                )
                repository.add_event(
                    batch_id=node["batch_id"], node_id=node_id, event_type="node.requeued", actor=worker_id,
                    from_status="running", to_status="ready",
                    detail={"error_code": error_code, "message": message[:2000], "retry_delay_seconds": delay}, now=now,
                )
            else:
                connection.execute(
                    "UPDATE orchestration_nodes SET status='failed',worker_id='',lease_expires_at='',"
                    "last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (error_code, message[:2000], now, now, node_id),
                )
                repository.add_event(
                    batch_id=node["batch_id"], node_id=node_id, event_type="node.failed", actor=worker_id,
                    from_status="running", to_status="failed",
                    detail={"error_code": error_code, "message": message[:2000], "retryable": retryable}, now=now,
                )
                self._propagate_failure(connection, repository, node, now, worker_id)
            self._refresh_batch_status(connection, repository, node["batch_id"], now, worker_id)
            return dict(repository.node_by_id(node_id))

    # 人工干预与恢复

    def skip(self, node_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            node = repository.node_by_id(node_id)
            if node is None:
                raise NotFoundError("编排节点不存在")
            if node["status"] not in {"pending", "ready", "blocked", "failed"}:
                raise ConflictError("当前节点状态不允许人工跳过")
            connection.execute(
                "UPDATE orchestration_nodes SET status='skipped',skip_reason=?,skipped_by=?,worker_id='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (reason, actor, now, now, node_id),
            )
            repository.add_event(
                batch_id=node["batch_id"], node_id=node_id, event_type="node.skipped", actor=actor,
                from_status=node["status"], to_status="skipped", detail={"reason": reason}, now=now,
            )
            self._promote_downstream(connection, repository, node, now, actor)
            self._refresh_batch_status(connection, repository, node["batch_id"], now, actor)
            return dict(repository.node_by_id(node_id))

    def retry_batch(self, batch_id: int, actor: str, reason: str) -> dict[str, Any]:
        """恢复批次：失败与受阻节点重新排队，已成功与已跳过节点保持原样不复算。"""
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            batch = repository.batch_by_id(batch_id)
            if batch is None:
                raise NotFoundError("编排批次不存在")
            if batch["status"] == "completed":
                raise ConflictError("批次已完成，无需恢复")
            reset: list[str] = []
            reopened: list[str] = []
            kept_succeeded: list[str] = []
            kept_skipped: list[str] = []
            for node in repository.nodes_of_batch(batch_id):
                if node["status"] == "failed":
                    connection.execute(
                        "UPDATE orchestration_nodes SET status='ready',attempt_count=0,available_at=?,worker_id='',lease_expires_at='',"
                        "last_error_code='',last_error_message='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                        (now, now, node["id"]),
                    )
                    repository.add_event(
                        batch_id=batch_id, node_id=node["id"], event_type="node.reset", actor=actor,
                        from_status="failed", to_status="ready", detail={"reason": reason}, now=now,
                    )
                    reset.append(node["node_key"])
                elif node["status"] == "blocked":
                    connection.execute(
                        "UPDATE orchestration_nodes SET status='pending',updated_at=?,version=version+1 WHERE id=?",
                        (now, node["id"]),
                    )
                    repository.add_event(
                        batch_id=batch_id, node_id=node["id"], event_type="node.reopened", actor=actor,
                        from_status="blocked", to_status="pending", detail={"reason": reason}, now=now,
                    )
                    reopened.append(node["node_key"])
                elif node["status"] == "succeeded":
                    kept_succeeded.append(node["node_key"])
                elif node["status"] == "skipped":
                    kept_skipped.append(node["node_key"])
            refreshed = {node["node_key"]: node for node in repository.nodes_of_batch(batch_id)}
            for node_key in reopened:
                node = refreshed[node_key]
                if repository.unsatisfied_dependency_count(node["id"]) == 0:
                    connection.execute(
                        "UPDATE orchestration_nodes SET status='ready',available_at=?,updated_at=?,version=version+1 WHERE id=? AND status='pending'",
                        (now, now, node["id"]),
                    )
                    repository.add_event(
                        batch_id=batch_id, node_id=node["id"], event_type="node.ready", actor=actor,
                        from_status="pending", to_status="ready", detail={"promoted_by": "batch_retry"}, now=now,
                    )
            repository.add_event(
                batch_id=batch_id, node_id=None, event_type="batch.retried", actor=actor,
                detail={"reason": reason, "reset": reset, "reopened": reopened,
                        "kept_succeeded": kept_succeeded, "kept_skipped": kept_skipped},
                now=now,
            )
            status = self._refresh_batch_status(connection, repository, batch_id, now, actor)
            return {
                "batch_id": batch_id, "batch_status": status, "reset": reset, "reopened": reopened,
                "kept_succeeded": kept_succeeded, "kept_skipped": kept_skipped,
            }

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = OrchestrationRepository(connection)
            affected_batches: set[int] = set()
            for node in repository.expired_running(now):
                if int(node["attempt_count"]) < int(node["max_attempts"]):
                    connection.execute(
                        "UPDATE orchestration_nodes SET status='ready',available_at=?,worker_id='',lease_expires_at='',"
                        "last_error_code='lease_expired',last_error_message='工作者租约已过期',updated_at=?,version=version+1 WHERE id=?",
                        (now, now, node["id"]),
                    )
                    repository.add_event(
                        batch_id=node["batch_id"], node_id=node["id"], event_type="lease.recovered", actor=actor,
                        from_status="running", to_status="ready", detail={"reason": "租约过期自动恢复"}, now=now,
                    )
                    recovered.append(int(node["id"]))
                else:
                    connection.execute(
                        "UPDATE orchestration_nodes SET status='failed',worker_id='',lease_expires_at='',"
                        "last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, now, node["id"]),
                    )
                    repository.add_event(
                        batch_id=node["batch_id"], node_id=node["id"], event_type="node.failed", actor=actor,
                        from_status="running", to_status="failed", detail={"error_code": "lease_expired"}, now=now,
                    )
                    self._propagate_failure(connection, repository, node, now, actor)
                    exhausted.append(int(node["id"]))
                affected_batches.add(int(node["batch_id"]))
            for batch_id in sorted(affected_batches):
                self._refresh_batch_status(connection, repository, batch_id, now, actor)
        return {"recovered": recovered, "exhausted": exhausted}

    # 内部 helpers

    @staticmethod
    def _require_running(repository: OrchestrationRepository, node_id: int, worker_id: str) -> sqlite3.Row:
        node = repository.node_by_id(node_id)
        if node is None:
            raise NotFoundError("编排节点不存在")
        if node["status"] != "running" or node["worker_id"] != worker_id:
            raise ConflictError("节点未由当前工作者持有")
        return node

    def _promote_downstream(self, connection: sqlite3.Connection, repository: OrchestrationRepository,
                            node: sqlite3.Row, now: str, actor: str) -> None:
        for downstream in repository.downstream_nodes(node["id"]):
            if downstream["status"] not in {"pending", "blocked"}:
                continue
            if repository.unsatisfied_dependency_count(downstream["id"]) > 0:
                continue
            connection.execute(
                "UPDATE orchestration_nodes SET status='ready',available_at=?,updated_at=?,version=version+1 WHERE id=? AND status=?",
                (now, now, downstream["id"], downstream["status"]),
            )
            repository.add_event(
                batch_id=downstream["batch_id"], node_id=downstream["id"], event_type="node.ready", actor=actor,
                from_status=downstream["status"], to_status="ready", detail={"promoted_by": node["node_key"]}, now=now,
            )

    def _propagate_failure(self, connection: sqlite3.Connection, repository: OrchestrationRepository,
                           failed_node: sqlite3.Row, now: str, actor: str) -> None:
        failed_node = repository.node_by_id(failed_node["id"])
        queue = [failed_node]
        visited = {failed_node["id"]}
        while queue:
            current = queue.pop(0)
            for downstream in repository.downstream_nodes(current["id"]):
                if downstream["id"] in visited:
                    continue
                visited.add(downstream["id"])
                if downstream["status"] not in {"pending", "ready"}:
                    continue
                connection.execute(
                    "UPDATE orchestration_nodes SET status='blocked',updated_at=?,version=version+1 WHERE id=?",
                    (now, downstream["id"]),
                )
                repository.add_event(
                    batch_id=downstream["batch_id"], node_id=downstream["id"], event_type="node.blocked", actor=actor,
                    from_status=downstream["status"], to_status="blocked",
                    detail={"cause_node": failed_node["node_key"], "cause_error": failed_node["last_error_code"]}, now=now,
                )
                queue.append(dict(downstream, status="blocked"))

    @staticmethod
    def _refresh_batch_status(connection: sqlite3.Connection, repository: OrchestrationRepository,
                              batch_id: int, now: str, actor: str) -> str:
        del connection
        counts = repository.status_counts(batch_id)
        active = counts.get("pending", 0) + counts.get("ready", 0) + counts.get("running", 0)
        if active > 0:
            status = "active"
        elif counts.get("failed", 0) + counts.get("blocked", 0) == 0:
            status = "completed"
        elif counts.get("succeeded", 0) + counts.get("skipped", 0) == 0:
            status = "failed"
        else:
            status = "partial"
        batch = repository.batch_by_id(batch_id)
        if batch["status"] != status:
            repository.update_batch_status(batch_id, status, now)
            repository.add_event(
                batch_id=batch_id, node_id=None, event_type="batch.status", actor=actor,
                from_status=batch["status"], to_status=status, detail={"counts": counts}, now=now,
            )
        return status
