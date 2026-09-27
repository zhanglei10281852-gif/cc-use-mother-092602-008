from __future__ import annotations

import json
import sqlite3
from typing import Any


class OrchestrationRepository:
    """封装 DAG 批次编排领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # 批次

    def batch_by_id(self, batch_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM orchestration_batches WHERE id=?", (batch_id,)).fetchone()

    def batch_by_key(self, requested_by: str, batch_key: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM orchestration_batches WHERE requested_by=? AND batch_key=?",
            (requested_by, batch_key),
        ).fetchone()

    def list_batches(self, *, status: str | None, project_code: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("status=?")
            values.append(status)
        if project_code:
            clauses.append("project_code=?")
            values.append(project_code)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT * FROM orchestration_batches" + where + " ORDER BY id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]

    def create_batch(self, *, batch_key: str, batch_digest: str, name: str, project_code: str, requested_by: str,
                     resource_budget: int, reserved_units: int, max_parallel: int, node_count: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO orchestration_batches(batch_key,batch_digest,name,project_code,requested_by,resource_budget,reserved_units,max_parallel,node_count,status,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,'active',?,?)",
            (batch_key, batch_digest, name, project_code, requested_by, resource_budget, reserved_units, max_parallel, node_count, now, now),
        )
        return dict(self.batch_by_id(cursor.lastrowid))

    def update_batch_status(self, batch_id: int, status: str, now: str) -> None:
        self.connection.execute(
            "UPDATE orchestration_batches SET status=?,updated_at=? WHERE id=?",
            (status, now, batch_id),
        )

    # 节点

    def node_by_id(self, node_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM orchestration_nodes WHERE id=?", (node_id,)).fetchone()

    def nodes_of_batch(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM orchestration_nodes WHERE batch_id=? ORDER BY id",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def create_node(self, *, batch_id: int, node_key: str, stage: str, payload: dict[str, Any], resource_units: int,
                    max_attempts: int, priority: int, status: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO orchestration_nodes(batch_id,node_key,stage,payload_json,resource_units,max_attempts,priority,status,available_at,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, node_key, stage, json.dumps(payload, ensure_ascii=False, sort_keys=True), resource_units, max_attempts, priority, status, now, now, now),
        )
        return dict(self.node_by_id(cursor.lastrowid))

    def add_edge(self, *, batch_id: int, node_id: int, depends_on_id: int) -> None:
        self.connection.execute(
            "INSERT INTO orchestration_edges(batch_id,node_id,depends_on_id) VALUES(?,?,?)",
            (batch_id, node_id, depends_on_id),
        )

    def edges_of_batch(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM orchestration_edges WHERE batch_id=? ORDER BY node_id,depends_on_id",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def dependencies(self, node_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT n.* FROM orchestration_edges e JOIN orchestration_nodes n ON n.id=e.depends_on_id WHERE e.node_id=? ORDER BY n.id",
            (node_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def downstream_nodes(self, node_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT n.* FROM orchestration_edges e JOIN orchestration_nodes n ON n.id=e.node_id WHERE e.depends_on_id=? ORDER BY n.id",
            (node_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def unsatisfied_dependency_count(self, node_id: int) -> int:
        return int(self.connection.execute(
            "SELECT COUNT(*) FROM orchestration_edges e JOIN orchestration_nodes n ON n.id=e.depends_on_id"
            " WHERE e.node_id=? AND n.status NOT IN ('succeeded','skipped')",
            (node_id,),
        ).fetchone()[0])

    def ready_candidates(self, *, batch_id: int | None, now: str, limit: int = 20) -> list[sqlite3.Row]:
        clauses = ["n.status='ready'", "n.available_at<=?", "b.status='active'"]
        values: list[Any] = [now]
        if batch_id is not None:
            clauses.append("n.batch_id=?")
            values.append(batch_id)
        values.append(limit)
        return self.connection.execute(
            "SELECT n.* FROM orchestration_nodes n JOIN orchestration_batches b ON b.id=n.batch_id"
            " WHERE " + " AND ".join(clauses) + " ORDER BY n.priority DESC,n.available_at ASC,n.id ASC LIMIT ?",
            values,
        ).fetchall()

    def running_count(self, batch_id: int) -> int:
        return int(self.connection.execute(
            "SELECT COUNT(*) FROM orchestration_nodes WHERE batch_id=? AND status='running'",
            (batch_id,),
        ).fetchone()[0])

    def status_counts(self, batch_id: int) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT status,COUNT(*) AS amount FROM orchestration_nodes WHERE batch_id=? GROUP BY status",
            (batch_id,),
        ).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def expired_running(self, now: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM orchestration_nodes WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
            (now,),
        ).fetchall()
        return [dict(row) for row in rows]

    # 事件

    def add_event(self, *, batch_id: int, node_id: int | None, event_type: str, actor: str,
                  from_status: str = "", to_status: str = "", detail: dict[str, Any] | None = None, now: str) -> None:
        self.connection.execute(
            "INSERT INTO orchestration_events(batch_id,node_id,event_type,actor,from_status,to_status,detail_json,created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (batch_id, node_id, event_type, actor, from_status, to_status, json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), now),
        )

    def events_of_batch(self, batch_id: int, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT e.*,n.node_key FROM orchestration_events e LEFT JOIN orchestration_nodes n ON n.id=e.node_id"
            " WHERE e.batch_id=? ORDER BY e.id LIMIT ?",
            (batch_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]
