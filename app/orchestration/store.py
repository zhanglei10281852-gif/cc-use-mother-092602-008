"""编排子系统的 SQLite 表结构与仓储查询。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

SCHEMA = r'''
CREATE TABLE IF NOT EXISTS orch_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_key TEXT NOT NULL,
    name TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','cancelling','succeeded','partial','failed','cancelled')),
    resource_limits_json TEXT NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 50,
    version INTEGER NOT NULL DEFAULT 1,
    summary_json TEXT,
    summary_generated_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(submitted_by, batch_key)
);

CREATE TABLE IF NOT EXISTS orch_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES orch_batches(id) ON DELETE CASCADE,
    node_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    stage TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    depends_on_json TEXT NOT NULL,
    resources_json TEXT NOT NULL DEFAULT '{}',
    payload_json TEXT NOT NULL DEFAULT '{}',
    priority INTEGER NOT NULL DEFAULT 50,
    status TEXT NOT NULL DEFAULT 'blocked' CHECK(status IN ('blocked','ready','running','succeeded','failed','skipped','cancelled')),
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_token TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    available_at TEXT NOT NULL,
    result_json TEXT,
    result_digest TEXT NOT NULL DEFAULT '',
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_message TEXT NOT NULL DEFAULT '',
    skipped_by TEXT NOT NULL DEFAULT '',
    skip_reason TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    UNIQUE(batch_id, node_key)
);
CREATE INDEX IF NOT EXISTS idx_orch_nodes_ready ON orch_nodes(status, available_at, priority DESC, ordinal);
CREATE INDEX IF NOT EXISTS idx_orch_nodes_batch ON orch_nodes(batch_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_orch_nodes_lease ON orch_nodes(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS orch_node_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL REFERENCES orch_nodes(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('claimed','succeeded','failed','recovered')),
    error_code TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    result_digest TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    ended_at TEXT,
    UNIQUE(node_id, attempt)
);

CREATE TABLE IF NOT EXISTS orch_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES orch_batches(id) ON DELETE CASCADE,
    node_id INTEGER REFERENCES orch_nodes(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    from_status TEXT NOT NULL DEFAULT '',
    to_status TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orch_events_batch ON orch_events(batch_id, id);
'''

BATCH_COLUMNS = (
    "id,batch_key,name,submitted_by,status,resource_limits_json,priority,version,"
    "summary_generated_at,created_at,updated_at,started_at,finished_at"
)
NODE_COLUMNS = (
    "id,batch_id,node_key,idempotency_key,stage,ordinal,depends_on_json,resources_json,"
    "payload_json,priority,status,max_attempts,attempt_count,lease_owner,"
    "lease_expires_at,available_at,result_json,result_digest,last_error_code,"
    "last_error_message,skipped_by,skip_reason,version,started_at,finished_at,"
    "created_at,updated_at"
)


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)


def _hydrate_node(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    for field in ("depends_on", "resources", "payload"):
        column = f"{field}_json"
        data[field] = json.loads(data.pop(column))
    data["result"] = json.loads(data.pop("result_json")) if data.get("result_json") is not None else None
    return data


def _hydrate_batch(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["resource_limits"] = json.loads(data.pop("resource_limits_json"))
    data["summary"] = json.loads(data.pop("summary_json")) if data.get("summary_json") is not None else None
    return data


class OrchestrationRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # ---- 批次 ----------------------------------------------------------
    def batch_by_key(self, submitted_by: str, batch_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            f"SELECT {BATCH_COLUMNS},summary_json FROM orch_batches WHERE submitted_by=? AND batch_key=?",
            (submitted_by, batch_key),
        ).fetchone()
        return _hydrate_batch(row) if row else None

    def batch_by_id(self, batch_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            f"SELECT {BATCH_COLUMNS},summary_json FROM orch_batches WHERE id=?", (batch_id,)
        ).fetchone()
        return _hydrate_batch(row) if row else None

    def insert_batch(self, *, batch_key: str, name: str, submitted_by: str, request_digest: str,
                     resource_limits: dict[str, int], priority: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO orch_batches(batch_key,name,submitted_by,request_digest,status,"
            "resource_limits_json,priority,created_at,updated_at) VALUES(?,?,?,?, 'pending',?,?,?,?)",
            (batch_key, name, submitted_by, request_digest,
             json.dumps(resource_limits, ensure_ascii=False, sort_keys=True), priority, now, now),
        )
        batch = self.batch_by_id(cursor.lastrowid)
        assert batch is not None
        return batch

    def touch_batch(self, batch_id: int, status: str, *, now: str,
                    started: bool = False, finished: str | None = None) -> None:
        self.connection.execute(
            "UPDATE orch_batches SET status=?,updated_at=?,version=version+1,"
            "started_at=COALESCE(started_at,?),finished_at=? WHERE id=?",
            (status, now, now if started else None, finished, batch_id),
        )

    def save_summary(self, batch_id: int, summary: dict[str, Any], now: str) -> None:
        self.connection.execute(
            "UPDATE orch_batches SET summary_json=?,summary_generated_at=?,updated_at=?,version=version+1 WHERE id=?",
            (json.dumps(summary, ensure_ascii=False, sort_keys=True), now, now, batch_id),
        )

    def list_batches(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = f"SELECT {BATCH_COLUMNS},summary_json FROM orch_batches"
        params: tuple[Any, ...] = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC LIMIT ?"
        return [_hydrate_batch(row) for row in self.connection.execute(sql, (*params, limit)).fetchall()]

    # ---- 节点 ----------------------------------------------------------
    def insert_node(self, *, batch_id: int, node_key: str, idempotency_key: str, stage: str,
                    ordinal: int, depends_on: list[str], resources: dict[str, int],
                    payload: dict[str, Any], priority: int, max_attempts: int,
                    status: str, available_at: str, now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO orch_nodes(batch_id,node_key,idempotency_key,stage,ordinal,"
            "depends_on_json,resources_json,payload_json,priority,status,max_attempts,"
            "available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, node_key, idempotency_key, stage, ordinal,
             json.dumps(depends_on, ensure_ascii=False),
             json.dumps(resources, ensure_ascii=False, sort_keys=True),
             json.dumps(payload, ensure_ascii=False, sort_keys=True),
             priority, status, max_attempts, available_at, now, now),
        )
        return int(cursor.lastrowid)

    def node_by_id(self, node_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(f"SELECT {NODE_COLUMNS} FROM orch_nodes WHERE id=?", (node_id,)).fetchone()
        return _hydrate_node(row) if row else None

    def node_by_idempotency_key(self, idempotency_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            f"SELECT {NODE_COLUMNS} FROM orch_nodes WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        return _hydrate_node(row) if row else None

    def nodes_of_batch(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            f"SELECT {NODE_COLUMNS} FROM orch_nodes WHERE batch_id=? ORDER BY ordinal", (batch_id,)
        ).fetchall()
        return [_hydrate_node(row) for row in rows]

    def ready_candidates(self, now: str, stages: tuple[str, ...] | None) -> list[dict[str, Any]]:
        sql = (
            f"SELECT {NODE_COLUMNS} FROM orch_nodes WHERE status='ready' AND available_at<=?"
        )
        params: list[Any] = [now]
        if stages is not None:
            placeholder = ",".join("?" for _ in stages)
            sql += f" AND stage IN ({placeholder})"
            params.extend(stages)
        sql += " ORDER BY priority DESC, ordinal"
        return [_hydrate_node(row) for row in self.connection.execute(sql, params).fetchall()]

    def running_nodes(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            f"SELECT {NODE_COLUMNS} FROM orch_nodes WHERE status='running'"
        ).fetchall()
        return [_hydrate_node(row) for row in rows]

    def expired_running_nodes(self, now: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            f"SELECT {NODE_COLUMNS} FROM orch_nodes WHERE status='running' "
            "AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
            (now,),
        ).fetchall()
        return [_hydrate_node(row) for row in rows]

    # ---- 尝试台账与事件 --------------------------------------------------
    def add_attempt(self, *, node_id: int, attempt: int, worker_id: str, lease_token: str,
                    outcome: str, started_at: str, ended_at: str | None = None,
                    error_code: str = "", message: str = "", result_digest: str = "") -> None:
        self.connection.execute(
            "INSERT INTO orch_node_attempts(node_id,attempt,worker_id,lease_token,outcome,"
            "error_code,message,result_digest,started_at,ended_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (node_id, attempt, worker_id, lease_token, outcome, error_code, message[:2000],
             result_digest, started_at, ended_at),
        )

    def close_attempt(self, *, node_id: int, attempt: int, outcome: str, ended_at: str,
                      error_code: str = "", message: str = "", result_digest: str = "") -> None:
        self.connection.execute(
            "UPDATE orch_node_attempts SET outcome=?,error_code=?,message=?,result_digest=?,ended_at=? "
            "WHERE node_id=? AND attempt=?",
            (outcome, error_code, message[:2000], result_digest, ended_at, node_id, attempt),
        )

    def attempts_of_node(self, node_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT id,node_id,attempt,worker_id,outcome,error_code,message,result_digest,"
            "started_at,ended_at FROM orch_node_attempts WHERE node_id=? ORDER BY attempt",
            (node_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def add_event(self, *, batch_id: int, node_id: int | None, event_type: str, actor: str,
                  reason: str = "", from_status: str = "", to_status: str = "",
                  detail: dict[str, Any] | None = None, now: str) -> None:
        self.connection.execute(
            "INSERT INTO orch_events(batch_id,node_id,event_type,actor,reason,from_status,"
            "to_status,detail_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (batch_id, node_id, event_type, actor, reason, from_status, to_status,
             json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), now),
        )

    def events_of_batch(self, batch_id: int, *, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT id,batch_id,node_id,event_type,actor,reason,from_status,to_status,"
            "detail_json,created_at FROM orch_events WHERE batch_id=? ORDER BY id LIMIT ?",
            (batch_id, limit),
        ).fetchall()
        events = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json"))
            events.append(item)
        return events
