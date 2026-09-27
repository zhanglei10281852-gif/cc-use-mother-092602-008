from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from fastapi.testclient import TestClient

from app.database import close_connection, database_path, get_connection, init_db
from app.main import app
from app.orchestration.service import OrchestrationService
from app.orchestration.store import ensure_schema as ensure_orchestration_schema


def command_init() -> int:
    init_db()
    print(json.dumps({"database": str(database_path()), "status": "initialized"}, ensure_ascii=False))
    return 0


def command_check() -> int:
    init_db()
    connection = get_connection()
    result = {
        "database": str(database_path()),
        "integrity": connection.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "tables": connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["integrity"] == "ok" and result["foreign_keys"] == 1 else 1


def command_smoke() -> int:
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    result = {"root": root.json(), "health": health.json(), "status_codes": [root.status_code, health.status_code]}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status_codes"] == [200, 200] else 1


def command_compute_demo() -> int:
    template = {
        "code": "monte-carlo-demo",
        "name": "蒙特卡洛演示",
        "algorithm": "monte-carlo",
        "parameter_schema": {
            "samples": {"type": "integer", "required": True, "minimum": 10, "maximum": 1000000},
            "seed": {"type": "integer", "required": True},
        },
        "default_parameters": {},
        "max_runtime_seconds": 60,
        "max_attempts": 3,
    }
    with TestClient(app) as client:
        created = client.post("/api/compute/templates?actor=cli-demo", json=template)
        if created.status_code not in {201, 409}:
            print(created.text)
            return 1
        task = client.post(
            "/api/compute/tasks",
            json={
                "template_code": "monte-carlo-demo",
                "project_code": "demo",
                "requested_by": "cli-user",
                "parameters": {"samples": 1000, "seed": 42},
                "priority": 80,
                "idempotency_key": "compute-demo-000001",
            },
        )
        claimed = client.post(
            "/api/compute/tasks/claim",
            json={"worker_id": "cli-worker", "capabilities": ["monte-carlo"], "lease_seconds": 60},
        )
    result = {"task": task.status_code, "claimed": claimed.status_code, "task_id": task.json().get("id")}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if task.status_code == 202 and claimed.status_code == 200 and claimed.json().get("task") else 1


def command_orch_topology(batch_id: int) -> int:
    init_db()
    ensure_orchestration_schema(get_connection())
    view = OrchestrationService().topology_view(batch_id)
    print(json.dumps(view, ensure_ascii=False, indent=2))
    return 0


def command_orch_status(batch_id: int) -> int:
    init_db()
    ensure_orchestration_schema(get_connection())
    service = OrchestrationService()
    batch = service.get_batch(batch_id)
    timeline = [
        {"seq": event["id"], "type": event["event_type"], "node": event["node_id"],
         "actor": event["actor"], "from": event["from_status"], "to": event["to_status"],
         "reason": event["reason"], "at": event["created_at"]}
        for event in batch["events"]
    ]
    print(json.dumps({
        "batch_id": batch_id,
        "status": batch["status"],
        "nodes": [
            {"node_key": n["node_key"], "stage": n["stage"], "status": n["status"],
             "attempt": n["attempt_count"], "lease_owner": n["lease_owner"],
             "error": n["last_error_code"], "skip_reason": n["skip_reason"]}
            for n in batch["nodes"]
        ],
        "timeline": timeline,
    }, ensure_ascii=False, indent=2))
    return 0


def command_orch_summary(batch_id: int) -> int:
    init_db()
    ensure_orchestration_schema(get_connection())
    batch = OrchestrationService().get_batch(batch_id)
    print(json.dumps({
        "batch_id": batch_id,
        "status": batch["status"],
        "summary_generated_at": batch["summary_generated_at"],
        "summary": batch["summary"],
    }, ensure_ascii=False, indent=2))
    return 0


def command_orch_recover() -> int:
    init_db()
    ensure_orchestration_schema(get_connection())
    print(json.dumps(OrchestrationService().recover_expired_leases(), ensure_ascii=False))
    return 0


def command_orch_demo() -> int:
    """端到端演示：校准失败后，推理/压缩/回传分支不消耗算力，其余分支照常完成。"""
    demo_db = Path(__file__).resolve().parent.parent / "data" / "orch-demo.db"
    demo_db.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = demo_db.with_name(demo_db.name + suffix)
        if path.exists():
            path.unlink()
    close_connection()
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(demo_db)
    init_db()
    ensure_orchestration_schema(get_connection())
    service = OrchestrationService()
    batch = service.submit_batch({
        "name": "观测分析-演示批次",
        "submitted_by": "cli-demo",
        "batch_key": "orch-demo-000001",
        "priority": 70,
        "resource_limits": {"radiometry-gpu": 1},
        "nodes": [
            {"key": "calibration", "stage": "calibration", "payload": {"idempotency_key": "node-demo-calibration"},
             "resources": {"radiometry-gpu": 1}, "max_attempts": 1},
            {"key": "inference", "stage": "inference", "depends_on": ["calibration"],
             "payload": {"idempotency_key": "node-demo-inference"}, "resources": {"radiometry-gpu": 1}},
            {"key": "compression", "stage": "compression", "depends_on": ["inference"],
             "payload": {"idempotency_key": "node-demo-compression"}},
            {"key": "downlink", "stage": "downlink", "depends_on": ["compression"],
             "payload": {"idempotency_key": "node-demo-downlink"}},
            {"key": "housekeeping", "stage": "telemetry", "payload": {"idempotency_key": "node-demo-housekeeping"}},
        ],
    })
    batch_id = batch["id"]

    def claim_one(worker: str):
        result = service.claim(worker, lease_seconds=60, max_nodes=1)
        return result["claimed"][0] if result["claimed"] else None

    # housekeeping 与 calibration 无依赖，按就绪顺序可领取；让两个工作者分别跑完
    first = claim_one("worker-a")
    second = claim_one("worker-b")
    leaves = {node["node_key"]: node for node in (first, second) if node}
    assert set(leaves) == {"calibration", "housekeeping"}, sorted(leaves)
    hk, cal = leaves["housekeeping"], leaves["calibration"]
    service.complete(hk["id"], hk["lease_owner"], hk["lease_token"],
                     result={"frames": 128}, metrics={"duration_s": 1.2})
    # 校准因辐射翻转失败（max_attempts=1），失败应沿主干传播
    service.fail(cal["id"], cal["lease_owner"], cal["lease_token"],
                 error_code="radiometric_flip", message="辐射翻转失败", retryable=False)

    # 此时再领取不应得到 inference/compression/downlink
    assert service.claim("worker-c", 60, max_nodes=10)["claimed"] == []

    final = service.get_batch(batch_id)
    statuses = {n["node_key"]: n["status"] for n in final["nodes"]}
    print(json.dumps({
        "batch_id": batch_id,
        "status": final["status"],
        "node_status": statuses,
        "root_causes": final["summary"]["root_causes"],
        "completed_branches": [c["node_key"] for c in final["summary"]["completed_branches"]],
    }, ensure_ascii=False, indent=2))
    expected = {"calibration": "failed", "inference": "skipped", "compression": "skipped",
                "downlink": "skipped", "housekeeping": "succeeded"}
    return 0 if statuses == expected and final["status"] == "partial" else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="compute-operations", description="科学计算任务运营服务维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")
    subparsers.add_parser("compute-demo", help="执行计算任务提交与领取演示")
    topology_parser = subparsers.add_parser("orch-topology", help="查看编排批次的 DAG 拓扑")
    topology_parser.add_argument("batch_id", type=int)
    status_parser = subparsers.add_parser("orch-status", help="查看编排批次状态变化时间线")
    status_parser.add_argument("batch_id", type=int)
    summary_parser = subparsers.add_parser("orch-summary", help="查看可追溯批次摘要")
    summary_parser.add_argument("batch_id", type=int)
    subparsers.add_parser("orch-recover", help="恢复租约过期的编排节点")
    subparsers.add_parser("orch-demo", help="执行 DAG 编排端到端演示")
    args = parser.parse_args()
    commands = {
        "init-db": command_init, "check-db": command_check, "smoke": command_smoke,
        "compute-demo": command_compute_demo,
        "orch-topology": lambda: command_orch_topology(args.batch_id),
        "orch-status": lambda: command_orch_status(args.batch_id),
        "orch-summary": lambda: command_orch_summary(args.batch_id),
        "orch-recover": command_orch_recover,
        "orch-demo": command_orch_demo,
    }
    return commands[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
