from __future__ import annotations

import argparse
import json
import os

from fastapi.testclient import TestClient

from app.database import database_path, get_connection, init_db
from app.main import app


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


def _admin_headers(client: TestClient) -> dict[str, str]:
    username = os.getenv("ORCHESTRATION_ADMIN_USERNAME", "admin")
    password = os.getenv("ORCHESTRATION_ADMIN_PASSWORD", "Admin!23456")
    credentials = {"username": username, "password": password, "client_label": "orchestration-cli"}
    login = client.post("/api/auth/login", json=credentials)
    if login.status_code != 200:
        client.post("/api/auth/bootstrap", json=credentials)
        login = client.post("/api/auth/login", json=credentials)
    if login.status_code != 200:
        raise SystemExit("无法登录管理员账号，请设置 ORCHESTRATION_ADMIN_USERNAME/ORCHESTRATION_ADMIN_PASSWORD")
    return {"Authorization": f"Bearer {login.json()['token']}"}


OBSERVATION_PIPELINE = {
    "batch_key": "observation-demo-000001",
    "name": "观测分析演示批次",
    "project_code": "orbit-observation",
    "requested_by": "cli-user",
    "resource_budget": 12,
    "max_parallel": 2,
    "nodes": [
        {"node_key": "calibrate", "stage": "calibration", "resource_units": 2, "priority": 90,
         "payload": {"channel": "visible"}},
        {"node_key": "infer", "stage": "inference", "depends_on": ["calibrate"], "resource_units": 4,
         "max_attempts": 1, "priority": 80, "payload": {"model": "cloud-v3"}},
        {"node_key": "compress", "stage": "compression", "depends_on": ["infer"], "resource_units": 1,
         "payload": {"codec": "zstd"}},
        {"node_key": "transmit", "stage": "transmission", "depends_on": ["compress"], "resource_units": 1,
         "payload": {"downlink": "ground-station-a"}},
        {"node_key": "health-check", "stage": "calibration", "depends_on": ["calibrate"], "resource_units": 1,
         "priority": 40, "payload": {"scope": "instrument"}},
    ],
}


def _complete_claimed(client: TestClient, worker_id: str, node: dict) -> dict:
    finished = client.post(
        f"/api/orchestration/nodes/{node['id']}/complete",
        json={"worker_id": worker_id, "result": {"node": node["node_key"], "ok": True}, "metrics": {"seconds": 1}},
    )
    return {"claimed": node["node_key"], "status": finished.json().get("status")}


def _claim_until(client: TestClient, worker_id: str, target_key: str, steps: list[dict], label: str) -> dict:
    """按就绪顺序领取并完成节点，直到领到目标节点为止。"""
    while True:
        claimed = client.post("/api/orchestration/nodes/claim", json={"worker_id": worker_id, "lease_seconds": 30})
        node = claimed.json().get("node")
        if node is None:
            raise SystemExit(f"没有可领取的节点，无法到达 {target_key}")
        if node["node_key"] == target_key:
            return node
        steps.append({"step": label, **_complete_claimed(client, worker_id, node)})


def command_orchestration_demo() -> int:
    """演示观测分析流水线：校准→推理→压缩→回传，推理遭遇辐射翻转后传播、恢复并复用已完成分支。"""
    with TestClient(app) as client:
        headers = _admin_headers(client)
        submitted = client.post("/api/orchestration/batches", json=OBSERVATION_PIPELINE)
        if submitted.status_code not in {202, 409}:
            print(submitted.text)
            return 1
        batch_id = submitted.json()["id"]
        steps: list[dict] = []
        infer = _claim_until(client, "worker-1", "infer", steps, "前置节点完成")
        failed = client.post(
            f"/api/orchestration/nodes/{infer['id']}/fail",
            json={"worker_id": "worker-1", "error_code": "radiation_bitflip",
                  "message": "单粒子翻转导致校验和不一致", "retryable": False},
        )
        steps.append({"step": "推理遭遇辐射翻转", "claimed": "infer", "status": failed.json().get("status")})
        blocked = client.get(f"/api/orchestration/batches/{batch_id}/summary").json()
        side_branch = client.post("/api/orchestration/nodes/claim", json={"worker_id": "worker-1", "lease_seconds": 30}).json()
        if side_branch.get("node"):
            steps.append({"step": "健康检查分支复用完成", **_complete_claimed(client, "worker-1", side_branch["node"])})
        recovered = client.post(
            f"/api/orchestration/batches/{batch_id}/retry",
            json={"reason": "辐射翻转后为失败节点重新排队，保留已完成分支"},
            headers=headers,
        )
        steps.append({"step": "批次恢复", "result": recovered.json()})
        while True:
            claimed = client.post("/api/orchestration/nodes/claim", json={"worker_id": "worker-1", "lease_seconds": 30}).json()
            if not claimed.get("node"):
                break
            steps.append({"step": "恢复后继续执行", **_complete_claimed(client, "worker-1", claimed["node"])})
        final = client.get(f"/api/orchestration/batches/{batch_id}/summary").json()
    result = {
        "batch_id": batch_id,
        "steps": steps,
        "blocked_after_failure": blocked["counts"],
        "final_status": final["batch"]["status"],
        "final_counts": final["counts"],
        "digest": final["digest"],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if final["batch"]["status"] == "completed" else 1


def command_orchestration_topology(batch_id: int) -> int:
    with TestClient(app) as client:
        response = client.get(f"/api/orchestration/batches/{batch_id}/topology")
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0 if response.status_code == 200 else 1


def command_orchestration_summary(batch_id: int) -> int:
    with TestClient(app) as client:
        response = client.get(f"/api/orchestration/batches/{batch_id}/summary")
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0 if response.status_code == 200 else 1


def command_orchestration_events(batch_id: int) -> int:
    with TestClient(app) as client:
        response = client.get(f"/api/orchestration/batches/{batch_id}/events")
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0 if response.status_code == 200 else 1


def command_orchestration_recover() -> int:
    with TestClient(app) as client:
        response = client.post("/api/orchestration/recovery/expired-leases")
    print(json.dumps(response.json(), ensure_ascii=False))
    return 0 if response.status_code == 200 else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="compute-operations", description="科学计算任务运营服务维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")
    subparsers.add_parser("compute-demo", help="执行计算任务提交与领取演示")
    subparsers.add_parser("orchestration-demo", help="演示观测分析 DAG 批次：失败传播、恢复与分支复用")
    topology = subparsers.add_parser("orchestration-topology", help="查看批次 DAG 拓扑与节点状态")
    topology.add_argument("--batch-id", type=int, required=True)
    summary = subparsers.add_parser("orchestration-summary", help="查看批次可追溯摘要")
    summary.add_argument("--batch-id", type=int, required=True)
    events = subparsers.add_parser("orchestration-events", help="查看批次状态变化事件")
    events.add_argument("--batch-id", type=int, required=True)
    subparsers.add_parser("orchestration-recover", help="恢复租约过期的编排节点")
    args = parser.parse_args()
    if args.command == "orchestration-topology":
        return command_orchestration_topology(args.batch_id)
    if args.command == "orchestration-summary":
        return command_orchestration_summary(args.batch_id)
    if args.command == "orchestration-events":
        return command_orchestration_events(args.batch_id)
    handlers = {
        "init-db": command_init,
        "check-db": command_check,
        "smoke": command_smoke,
        "compute-demo": command_compute_demo,
        "orchestration-demo": command_orchestration_demo,
        "orchestration-recover": command_orchestration_recover,
    }
    return handlers[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
