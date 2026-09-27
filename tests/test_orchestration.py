from __future__ import annotations

import json
from datetime import UTC, datetime

from app.core.clock import FrozenClock
from app.database import get_connection, init_db
from app.orchestration.service import OrchestrationService


def node(node_key: str, stage: str, **overrides) -> dict:
    spec = {
        "node_key": node_key,
        "stage": stage,
        "depends_on": [],
        "resource_units": 1,
        "max_attempts": 3,
        "priority": 50,
        "payload": {},
    }
    spec.update(overrides)
    return spec


def pipeline_payload(batch_key: str = "obs-000001", **overrides) -> dict:
    payload = {
        "batch_key": batch_key,
        "name": "观测分析批次",
        "project_code": "orbit",
        "requested_by": "researcher-1",
        "resource_budget": 12,
        "max_parallel": 2,
        "nodes": [
            node("calibrate", "calibration", resource_units=2, priority=90),
            node("infer", "inference", depends_on=["calibrate"], resource_units=4, max_attempts=1, priority=80),
            node("compress", "compression", depends_on=["infer"]),
            node("transmit", "transmission", depends_on=["compress"]),
            node("health-check", "calibration", depends_on=["calibrate"], priority=40),
        ],
    }
    payload.update(overrides)
    return payload


def submit(client, batch_key: str = "obs-000001", **overrides) -> dict:
    response = client.post("/api/orchestration/batches", json=pipeline_payload(batch_key, **overrides))
    assert response.status_code == 202, response.text
    return response.json()


def claim_one(client, worker: str = "w1") -> dict | None:
    response = client.post("/api/orchestration/nodes/claim", json={"worker_id": worker, "lease_seconds": 60})
    assert response.status_code == 200, response.text
    return response.json()["node"]


def complete(client, node_row: dict, worker: str = "w1") -> dict:
    response = client.post(
        f"/api/orchestration/nodes/{node_row['id']}/complete",
        json={"worker_id": worker, "result": {"ok": True}, "metrics": {"seconds": 1}},
    )
    assert response.status_code == 200, response.text
    return response.json()


def fail(client, node_row: dict, worker: str = "w1", retryable: bool = False) -> dict:
    response = client.post(
        f"/api/orchestration/nodes/{node_row['id']}/fail",
        json={"worker_id": worker, "error_code": "radiation_bitflip", "message": "单粒子翻转导致校验失败", "retryable": retryable},
    )
    assert response.status_code == 200, response.text
    return response.json()


def states_of(client, batch_id: int) -> dict[str, str]:
    detail = client.get(f"/api/orchestration/batches/{batch_id}").json()
    return {item["node_key"]: item["status"] for item in detail["nodes"]}


def test_graph_validation_rejects_invalid_batches(client):
    cycle = pipeline_payload("cycle-000001", nodes=[
        node("step-a", "calibration", depends_on=["step-b"]),
        node("step-b", "inference", depends_on=["step-a"]),
    ])
    response = client.post("/api/orchestration/batches", json=cycle)
    assert response.status_code == 422
    assert "循环" in response.json()["error"]["message"]

    unknown = pipeline_payload("unknown-00001", nodes=[node("step-a", "calibration", depends_on=["ghost"])])
    assert client.post("/api/orchestration/batches", json=unknown).status_code == 422

    duplicated = pipeline_payload("dup-0000001", nodes=[
        node("step-a", "calibration"), node("step-a", "inference"),
    ])
    assert client.post("/api/orchestration/batches", json=duplicated).status_code == 422

    self_dependent = pipeline_payload("self-0000001", nodes=[node("step-a", "calibration", depends_on=["step-a"])])
    assert client.post("/api/orchestration/batches", json=self_dependent).status_code == 422

    over_budget = pipeline_payload("budget-000001", resource_budget=4)
    response = client.post("/api/orchestration/batches", json=over_budget)
    assert response.status_code == 422
    assert response.json()["error"]["context"]["required"] == 9

    node_over_budget = pipeline_payload(
        "budget-000002",
        nodes=[node("heavy", "inference", resource_units=5), node("light", "calibration")],
        resource_budget=4,
    )
    assert client.post("/api/orchestration/batches", json=node_over_budget).status_code == 422


def test_batch_and_node_idempotency_scopes(client):
    first = submit(client)
    again = client.post("/api/orchestration/batches", json=pipeline_payload())
    assert again.status_code == 202
    assert again.json()["id"] == first["id"]
    assert len(again.json()["nodes"]) == 5

    changed = pipeline_payload()
    changed["nodes"][0]["resource_units"] = 3
    conflict = client.post("/api/orchestration/batches", json=changed)
    assert conflict.status_code == 409

    reused_node_keys = client.post("/api/orchestration/batches", json=pipeline_payload("obs-000002"))
    assert reused_node_keys.status_code == 202
    assert reused_node_keys.json()["id"] != first["id"]

    key_namespaces = pipeline_payload("calibrate")
    assert client.post("/api/orchestration/batches", json=key_namespaces).status_code == 202


def test_ready_order_failure_propagation_and_partial_summary(client):
    batch = submit(client)
    assert states_of(client, batch["id"]) == {
        "calibrate": "ready", "infer": "pending", "compress": "pending",
        "transmit": "pending", "health-check": "pending",
    }

    first = claim_one(client)
    assert first["node_key"] == "calibrate"
    assert claim_one(client, "w2") is None
    complete(client, first)

    second = claim_one(client)
    assert second["node_key"] == "infer"
    fail(client, second, retryable=False)

    states = states_of(client, batch["id"])
    assert states["infer"] == "failed"
    assert states["compress"] == "blocked"
    assert states["transmit"] == "blocked"

    third = claim_one(client)
    assert third["node_key"] == "health-check"
    complete(client, third)
    assert claim_one(client, "w2") is None

    summary = client.get(f"/api/orchestration/batches/{batch['id']}/summary").json()
    assert summary["batch"]["status"] == "partial"
    assert summary["counts"] == {"blocked": 2, "failed": 1, "succeeded": 2}
    assert summary["stages"]["calibration"]["succeeded"] == 2
    assert summary["resource"]["reserved_units"] == 9
    assert summary["digest"]
    event_types = [event["event_type"] for event in summary["events"]]
    assert "node.blocked" in event_types
    assert "batch.status" in event_types
    blocked_event = next(event for event in summary["events"] if event["event_type"] == "node.blocked")
    assert json.loads(blocked_event["detail_json"])["cause_node"] == "infer"


def test_max_parallel_limits_running_nodes(client):
    batch = submit(client, "parallel-0001", max_parallel=1, nodes=[
        node("root-a", "calibration"), node("root-b", "calibration"),
    ])
    first = claim_one(client)
    assert first is not None
    assert claim_one(client, "w2") is None
    complete(client, first)
    assert claim_one(client, "w2") is not None
    assert client.get(f"/api/orchestration/batches/{batch['id']}").status_code == 200


def test_batch_retry_reuses_succeeded_nodes(client, admin):
    batch = submit(client)
    complete(client, claim_one(client))
    fail(client, claim_one(client), retryable=False)
    complete(client, claim_one(client))
    assert states_of(client, batch["id"])["infer"] == "failed"

    denied = client.post(f"/api/orchestration/batches/{batch['id']}/retry", json={"reason": "未授权尝试"})
    assert denied.status_code == 401

    retry = client.post(
        f"/api/orchestration/batches/{batch['id']}/retry",
        json={"reason": "辐射翻转恢复，重算失败分支"},
        headers=admin["headers"],
    )
    assert retry.status_code == 200, retry.text
    result = retry.json()
    assert result["reset"] == ["infer"]
    assert sorted(result["reopened"]) == ["compress", "transmit"]
    assert sorted(result["kept_succeeded"]) == ["calibrate", "health-check"]

    detail = client.get(f"/api/orchestration/batches/{batch['id']}").json()
    nodes = {item["node_key"]: item for item in detail["nodes"]}
    assert nodes["calibrate"]["status"] == "succeeded"
    assert nodes["calibrate"]["attempt_count"] == 1
    assert nodes["health-check"]["status"] == "succeeded"
    assert nodes["infer"]["status"] == "ready"
    assert nodes["infer"]["attempt_count"] == 0
    assert nodes["compress"]["status"] == "pending"

    for expected in ("infer", "compress", "transmit"):
        claimed = claim_one(client)
        assert claimed["node_key"] == expected
        complete(client, claimed)
    summary = client.get(f"/api/orchestration/batches/{batch['id']}/summary").json()
    assert summary["batch"]["status"] == "completed"
    assert summary["counts"] == {"succeeded": 5}
    assert summary["nodes"][1]["attempt_count"] == 1


def test_manual_skip_requires_reason_and_permission(client, admin):
    batch = submit(client)
    complete(client, claim_one(client))
    detail = client.get(f"/api/orchestration/batches/{batch['id']}").json()
    infer = next(item for item in detail["nodes"] if item["node_key"] == "infer")

    assert client.post(f"/api/orchestration/nodes/{infer['id']}/skip", json={"reason": "未登录"}).status_code == 401
    assert client.post(
        f"/api/orchestration/nodes/{infer['id']}/skip", json={}, headers=admin["headers"]
    ).status_code == 422

    role = client.post("/api/roles", json={"code": "observer", "name": "观察者", "permission_codes": []}, headers=admin["headers"])
    assert role.status_code == 201, role.text
    user = client.post(
        "/api/users",
        json={"username": "observer-1", "password": "Observer!234", "display_name": "观察者", "role_codes": ["observer"]},
        headers=admin["headers"],
    )
    assert user.status_code == 201, user.text
    login = client.post("/api/auth/login", json={"username": "observer-1", "password": "Observer!234", "client_label": "tests"})
    limited = {"Authorization": f"Bearer {login.json()['token']}"}
    denied = client.post(f"/api/orchestration/nodes/{infer['id']}/skip", json={"reason": "权限外尝试"}, headers=limited)
    assert denied.status_code == 403

    skipped = client.post(
        f"/api/orchestration/nodes/{infer['id']}/skip",
        json={"reason": "辐射损坏无法重算，人工放行下游"},
        headers=admin["headers"],
    )
    assert skipped.status_code == 200, skipped.text
    assert skipped.json()["status"] == "skipped"
    assert skipped.json()["skip_reason"] == "辐射损坏无法重算，人工放行下游"
    assert skipped.json()["skipped_by"] == "admin"

    promoted = claim_one(client)
    assert promoted["node_key"] == "compress"
    conflict = client.post(
        f"/api/orchestration/nodes/{promoted['id']}/skip",
        json={"reason": "运行中不允许跳过"},
        headers=admin["headers"],
    )
    assert conflict.status_code == 409

    events = client.get(f"/api/orchestration/batches/{batch['id']}/events").json()["items"]
    skipped_events = [event for event in events if event["event_type"] == "node.skipped"]
    assert len(skipped_events) == 1
    assert json.loads(skipped_events[0]["detail_json"])["reason"] == "辐射损坏无法重算，人工放行下游"


def test_topology_levels_and_missing_batch(client):
    batch = submit(client)
    topology = client.get(f"/api/orchestration/batches/{batch['id']}/topology").json()
    levels = {item["node_key"]: item["level"] for item in topology["nodes"]}
    assert levels == {"calibrate": 0, "infer": 1, "health-check": 1, "compress": 2, "transmit": 3}
    depends = {item["node_key"]: item["depends_on"] for item in topology["nodes"]}
    assert depends["transmit"] == ["compress"]
    assert {"from": "calibrate", "to": "infer"} in topology["edges"]
    assert client.get("/api/orchestration/batches/9999").status_code == 404
    assert client.get("/api/orchestration/batches/9999/summary").status_code == 404


def test_retry_backoff_and_lease_recovery(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 27, 2, 0, tzinfo=UTC))
    service = OrchestrationService(get_connection(), clock)
    batch = service.submit({
        "batch_key": "svc-000001",
        "name": "服务级批次",
        "project_code": "orbit",
        "requested_by": "researcher-1",
        "resource_budget": 10,
        "max_parallel": 2,
        "nodes": [
            node("step-a", "calibration", max_attempts=2),
            node("step-b", "inference", depends_on=["step-a"], max_attempts=2),
            node("step-c", "compression", depends_on=["step-b"]),
        ],
    })

    first = service.claim("worker-a", None, 10)
    assert first["node_key"] == "step-a"
    service.complete(first["id"], "worker-a", {"ok": True}, {})

    second = service.claim("worker-a", None, 10)
    assert second["node_key"] == "step-b"
    requeued = service.fail(second["id"], "worker-a", "radiation_bitflip", "单粒子翻转", True)
    assert requeued["status"] == "ready"
    assert requeued["available_at"] > requeued["updated_at"]
    assert service.claim("worker-a", None, 10) is None

    clock.advance(seconds=2)
    again = service.claim("worker-a", None, 10)
    assert again["node_key"] == "step-b"
    assert again["attempt_count"] == 2

    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["recovered"] == []
    assert recovered["exhausted"] == [second["id"]]

    detail = service.get_batch(batch["id"])
    states = {item["node_key"]: item["status"] for item in detail["nodes"]}
    assert states == {"step-a": "succeeded", "step-b": "failed", "step-c": "blocked"}
    assert detail["status"] == "partial"
    summary = service.summary(batch["id"])
    assert summary["counts"] == {"blocked": 1, "failed": 1, "succeeded": 1}
    assert summary["digest"]
