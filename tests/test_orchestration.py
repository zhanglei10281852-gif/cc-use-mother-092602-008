"""观测分析 DAG 编排子系统测试。"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import pytest

from app.core.clock import FrozenClock
from app.core.errors import ConflictError, PermissionDeniedError, ValidationError
from app.core.security import Principal
from app.database import close_connection, init_db
from app.orchestration.service import OrchestrationService


ADMIN = Principal(user_id=1, username="admin", display_name="管理员",
                  department_id=None, permissions=frozenset({"*"}), session_id=1)
NOBODY = Principal(user_id=2, username="nobody", display_name="无权限",
                   department_id=None, permissions=frozenset(), session_id=2)


@pytest.fixture()
def orch(tmp_path):
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "orch.db")
    close_connection()
    init_db()
    from app.database import transaction
    from app.services.auth import AuthService
    with transaction(immediate=True) as connection:
        AuthService(connection).bootstrap_admin("admin", "Admin!234567", "管理员")
    clock = FrozenClock(datetime(2026, 9, 27, 8, 0, tzinfo=UTC))
    service = OrchestrationService(clock=clock)
    yield service, clock
    close_connection()


def pipeline(**overrides) -> dict:
    payload = {
        "name": "观测分析批次",
        "submitted_by": "scheduler",
        "batch_key": "obs-batch-000001",
        "priority": 50,
        "resource_limits": {"gpu": 1},
        "nodes": [
            {"key": "calibration", "stage": "calibration", "resources": {"gpu": 1},
             "max_attempts": 2, "payload": {"idempotency_key": "node-calibration"}},
            {"key": "inference", "stage": "inference", "depends_on": ["calibration"],
             "resources": {"gpu": 1}, "payload": {"idempotency_key": "node-inference"}},
            {"key": "compression", "stage": "compression", "depends_on": ["inference"],
             "payload": {"idempotency_key": "node-compression"}},
            {"key": "downlink", "stage": "downlink", "depends_on": ["compression"],
             "payload": {"idempotency_key": "node-downlink"}},
        ],
    }
    payload.update(overrides)
    return payload


def status_map(batch: dict) -> dict[str, str]:
    return {n["node_key"]: n["status"] for n in batch["nodes"]}


def claim_all(service, worker="worker-1", max_nodes=10, lease_seconds=60, **kwargs):
    return service.claim(worker, lease_seconds=lease_seconds, max_nodes=max_nodes, **kwargs)["claimed"]


def complete_chain_after_calibration(service, batch_id):
    """领取并依次完成 inference -> compression -> downlink。"""
    for expected, worker in (("inference", "w-i"), ("compression", "w-c"), ("downlink", "w-d")):
        claimed = claim_all(service, worker)
        assert [n["node_key"] for n in claimed] == [expected]
        node = claimed[0]
        service.complete(node["id"], worker, node["lease_token"],
                         result={"ok": True}, metrics={"seconds": 1})


# ---------------------------------------------------------------------------
# 拓扑校验
# ---------------------------------------------------------------------------
def test_submit_sets_initial_states_and_validates(orch):
    service, _ = orch
    batch = service.submit_batch(pipeline())
    assert batch["status"] == "pending"
    assert status_map(batch) == {
        "calibration": "ready", "inference": "blocked",
        "compression": "blocked", "downlink": "blocked",
    }


def test_cycle_is_rejected(orch):
    service, _ = orch
    cyclic = pipeline(nodes=[
        {"key": "a", "stage": "s", "payload": {"idempotency_key": "k-a"}, "depends_on": ["c"]},
        {"key": "b", "stage": "s", "payload": {"idempotency_key": "k-b"}, "depends_on": ["a"]},
        {"key": "c", "stage": "s", "payload": {"idempotency_key": "k-c"}, "depends_on": ["b"]},
    ])
    with pytest.raises(ValidationError) as exc:
        service.submit_batch(cyclic)
    assert exc.value.code == "cycle_detected"


def test_missing_dependency_duplicate_key_and_self_dependency_are_rejected(orch):
    service, _ = orch
    missing = pipeline(nodes=[
        {"key": "a", "stage": "s", "depends_on": ["ghost"], "payload": {"idempotency_key": "k-a"}},
    ])
    with pytest.raises(ValidationError):
        service.submit_batch(missing)

    duplicate = pipeline(nodes=[
        {"key": "a", "stage": "s", "payload": {"idempotency_key": "k-a"}},
        {"key": "a", "stage": "s", "payload": {"idempotency_key": "k-b"}},
    ])
    with pytest.raises(ValidationError):
        service.submit_batch(duplicate)

    self_dep = pipeline(batch_key="obs-batch-000002", nodes=[
        {"key": "a", "stage": "s", "depends_on": ["a"], "payload": {"idempotency_key": "k-c"}},
    ])
    with pytest.raises(ValidationError):
        service.submit_batch(self_dep)


def test_node_demand_over_capacity_is_rejected(orch):
    service, _ = orch
    oversized = pipeline(resource_limits={"gpu": 1}, nodes=[
        {"key": "a", "stage": "s", "resources": {"gpu": 2}, "payload": {"idempotency_key": "k-a"}},
    ])
    with pytest.raises(ValidationError) as exc:
        service.submit_batch(oversized)
    assert exc.value.context["resource"] == "gpu"


def test_node_idempotency_key_is_required_and_separate_from_batch_key(orch):
    service, _ = orch
    payload = pipeline(nodes=[{"key": "a", "stage": "s", "payload": {}}])
    with pytest.raises(ValidationError):
        service.submit_batch(payload)


# ---------------------------------------------------------------------------
# 幂等
# ---------------------------------------------------------------------------
def test_batch_idempotency_replays_same_request(orch):
    service, _ = orch
    first = service.submit_batch(pipeline())
    replay = service.submit_batch(pipeline())
    assert replay["id"] == first["id"]
    assert len(replay["nodes"]) == 4


def test_batch_idempotency_conflicts_on_different_dag(orch):
    service, _ = orch
    service.submit_batch(pipeline())
    changed = pipeline(nodes=[
        {"key": "calibration", "stage": "calibration",
         "payload": {"idempotency_key": "node-calibration"}},
    ])
    with pytest.raises(ConflictError):
        service.submit_batch(changed)


def test_node_idempotency_key_cannot_be_reused_across_batches(orch):
    service, _ = orch
    service.submit_batch(pipeline())
    other = pipeline(batch_key="obs-batch-000002", nodes=[
        {"key": "other", "stage": "s", "payload": {"idempotency_key": "node-calibration"}},
    ])
    with pytest.raises(ConflictError) as exc:
        service.submit_batch(other)
    assert exc.value.context["idempotency_key"] == "node-calibration"


# ---------------------------------------------------------------------------
# 调度与资源约束
# ---------------------------------------------------------------------------
def test_claim_respects_ready_order_and_blocks_descendants(orch):
    service, _ = orch
    service.submit_batch(pipeline())
    claimed = claim_all(service)
    assert [n["node_key"] for n in claimed] == ["calibration"]
    assert claimed[0]["attempt_count"] == 1
    assert claimed[0]["lease_token"]


def test_priority_drives_ready_order(orch):
    service, _ = orch
    service.submit_batch(pipeline(batch_key="obs-batch-000002", resource_limits={}, nodes=[
        {"key": "low", "stage": "s", "priority": 10, "payload": {"idempotency_key": "n-low"}},
        {"key": "high", "stage": "s", "priority": 90, "payload": {"idempotency_key": "n-high"}},
    ]))
    claimed = claim_all(service)
    assert [n["node_key"] for n in claimed] == ["high", "low"]


def test_resource_capacity_serializes_nodes_within_batch(orch):
    service, _ = orch
    service.submit_batch(pipeline(batch_key="obs-batch-000002", resource_limits={"gpu": 1}, nodes=[
        {"key": "r1", "stage": "s", "resources": {"gpu": 1}, "payload": {"idempotency_key": "n-r1"}},
        {"key": "r2", "stage": "s", "resources": {"gpu": 1}, "payload": {"idempotency_key": "n-r2"}},
    ]))
    first = claim_all(service)
    assert [n["node_key"] for n in first] == ["r1"]
    # 容量被 r1 占用，r2 无法领取
    assert claim_all(service, "worker-2") == []
    service.complete(first[0]["id"], "worker-1", first[0]["lease_token"], result={}, metrics={})
    second = claim_all(service, "worker-2")
    assert [n["node_key"] for n in second] == ["r2"]


def test_resource_limits_are_scoped_per_batch(orch):
    service, _ = orch
    for index in (1, 2):
        service.submit_batch(pipeline(
            batch_key=f"obs-batch-00000{index}", resource_limits={"gpu": 1},
            nodes=[{"key": "root", "stage": "s", "resources": {"gpu": 1},
                    "payload": {"idempotency_key": f"n-root-{index}"}}],
        ))
    claimed = claim_all(service, max_nodes=10)
    # 两个批次各有 1 份 gpu 容量，互不限流
    assert len(claimed) == 2


def test_stage_filter_only_claims_matching_stages(orch):
    service, _ = orch
    service.submit_batch(pipeline(batch_key="obs-batch-000002", resource_limits={}, nodes=[
        {"key": "a", "stage": "calibration", "payload": {"idempotency_key": "n-a"}},
        {"key": "b", "stage": "telemetry", "payload": {"idempotency_key": "n-b"}},
    ]))
    claimed = claim_all(service, stages=["telemetry"])
    assert [n["node_key"] for n in claimed] == ["b"]


# ---------------------------------------------------------------------------
# 失败传播：核心业务规则
# ---------------------------------------------------------------------------
def test_hard_failure_skips_downstream_but_keeps_sibling_branch(orch):
    service, _ = orch
    with_sibling = pipeline(nodes=pipeline()["nodes"] + [
        {"key": "housekeeping", "stage": "telemetry",
         "payload": {"idempotency_key": "node-housekeeping"}},
    ])
    service.submit_batch(with_sibling)
    claimed = {n["node_key"]: n for n in claim_all(service, max_nodes=10)}
    assert set(claimed) == {"calibration", "housekeeping"}

    hk = claimed["housekeeping"]
    service.complete(hk["id"], hk["lease_owner"], hk["lease_token"],
                     result={"frames": 128}, metrics={})
    cal = claimed["calibration"]
    result = service.fail(cal["id"], cal["lease_owner"], cal["lease_token"],
                          error_code="radiometric_flip", message="辐射翻转失败", retryable=False)

    assert status_map(result) == {
        "calibration": "failed", "inference": "skipped",
        "compression": "skipped", "downlink": "skipped",
        "housekeeping": "succeeded",
    }
    assert result["status"] == "partial"
    # 被跳过的后继不能再被领取，后续阶段不消耗算力
    assert claim_all(service, "late-worker") == []

    inference = next(n for n in result["nodes"] if n["node_key"] == "inference")
    assert inference["skipped_by"] == "system:propagator"
    assert "calibration" in inference["skip_reason"]


def test_retryable_failure_uses_backoff_then_runs_again(orch):
    service, clock = orch
    service.submit_batch(pipeline())
    cal = claim_all(service)[0]
    service.fail(cal["id"], "worker-1", cal["lease_token"],
                 error_code="transient", message="瞬时错误", retryable=True)

    # 退避窗口内不可领取
    assert claim_all(service, "worker-2") == []
    clock.advance(seconds=2)
    cal2 = claim_all(service, "worker-2")[0]
    assert cal2["node_key"] == "calibration"
    assert cal2["attempt_count"] == 2
    service.complete(cal2["id"], "worker-2", cal2["lease_token"], result={}, metrics={})
    assert [n["node_key"] for n in claim_all(service, "worker-3")] == ["inference"]


def test_exhausted_attempts_propagate_failure(orch):
    service, clock = orch
    service.submit_batch(pipeline())
    for attempt in (1, 2):
        node = claim_all(service, f"w{attempt}")[0]
        assert node["attempt_count"] == attempt
        service.fail(node["id"], f"w{attempt}", node["lease_token"],
                     error_code="flip", message="再次翻转失败", retryable=True)
        clock.advance(seconds=300)
    batch = service.get_batch(1)
    assert status_map(batch)["calibration"] == "failed"
    assert status_map(batch)["inference"] == "skipped"
    assert batch["status"] == "failed"


def test_lease_owner_and_token_are_validated(orch):
    service, _ = orch
    service.submit_batch(pipeline())
    cal = claim_all(service)[0]
    with pytest.raises(PermissionDeniedError):
        service.complete(cal["id"], "impostor", cal["lease_token"], result={}, metrics={})
    with pytest.raises(PermissionDeniedError):
        service.complete(cal["id"], "worker-1", "wrong-token", result={}, metrics={})


def test_heartbeat_extends_lease(orch):
    service, clock = orch
    service.submit_batch(pipeline())
    cal = claim_all(service)[0]
    renewed = service.heartbeat(cal["id"], "worker-1", cal["lease_token"], lease_seconds=120)
    assert renewed["lease_expires_at"] > cal["lease_expires_at"]


# ---------------------------------------------------------------------------
# 租约恢复
# ---------------------------------------------------------------------------
def test_recovery_requeues_unexpired_budget_and_fails_exhausted(orch):
    service, clock = orch
    # 第一次：仍有重试预算，过期后恢复并重新排队
    service.submit_batch(pipeline())
    cal = claim_all(service)[0]
    clock.advance(seconds=120)
    result = service.recover_expired_leases()
    assert result["recovered"] == [cal["id"]]
    assert claim_all(service) == []  # 退避中
    clock.advance(seconds=2)
    assert [n["node_key"] for n in claim_all(service)] == ["calibration"]

    # 第二次（attempt_count=2 = max_attempts）：耗尽 -> 失败并传播
    clock.advance(seconds=120)
    result = service.recover_expired_leases()
    assert result["exhausted"] == [cal["id"]]
    batch = service.get_batch(cal["batch_id"])
    assert status_map(batch)["inference"] == "skipped"
    assert batch["status"] == "failed"


def test_recovery_during_cancel_does_not_requeue(orch):
    service, clock = orch
    batch = service.submit_batch(pipeline())
    cal = claim_all(service, lease_seconds=10)[0]
    service.cancel_batch(batch["id"], ADMIN, "窗口作废")
    clock.advance(seconds=30)
    # 即使仍有重试预算，取消中的批次也不再重新排队
    result = service.recover_expired_leases()
    assert result["exhausted"] == [cal["id"]]
    final = service.get_batch(batch["id"])
    assert final["status"] == "cancelled"
    assert claim_all(service) == []


# ---------------------------------------------------------------------------
# 人工跳过 / 重试 / 取消
# ---------------------------------------------------------------------------
def test_manual_skip_requires_reason_permission_and_unblocks_descendants(orch):
    service, _ = orch
    batch = service.submit_batch(pipeline())
    inference_id = next(n["id"] for n in batch["nodes"] if n["node_key"] == "inference")

    with pytest.raises(PermissionDeniedError):
        service.manual_skip(inference_id, NOBODY, "该数据不需要推理")
    with pytest.raises(ValidationError):
        service.manual_skip(inference_id, ADMIN, " ")

    result = service.manual_skip(inference_id, ADMIN, "该数据不需要推理")
    inference = next(n for n in result["nodes"] if n["node_key"] == "inference")
    assert inference["status"] == "skipped"
    assert inference["skipped_by"] == "admin"
    # 人工跳过视为完成，后继继续
    assert status_map(result)["compression"] == "ready"

    # 已进入终态的节点不能重复跳过
    with pytest.raises(ConflictError):
        service.manual_skip(inference_id, ADMIN, "再次跳过")


def test_manual_retry_revives_propagated_branch_without_rerunning_success(orch):
    service, _ = orch
    payload = pipeline(nodes=pipeline()["nodes"] + [
        {"key": "housekeeping", "stage": "telemetry",
         "payload": {"idempotency_key": "node-housekeeping"}},
    ])
    # 校准仅一次尝试，直接终态失败
    payload["nodes"][0]["max_attempts"] = 1
    service.submit_batch(payload)
    claimed = {n["node_key"]: n for n in claim_all(service, max_nodes=10)}
    hk = claimed["housekeeping"]
    service.complete(hk["id"], hk["lease_owner"], hk["lease_token"], result={}, metrics={})
    cal = claimed["calibration"]
    service.fail(cal["id"], cal["lease_owner"], cal["lease_token"],
                 error_code="radiometric_flip", message="辐射翻转失败", retryable=False)

    with pytest.raises(ConflictError):
        service.manual_retry_node(hk["id"], ADMIN, "成功节点不应被重试")

    result = service.manual_retry_node(cal["id"], ADMIN, "已更换辐射定标源，重试校准")
    assert status_map(result)["calibration"] == "ready"
    revived = {n["node_key"]: n["status"] for n in result["nodes"]}
    assert revived["inference"] == "blocked"
    # 成功分支保持成功，不会被重新执行
    housekeeping = next(n for n in result["nodes"] if n["node_key"] == "housekeeping")
    assert housekeeping["status"] == "succeeded"
    assert housekeeping["attempt_count"] == 1

    # 完整恢复：重新跑通校准后，被复活的下游依次执行
    cal2 = claim_all(service, "recovery-w")[0]
    service.complete(cal2["id"], "recovery-w", cal2["lease_token"], result={"gain": 1.0}, metrics={})
    complete_chain_after_calibration(service, cal["batch_id"])
    final = service.get_batch(cal["batch_id"])
    assert final["status"] == "succeeded"
    housekeeping = next(n for n in final["nodes"] if n["node_key"] == "housekeeping")
    assert housekeeping["attempt_count"] == 1
    calibration = next(n for n in final["nodes"] if n["node_key"] == "calibration")
    assert calibration["attempt_count"] == 2


def test_cancel_batch_marks_unstarted_nodes_and_converges(orch):
    service, _ = orch
    batch = service.submit_batch(pipeline())
    cal = claim_all(service)[0]
    result = service.cancel_batch(batch["id"], ADMIN, "观测窗口作废")
    assert status_map(result) == {
        "calibration": "running", "inference": "cancelled",
        "compression": "cancelled", "downlink": "cancelled",
    }
    assert result["status"] == "cancelling"
    # 取消中的批次，运行节点报告可重试失败也不会重新排队
    result = service.fail(cal["id"], "worker-1", cal["lease_token"],
                          error_code="abort", message="随批次取消", retryable=True)
    assert result["status"] == "cancelled"
    assert claim_all(service) == []


def test_cancel_with_completed_branch_converges_to_partial(orch):
    service, _ = orch
    payload = pipeline(nodes=pipeline()["nodes"] + [
        {"key": "housekeeping", "stage": "telemetry",
         "payload": {"idempotency_key": "node-housekeeping"}},
    ])
    service.submit_batch(payload)
    claimed = {n["node_key"]: n for n in claim_all(service, max_nodes=10)}
    hk = claimed["housekeeping"]
    service.complete(hk["id"], hk["lease_owner"], hk["lease_token"], result={}, metrics={})
    service.cancel_batch(1, ADMIN, "窗口作废")
    cal = claimed["calibration"]
    result = service.complete(cal["id"], cal["lease_owner"], cal["lease_token"],
                              result={}, metrics={})
    # 校准成功但下游已在取消时终止：已有成功分支 -> partial
    assert result["status"] == "partial"


# ---------------------------------------------------------------------------
# 批次摘要与事件追溯
# ---------------------------------------------------------------------------
def test_summary_is_generated_and_traceable(orch):
    service, _ = orch
    payload = pipeline(nodes=pipeline()["nodes"] + [
        {"key": "housekeeping", "stage": "telemetry",
         "payload": {"idempotency_key": "node-housekeeping"}},
    ])
    payload["nodes"][0]["max_attempts"] = 1
    service.submit_batch(payload)
    claimed = {n["node_key"]: n for n in claim_all(service, max_nodes=10)}
    hk = claimed["housekeeping"]
    service.complete(hk["id"], hk["lease_owner"], hk["lease_token"],
                     result={"frames": 10}, metrics={"seconds": 0.5})
    cal = claimed["calibration"]
    service.fail(cal["id"], cal["lease_owner"], cal["lease_token"],
                 error_code="radiometric_flip", message="辐射翻转失败", retryable=False)

    batch = service.get_batch(1)
    summary = batch["summary"]
    assert summary["batch_status"] == "partial"
    assert summary["state_counts"] == {
        "failed": 1, "skipped": 3, "succeeded": 1,
    }
    root_cause = summary["root_causes"][0]
    assert root_cause["node_key"] == "calibration"
    assert root_cause["error_code"] == "radiometric_flip"
    assert root_cause["skipped_descendants"] == ["compression", "downlink", "inference"]
    # 已完成的独立分支仍在摘要中可追溯
    completed_keys = {item["node_key"] for item in summary["completed_branches"]}
    assert completed_keys == {"housekeeping"}
    assert len(summary["node_trace"]) == 5
    assert summary["summary_digest"]

    event_types = {event["event_type"] for event in batch["events"]}
    assert {"batch_submitted", "node_failed", "node_skip_propagated", "node_succeeded"} <= event_types
    node = service.get_node(cal["id"])
    assert [a["outcome"] for a in node["attempts"]] == ["failed"]
    assert node["attempts"][0]["error_code"] == "radiometric_flip"


def test_full_success_batch_status_and_summary(orch):
    service, _ = orch
    service.submit_batch(pipeline())
    cal = claim_all(service)[0]
    service.complete(cal["id"], "worker-1", cal["lease_token"], result={}, metrics={})
    complete_chain_after_calibration(service, 1)
    batch = service.get_batch(1)
    assert batch["status"] == "succeeded"
    assert set(batch["summary"]["state_counts"]) == {"succeeded"}
    assert batch["summary"]["root_causes"] == []


def test_topology_view_lists_edges(orch):
    service, _ = orch
    service.submit_batch(pipeline())
    view = service.topology_view(1)
    edges = {(edge["from"], edge["to"]) for edge in view["edges"]}
    assert edges == {
        ("calibration", "inference"),
        ("inference", "compression"),
        ("compression", "downlink"),
    }
    assert view["resource_limits"] == {"gpu": 1}


# ---------------------------------------------------------------------------
# HTTP 接口
# ---------------------------------------------------------------------------
def test_api_end_to_end_propagation_and_permission(client, admin):
    submit = client.post("/api/orchestration/batches", json=pipeline())
    assert submit.status_code == 202, submit.text
    batch_id = submit.json()["id"]

    claimed = client.post("/api/orchestration/claim",
                          json={"worker_id": "w1", "lease_seconds": 60, "max_nodes": 10})
    assert claimed.status_code == 200
    node = claimed.json()["claimed"][0]
    failed = client.post(f"/api/orchestration/nodes/{node['id']}/fail", json={
        "worker_id": "w1", "lease_token": node["lease_token"],
        "error_code": "radiometric_flip", "message": "辐射翻转失败", "retryable": False,
    })
    assert failed.status_code == 200
    assert failed.json()["status"] == "failed"

    summary = client.get(f"/api/orchestration/batches/{batch_id}/summary")
    assert summary.status_code == 200
    assert summary.json()["summary"]["root_causes"][0]["node_key"] == "calibration"

    topology = client.get(f"/api/orchestration/batches/{batch_id}/topology")
    assert topology.status_code == 200
    assert any(edge == {"from": "inference", "to": "compression"} for edge in topology.json()["edges"])


def test_api_manual_skip_requires_session_and_permission(client, admin):
    submitted = client.post("/api/orchestration/batches", json=pipeline())
    nodes = submitted.json()["nodes"]
    inference_id = next(n["id"] for n in nodes if n["node_key"] == "inference")

    # 无会话令牌
    response = client.post(f"/api/orchestration/nodes/{inference_id}/skip",
                           json={"reason": "无需推理"})
    assert response.status_code == 401

    # 管理员可跳过并记录理由
    response = client.post(f"/api/orchestration/nodes/{inference_id}/skip",
                           headers=admin["headers"], json={"reason": "无需推理"})
    assert response.status_code == 200, response.text
    skipped = next(n for n in response.json()["nodes"] if n["id"] == inference_id)
    assert skipped["status"] == "skipped"
    assert skipped["skip_reason"] == "无需推理"

    # 人工跳过进入平台级审计日志，理由可追溯
    audit = client.get("/api/audit?resource_type=orchestration", headers=admin["headers"])
    assert audit.status_code == 200, audit.text
    records = audit.json()["data"]
    skip_record = next(r for r in records if r["action"] == "orchestration.node.skip")
    assert skip_record["actor_name"] == "admin"
    assert json.loads(skip_record["after_json"])["reason"] == "无需推理"

    # 无权限用户被拒绝
    role = client.post("/api/roles", headers=admin["headers"],
                       json={"code": "plain.reader", "name": "普通角色", "permission_codes": []})
    assert role.status_code == 201, role.text
    client.post("/api/users", headers=admin["headers"],
                json={"username": "plain.user", "password": "Plain!234567",
                      "display_name": "普通用户", "role_codes": ["plain.reader"]})
    login = client.post("/api/auth/login",
                        json={"username": "plain.user", "password": "Plain!234567",
                              "client_label": "tests"})
    headers = {"Authorization": f"Bearer {login.json()['token']}"}
    denied = client.post(
        f"/api/orchestration/nodes/{next(n['id'] for n in nodes if n['node_key'] == 'compression')}/skip",
        headers=headers, json={"reason": "试图跳过"},
    )
    assert denied.status_code == 403


def test_api_validation_rejects_cycle(client, admin):
    response = client.post("/api/orchestration/batches", json=pipeline(nodes=[
        {"key": "a", "stage": "s", "depends_on": ["b"], "payload": {"idempotency_key": "k-a"}},
        {"key": "b", "stage": "s", "depends_on": ["a"], "payload": {"idempotency_key": "k-b"}},
    ]))
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "cycle_detected"
