"""DAG 拓扑与资源约束校验。

校验在批次提交时完成，包括：节点标识、依赖只指向同批次节点、无环、
资源声明合法、以及批次级资源约束（任一时刻同时运行节点对单一资源的
需求之和不得超过批次容量）。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Iterable

from app.core.errors import ValidationError

# 节点状态机：
# blocked -> ready -> running -> succeeded
#                  \-> failed / skipped / cancelled
# ready/blocked 还可进入 skipped（人工跳过或失败传播）；running 可被取消。
TERMINAL_STATES = frozenset({"succeeded", "failed", "skipped", "cancelled"})
DONE_STATES = frozenset({"succeeded", "skipped"})  # 后继可以继续的“完成”状态
BLOCKING_TERMINAL_STATES = frozenset({"failed", "cancelled"})

# 每个节点可重试次数上限，防止异常输入造成无限重试
MAX_NODE_ATTEMPTS = 20
# 失败传播时每个节点标记跳过的最大数量，避免病态批次导致栈/内存问题
MAX_BATCH_SIZE = 500


class TopologyValidationError(ValidationError):
    code = "topology_invalid"


class CycleDetectedError(ValidationError):
    code = "cycle_detected"


@dataclass(frozen=True, slots=True)
class NodeSpec:
    key: str
    stage: str
    dependencies: frozenset[str]
    resources: dict[str, int]
    max_attempts: int
    priority: int
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ValidatedBatch:
    nodes: dict[str, NodeSpec]
    resource_limits: dict[str, int]
    children: dict[str, list[str]]
    index_by_key: dict[str, int]

    @property
    def resource_count(self) -> int:
        return len(self.resource_limits)

    def topological_order(self) -> list[str]:
        """返回拓扑序（同层按提交索引排序，保证调度顺序稳定）。"""
        return _topological_sort(self.nodes, self.children, self.index_by_key)


def validate_batch(
    nodes: list[dict[str, Any]],
    resource_limits: dict[str, int] | None = None,
) -> ValidatedBatch:
    """校验原始批次输入并返回规范化拓扑。

    资源约束语义：节点声明它运行时独占的资源量；同一资源在同一时刻
    处于 running 的节点需求之和不得超过批次容量。未声明容量的资源
    视为无限，但仍会校验单节点需求必须为非负整数。
    """
    if not nodes:
        raise TopologyValidationError("批次至少需要一个节点")
    if len(nodes) > MAX_BATCH_SIZE:
        raise TopologyValidationError(f"批次节点数不能超过 {MAX_BATCH_SIZE}")

    specs: dict[str, NodeSpec] = {}
    index_by_key: dict[str, int] = {}
    for index, raw in enumerate(nodes):
        key = _require_text(raw, "key")
        if key in specs:
            raise TopologyValidationError("节点 key 在批次内重复", context={"key": key})
        stage = _require_text(raw, "stage")
        depends_on = raw.get("depends_on") or []
        if not isinstance(depends_on, list) or not all(isinstance(item, str) and item.strip() for item in depends_on):
            raise TopologyValidationError("节点依赖列表不合法", context={"key": key})
        dependencies = frozenset(item.strip() for item in depends_on)
        if key in dependencies:
            raise TopologyValidationError("节点不能依赖自身", context={"key": key})
        resources = _validate_resources(raw.get("resources") or {}, key)
        max_attempts = int(raw.get("max_attempts", 3))
        if not 1 <= max_attempts <= MAX_NODE_ATTEMPTS:
            raise TopologyValidationError(
                f"节点最大尝试次数必须在 1 到 {MAX_NODE_ATTEMPTS} 之间", context={"key": key}
            )
        priority = int(raw.get("priority", 50))
        if not 0 <= priority <= 100:
            raise TopologyValidationError("节点优先级必须在 0 到 100 之间", context={"key": key})
        payload = raw.get("payload") or {}
        if not isinstance(payload, dict):
            raise TopologyValidationError("节点 payload 必须是对象", context={"key": key})
        specs[key] = NodeSpec(
            key=key, stage=stage, dependencies=dependencies, resources=resources,
            max_attempts=max_attempts, priority=priority, payload=payload,
        )
        index_by_key[key] = index

    missing = {dep for spec in specs.values() for dep in spec.dependencies if dep not in specs}
    if missing:
        raise TopologyValidationError(
            "依赖指向了批次内不存在的节点", context={"dependencies": sorted(missing)}
        )

    children: dict[str, list[str]] = {}
    for spec in specs.values():
        for dep in spec.dependencies:
            children.setdefault(dep, []).append(spec.key)

    order = _topological_sort(specs, children, index_by_key)
    if len(order) != len(specs):
        unresolved = sorted(set(specs) - set(order))
        raise CycleDetectedError("依赖图中存在循环", context={"nodes": unresolved})

    limits = _validate_limits(resource_limits or {}, specs)
    _check_resource_feasibility(specs.values(), limits)

    return ValidatedBatch(
        nodes=specs,
        resource_limits=limits,
        children=children,
        index_by_key=index_by_key,
    )


def _topological_sort(
    specs: dict[str, NodeSpec],
    children: dict[str, list[str]],
    index_by_key: dict[str, int],
) -> list[str]:
    indegree = {key: len(spec.dependencies) for key, spec in specs.items()}
    ready = deque(sorted((key for key, degree in indegree.items() if degree == 0), key=index_by_key.get))
    order: list[str] = []
    while ready:
        key = ready.popleft()
        order.append(key)
        for child in sorted(children.get(key, ()), key=index_by_key.get):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
    return order


def _require_text(raw: dict[str, Any], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value.strip():
        raise TopologyValidationError(f"节点缺少合法字段：{field}")
    return value.strip()


def _validate_resources(resources: Any, key: str) -> dict[str, int]:
    if not isinstance(resources, dict):
        raise TopologyValidationError("节点资源声明必须是对象", context={"key": key})
    normalized: dict[str, int] = {}
    for name, amount in resources.items():
        if not isinstance(name, str) or not name.strip():
            raise TopologyValidationError("资源名称不合法", context={"key": key})
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise TopologyValidationError("资源需求量必须是非负整数",
                                          context={"resource": name, "node_key": key})
        normalized[name.strip()] = amount
    return dict(sorted(normalized.items()))


def _validate_limits(raw: Any, specs: dict[str, NodeSpec]) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise TopologyValidationError("批次资源容量必须是对象")
    limits: dict[str, int] = {}
    for name, amount in raw.items():
        if not isinstance(name, str) or not name.strip():
            raise TopologyValidationError("资源名称不合法")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            raise TopologyValidationError("资源容量必须是正整数", context={"resource": name})
        limits[name.strip()] = amount
    # 节点使用但批次未声明容量的资源：补一个仅用于响应中的无限标记不入库，
    # 实际是否受限以声明容量为准；此处不报错以允许“只记账不限量”的资源。
    return dict(sorted(limits.items()))


def _check_resource_feasibility(specs: Iterable[NodeSpec], limits: dict[str, int]) -> None:
    """单节点需求超过容量的批次永远无法调度，提前拒绝。"""
    for spec in specs:
        for name, amount in spec.resources.items():
            if name in limits and amount > limits[name]:
                raise TopologyValidationError(
                    "节点资源需求超过批次容量，批次永远无法调度",
                    context={"key": spec.key, "resource": name, "demand": amount, "capacity": limits[name]},
                )
