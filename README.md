# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。
- DAG 依赖编排（`app/orchestration`）：提交带依赖 DAG 的观测分析批次（校准→推理→压缩→回传），提交时校验循环、缺失依赖和资源可行性；工作者按就绪顺序领取节点并受批次资源容量约束；上游硬失败会把未启动的后继标记为系统传播跳过，不继续消耗算力，已完成分支保留并可复用；支持退避重试、租约过期恢复、受权限控制的人工跳过/重试/取消（必须填写理由），并在每次状态收敛时生成带结果摘要、根因和事件时间线的可追溯批次摘要。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复，以及 DAG 编排的循环/资源校验、两级幂等、就绪领取与资源容量、失败传播与分支复用、退避重试、租约恢复、带权限理由的人工跳过/重试/取消、批次摘要追溯和身份与既有模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。

## DAG 依赖编排

观测分析批次以 DAG 描述节点（如校准、推理、压缩、回传）及其依赖，接口前缀为 `/api/orchestration`：

- `POST /batches`：提交批次。批次键 `batch_key`（按提交人幂等）与每个节点 `payload.idempotency_key`（全局唯一）相互独立；同键重放返回同一批次，节点键复用会被拒绝，成功节点不会因重试而重复执行。
- `GET /batches/{id}/topology`、`GET /batches/{id}`、`GET /batches/{id}/summary`：查看拓扑边、全部节点状态、事件时间线和批次摘要。
- `POST /claim`：工作者按 `优先级降序、提交顺序` 领取就绪节点，支持 `stages` 能力过滤和 `max_nodes` 批量领取；返回的 `lease_token` 用于续租与回执。
- `POST /nodes/{id}/heartbeat|complete|fail`：续租、成功回执（保存结果摘要）、失败回执（可重试错误按指数退避重新排队，次数耗尽转为终态失败）。
- `POST /nodes/{id}/skip`、`POST /nodes/{id}/retry`：人工跳过（视为该步有意省略，后继继续）与人工重试（仅复活被系统传播跳过的下游，成功节点不重跑）；均要求会话令牌、对应权限和非空理由，并写入审计日志。
- `POST /batches/{id}/cancel`：取消批次，未启动节点立即终止，运行中节点收敛后批次转为 `cancelled` 或 `partial`。
- `POST /recovery/expired-leases`：恢复租约过期节点，仍有预算则退避重排，否则失败并传播。

节点状态为 `blocked → ready → running → succeeded/failed/skipped/cancelled`；批次状态为 `pending → running/cancelling → succeeded/partial/failed/cancelled`。上游 `failed/cancelled` 会级联地把未启动后继标记为系统传播 `skipped`，因此失败分支不再领取执行，而已完成的独立分支保留在摘要的 `completed_branches` 中，根因与其传播到的节点列在 `root_causes` 中。

命令行检查（配合 `TOWNSHIP_DATABASE_PATH` 指定数据库）：

```bash
python -m app.cli orch-demo                  # 端到端演示：校准辐射翻转失败后的传播与部分完成
python -m app.cli orch-topology <batch_id>   # 查看 DAG 节点、边和资源容量
python -m app.cli orch-status <batch_id>     # 查看节点状态与事件时间线
python -m app.cli orch-summary <batch_id>    # 查看可追溯批次摘要
python -m app.cli orch-recover               # 执行租约恢复
```

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  orchestration/   观测分析 DAG 批次编排：拓扑校验、调度、传播、租约、幂等和摘要
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
