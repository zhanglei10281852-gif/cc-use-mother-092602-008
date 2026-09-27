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
- 依赖编排：以 DAG 形式提交观测分析批次（校准、推理、压缩、回传等阶段），提交时校验循环依赖、节点幂等键重复、未知依赖和算力资源预算。
- 就绪调度：节点按依赖就绪顺序与优先级被工作者领取，批次级并发上限限制同时运行的节点数，避免浪费宝贵算力。
- 失败传播：节点彻底失败（如辐射翻转）后，下游节点自动转为受阻状态不再被领取；已完成分支保持成功结果可被复用。
- 批次恢复：批次级重试只重置失败与受阻节点，绝不重复执行已成功节点；租约过期节点可自动恢复或转为失败并继续传播。
- 人工跳过：跳过节点必须填写理由并持有 `orchestration.skip` 权限，跳过后下游节点按依赖满足处理。
- 可追溯摘要：批次摘要汇总各状态计数、阶段分布、资源占用和完整状态变化事件，并附内容摘要指纹；批次与节点使用相互独立的幂等键。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

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

观测分析编排接口统一使用 `/api/orchestration` 前缀：提交 DAG 批次（`POST /batches`）、查看拓扑（`GET /batches/{id}/topology`）、可追溯摘要（`GET /batches/{id}/summary`）、状态变化事件（`GET /batches/{id}/events`）、领取与回执节点（`POST /nodes/claim`、`POST /nodes/{id}/complete`、`POST /nodes/{id}/fail`）、人工跳过（`POST /nodes/{id}/skip`，需登录并持有 `orchestration.skip` 权限）、批次恢复（`POST /batches/{id}/retry`，需 `orchestration.retry` 权限）以及租约恢复（`POST /recovery/expired-leases`）。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复，并保留身份与既有科学计算模块的回归用例。编排测试覆盖循环与资源约束校验、批次与节点双重幂等、就绪领取顺序、失败传播阻断、批次恢复复用成功节点、人工跳过的理由与权限控制以及可追溯摘要。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
python -m app.cli orchestration-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。`orchestration-demo` 提交一个“校准→推理→压缩→回传”的观测分析 DAG 批次，模拟推理节点遭遇辐射翻转失败：下游压缩与回传被阻断不再消耗算力，健康检查分支正常完成；随后以管理员权限恢复批次，只重算失败节点并复用已完成分支，最终输出可追溯摘要指纹。演示需要管理员会话，首次运行会自动初始化 `admin` 账号，也可用 `ORCHESTRATION_ADMIN_USERNAME`/`ORCHESTRATION_ADMIN_PASSWORD` 指定已有账号。

批次运行期间或结束后，可用以下命令检查拓扑、状态变化和恢复结果：

```bash
python -m app.cli orchestration-topology --batch-id 1   # DAG 层级与节点状态
python -m app.cli orchestration-summary --batch-id 1    # 可追溯批次摘要
python -m app.cli orchestration-events --batch-id 1     # 状态变化事件流
python -m app.cli orchestration-recover                 # 恢复租约过期节点
```

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  orchestration/   DAG 批次编排：校验、调度、失败传播、跳过与恢复
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查、冒烟和编排演示入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营、编排和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
