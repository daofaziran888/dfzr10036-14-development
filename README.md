# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 迟到事件分段（跨已关闭窗口的乱序处理）

旧版乱序策略在「超窗口丢弃」时把迟到事件直接计入丢弃行数，缺少入窗阈值判定。
现在跨过已关闭窗口的事件流先做边界判定，再形成可回验的迟到事件分段：

- **入窗阈值含边界**：`源位点 <= 窗口关闭位点` 归原窗口，`> 关闭位点` 进入下一窗口。
  关闭位点为 120 时，同流位点 119、120 属于原窗口 `W-0001/v1`，121 进入下一窗口 `W-0002`。
- **迟到分段**：归原窗口的迟到事件落成 `late_event_segment`，状态为「待复核」，
  并生成补偿行；进入下一窗口的事件状态为「正常可消费」，不会被迟到纠偏连带丢行。
- **窗口版本 + 源位点可追溯**：每个分段都记录窗口编号、窗口版本、源位点和入窗阈值判定依据。
- **幂等**：同一 `(数据流, 窗口编号, 窗口版本, 源位点, 事件引用)` 重复提交只生效一次；
  回放时已成功回执覆盖或已消费的位点只计入重复跳过，不会二次交付。
- **补偿行可查**：中断期间已交给下游的行带「中断期间交付」标记，
  `GET /api/late-window/compensations?interrupted_only=true` 可单独查询。
- **提交进度**：由分段状态推导实际可确认区间，遇到待复核分段就停在它前面；
  确认一笔补偿后区间才继续推进，不会越过关闭位点。
- **续跑关联**：`affected_windows` 把受影响窗口和已有成功回执关联起来，
  断点恢复按「受影响窗口 + 回执覆盖区间」安排回放，水位提交位点也取同一口径。

### 回验场景（关闭位点 120）

```bash
B=http://127.0.0.1:8000/api
# 1. 搭建场景：119/120 归原窗口，121 进入下一窗口；两种到达顺序等价
curl -s -X POST "$B/late-window/scenario?arrival_order=A"      # 或 arrival_order=B
curl -s -X POST "$B/late-window/compare-orders" -H 'Content-Type: application/json' \
  -d '{"positions":[119,120,121],"close_position":120}'
# 2. 未确认 120 的补偿时，提交位置停在 119，回放受阻
curl -s "$B/late-window/commit?stream=order_events_cdc"
curl -s -X POST "$B/late-window/replay" -H 'Content-Type: application/json' \
  -d '{"stream":"order_events_cdc","from_position":119}'
# 3. 确认 120 的补偿（重复确认返回 duplicate），从位点 119 回放
#    补偿编号按源位点确定性生成，两种到达顺序下位点 120 都是 COMP-0120
curl -s -X POST "$B/late-window/compensations/COMP-0120/confirm"
curl -s -X POST "$B/late-window/replay" -H 'Content-Type: application/json' \
  -d '{"stream":"order_events_cdc","from_position":119}'
# 4. 逐项回验：入窗阈值、幂等、窗口版本、下一窗口不丢行、提交区间
curl -s "$B/late-window/verify"
```

前端「迟到事件分段」页面（`/late-window`）提供同样的搭建、确认、回放和校验入口。
策略级入口在乱序处理页：`POST /api/out_of_order/{id}/actions` 只做状态流转，
迟到判定走 `OutOfOrderService.judge_late_events`（带源位点时返回分段与两种顺序对照）。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/late_window.py 迟到事件分段：判定/补偿/回放/续跑/回验
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/late_window.py 迟到事件分段引擎（入窗阈值、幂等、提交区间）
│   ├── app/services/base.py     共享服务骨架
│   ├── app/store.py          内存数据仓库与示例数据
│   └── tests/                迟到事件分段的回归测试（标准库 unittest）
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

迟到事件分段回归测试（只依赖标准库）：

```bash
cd backend
.venv/bin/python -m unittest discover -s tests
```

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
