# 短视频转码副本伸缩模拟器

FastAPI 模拟 Kubernetes 转码服务的副本伸缩，状态存 SQLite。监控漏报时按已知积压判断扩容；
缩容需连续两轮报齐且均建议缩容。在 `/docs`（Swagger UI）上报和查询。

## 规则实现

| 规则 | 实现 |
| --- | --- |
| 每 10 个待转 1 副本，向上取整，至少 1 个 | `app/scaler.py` 中 `_suggest()` |
| 漏报扩容：已知积压 > 目标×10 即扩，不等报齐 | `submit_report()` 内部分积压检查 |
| 缩容：就绪数==目标，且轮号连续两轮报齐、均建议缩容，取两轮建议较大值 | `submit_report()` 报齐分支 |
| 名单或目标变更后重新累计 | 名单更新 / 目标变更时清空连续低负载累计 |
| 过期名单、重报、迟到旧轮（含并发）无副作用 | 全局锁 + `BEGIN IMMEDIATE` 事务；`UNIQUE(version, round, pod)` 兜底；忽略时不写库 |

## 副本缺口观察（值班员视角）

扩容目标调高但 Pod 没补齐时，值班员通过 `GET /gap` 直接拿到**目标、就绪数、还差几个、已等多久、是否超时**。

| 规则 | 实现 |
| --- | --- |
| 首次登记就绪名单后，就绪数 < 目标即开始计时 | 名单/上报写事务内 `_evaluate_gap()`，初始占位空名单（version=0）不观察 |
| 缺口持续期间目标或名单变化，只要缺口一直在，沿用原起点 | `state.gap_started_at` 持久化，持续缺口只更新不重置 |
| 连续缺副本满 30 秒记为超时 | 默认 `GAP_TIMEOUT_DEFAULT=30`，环境变量 `GAP_TIMEOUT_SECONDS` 覆盖；进行中片段查询时动态计算，补齐关闭时落库 |
| 补齐后保留起止时间和超时结果；短暂补齐后再不足记为两次 | 补齐关闭当前 `gap_episodes` 行，再次不足新开一行；评估挂在写事务上，无需有人查询 |
| 重启后仍按原起点计时 | 起点与片段持久化在 SQLite（compose 卷 `/data`），重启构造时不重开片段 |
| 重置演练清除缺口记录 | `POST /admin/reset` 清空 `gap_episodes` 并复位 gap 状态 |
| 已有库从启用功能时开始观察 | 启动时 `PRAGMA table_info` 检测并 `ALTER TABLE` 补三列，随后按当前状态立即评估一次 |
| 目标或名单写入失败时保留原缺口记录 | 缺口评估与业务写入同一 `BEGIN IMMEDIATE` 事务，失败整体回滚 |
| 查询从同一已提交状态返回，无需重扫历史 | `GET /gap` 单事务读 `state`+当前名单，超时由当前时间与持久化起点计算 |

## 接口（详见 /docs）

- `POST /roster` — 值班员更新就绪 Pod 名单（内容变化才升版号并清零累计）
- `POST /reports` — 监控上报 `{roster_version, round, pod, pending}`；过期/重报/迟到返回 `outcome=ignored`
- `GET /target` — 目标副本数及最近一次调整原因
- `GET /gap` — 当前副本缺口：`target / ready / shortage / gap_active / started_at / elapsed_seconds / timeout_seconds / timed_out`
- `GET /gap/history?status=all|active|recovered` — 缺口片段历史，已恢复片段含起止时间与超时结果
- `GET /state` — 当前轮上报明细、低负载累计
- `GET /events` — 伸缩与名单变更历史
- `GET /roster`、`POST /admin/reset`（演示/测试辅助）

## 启动

```bash
docker compose up --build   # 然后访问 http://localhost:8000/docs
```

需要调小超时阈值做演练：`GAP_TIMEOUT_SECONDS=1 docker compose up --build`。

本地开发：`pip install -r requirements.txt && uvicorn app.main:app --reload`
（DB 路径用环境变量 `SCALER_DB` 指定，默认 `./scaler.db`，compose 中为 `/data/scaler.db` 卷持久化；
缺口超时秒数用 `GAP_TIMEOUT_SECONDS` 指定，默认 30）

测试：

- `python3 tests/scenario_test.py [base_url]`（仅标准库，伸缩主流程 34 项断言，含缺口查询）
- `python3 tests/gap_test.py [base_url]`（仅标准库，缺口计时/超时/切分/重启/回滚/迁移 29 项断言；
  重启子场景需 `GAP_TEST_RESTART=1` 并由驱动脚本传入服务 PID）

## 改动清单

- `app/scaler.py` — 伸缩状态机 + SQLite 持久化（rosters / reports / state / events / gap_episodes 五表）；
  `state` 增加 `gap_active / gap_started_at / gap_timeout_secs` 三列并支持老库 `ALTER TABLE` 迁移；
  新增 `_evaluate_gap()`、`get_gap()`、`get_gap_history()`
- `app/main.py` — FastAPI 路由与入参校验，新增 `GET /gap`、`GET /gap/history`，读取 `GAP_TIMEOUT_SECONDS`
- `tests/scenario_test.py` — 原端到端场景测试补缺口断言
- `tests/gap_test.py` — 缺口观察专项端到端测试（含真实进程重启与写入失败回滚）
- `docker-compose.yml` — 透传 `GAP_TIMEOUT_SECONDS`（默认 30）
- `Dockerfile`、`.dockerignore`、`requirements.txt` — 容器化交付（无需改动）
- `README.md` — 本文件

## 实测结果（本地 uvicorn + SQLite，两套测试全部通过）

1. **缺口计时与超时（阈值临时设为 1 秒）**：2 Pod 名单目标 1 时无缺口；漏报扩容到 3 后 `GET /gap`
   返回缺口 1、`gap_active=true`、已等时长从 0 起；再扩容到 6 缺口扩到 4，但 `started_at` 与片段 ID
   不变（沿用原起点）；换名单（就绪仍 2）起点依旧；满 1 秒后 `timed_out=true`，进行中历史同样动态显示超时。
2. **补齐与片段切分**：补齐到 6 个 Pod 后片段关闭，`/gap/history?status=recovered` 保留起止时间、
   起点目标/就绪/缺口和超时标记；随后在**全程不查询**的情况下缩名单→补齐→再不足，历史落为 3 个片段、
   其中 2 条已恢复，证明短暂补齐后再不足记为两次。
3. **重启沿用起点**：停掉 uvicorn 进程后用同一 SQLite 文件重新拉起，缺口仍在、`started_at` 与重启前
   完全一致、已等时长持续累计、历史片段不丢失。
4. **重置与写入失败**：`/admin/reset` 后缺口状态与历史全部清空；通过预置主键冲突让名单插入失败，
   事务回滚后原缺口起点、缺口数、目标和片段数量均保持不变，故障解除后写入恢复且仍沿用原起点。
5. **老库迁移**：用旧四表 schema 造的已有库（目标 5、就绪 2）被新代码自动补列建表，启用即观察到
   缺口 3 并从启用时刻计时，旧数据完好；无缺口的老库迁移后不产生空片段。
6. **原伸缩流程回归**：`scenario_test.py` 全部通过（漏报扩容、迟报/重报/并发忽略、连续低负载缩容、
   就绪数≠目标禁缩等）。

服务日志无 error/warning；`/docs`、`/openapi.json` 正常，新接口在 Swagger 中可见；非法 `status`
查询参数返回 422。

## 余项

- 本沙箱无 Docker，`Dockerfile`/`docker-compose.yml` 未实际构建，仅通过结构/YAML 校验；
  建议在有 Docker 的环境跑一次 `docker compose up --build` 冒烟（功能已用同版本 uvicorn 本地实测）。
- 缺口评估挂在名单/上报写事务上：若长期没有任何写入且不查询，进行中片段不会被主动关闭——
  这符合"补齐必然伴随名单写入"的模型；超时标记在查询或补齐时计算/落库，不依赖后台定时器。
- 扩容只按"当前轮已知积压"触发，不跨轮累计积压；如需按多轮未消化积压扩容需另加规则。
- 名单更新按内容集合判等（忽略顺序），重复提交相同名单不升版号、不清累计。
- 上报要求 `round >= 1`、`pending >= 0`；更旧的未报齐轮在新轮到达后即放弃，不再补算。
- 单进程 + 全局锁足以支撑本模拟场景；若要多副本部署需把锁下沉到数据库（如 `BEGIN IMMEDIATE` 已做，但跨进程未实测）。
