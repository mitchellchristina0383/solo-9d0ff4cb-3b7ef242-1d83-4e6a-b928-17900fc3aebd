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

## 接口（详见 /docs）

- `POST /roster` — 值班员更新就绪 Pod 名单（内容变化才升版号并清零累计）
- `POST /reports` — 监控上报 `{roster_version, round, pod, pending}`；过期/重报/迟到返回 `outcome=ignored`
- `GET /target` — 目标副本数及最近一次调整原因
- `GET /state` — 当前轮上报明细、低负载累计
- `GET /events` — 伸缩与名单变更历史
- `GET /roster`、`POST /admin/reset`（演示/测试辅助）

## 启动

```bash
docker compose up --build   # 然后访问 http://localhost:8000/docs
```

本地开发：`pip install -r requirements.txt && uvicorn app.main:app --reload`
（DB 路径用环境变量 `SCALER_DB` 指定，默认 `./scaler.db`，compose 中为 `/data/scaler.db` 卷持久化）

测试：`python3 tests/scenario_test.py [base_url]`（仅标准库，29 项断言）

## 改动清单

- `app/scaler.py` — 伸缩状态机 + SQLite 持久化（rosters / reports / state / events 四表）
- `app/main.py` — FastAPI 路由与入参校验
- `tests/scenario_test.py` — 端到端场景测试
- `Dockerfile`、`docker-compose.yml`、`.dockerignore`、`requirements.txt` — 容器化交付
- `README.md` — 本文件

## 实测结果（本地 uvicorn + SQLite，29 项断言全部通过）

1. **漏报扩容**：3 Pod 名单、目标 1；第 1 轮仅 p1 报 25（p2/p3 漏报）→ 立即扩到 3；p2 补报 30 → 已知积压 55 > 容量 30 → 扩到 6；p3 报 0 报齐后建议 6 与目标一致，不再变。事件流记录 2 次扩容及原因。
2. **换 Pod 后迟报**：名单升版后，旧版号上报、迟到的旧轮（2 < 已处理到 5）、重报、名单外 Pod 全部 `ignored`；16 路并发重报仅 1 条受理、8 路并发过期名单全部忽略；全程目标与低负载累计不变，无伸缩事件。
3. **连续低负载**：单轮低负载不缩容；连续两轮报齐且建议缩容才缩（2→1）；两轮建议 3、2 时取较大值缩到 3（5→3）。
4. **附加**：就绪数≠目标时连续低负载也不缩容；换名单后累计清零，需重新连续两轮才缩容。

服务日志无 error/warning；`/docs`、`/openapi.json` 正常。

## 余项

- 本沙箱无 Docker，`Dockerfile`/`docker-compose.yml` 未实际构建，仅通过 YAML 校验；建议在有 Docker 的环境跑一次 `docker compose up --build` 冒烟。
- 扩容只按"当前轮已知积压"触发，不跨轮累计积压；如需按多轮未消化积压扩容需另加规则。
- 名单更新按内容集合判等（忽略顺序），重复提交相同名单不升版号、不清累计。
- 上报要求 `round >= 1`、`pending >= 0`；更旧的未报齐轮在新轮到达后即放弃，不再补算。
- 单进程 + 全局锁足以支撑本模拟场景；若要多副本部署需把锁下沉到数据库（如 `BEGIN IMMEDIATE` 已做，但跨进程未实测）。
