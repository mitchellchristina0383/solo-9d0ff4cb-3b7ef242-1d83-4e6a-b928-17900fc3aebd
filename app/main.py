"""FastAPI 入口：模拟 Kubernetes 转码服务的副本伸缩。

在 /docs 打开 Swagger UI 即可上报和查询。
"""
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.scaler import MIN_REPLICAS, VIDEOS_PER_REPLICA, Scaler

DB_PATH = os.environ.get("SCALER_DB", "./scaler.db")

app = FastAPI(
    title="短视频转码副本伸缩模拟器",
    version="1.0.0",
    description=(
        f"每 {VIDEOS_PER_REPLICA} 个待转视频需一个副本（向上取整，至少 {MIN_REPLICAS} 个）。\n\n"
        "- **POST /roster**：值班员更新就绪 Pod 名单（产生新版号并重新累计）\n"
        "- **POST /reports**：监控每轮上报各 Pod 待转数（附轮号和名单版号）\n"
        "- **GET /target**：查询目标副本数及调整原因\n"
        "- **GET /state /events /roster**：查询累计状态、调整历史、当前名单"
    ),
)
scaler = Scaler(DB_PATH)


class RosterIn(BaseModel):
    pods: list[str] = Field(..., min_length=1, description="就绪 Pod 名单，如 [\"pod-a\",\"pod-b\"]")


class ReportIn(BaseModel):
    roster_version: int = Field(..., ge=0, description="名单版号")
    round: int = Field(..., ge=1, description="监控轮号")
    pod: str = Field(..., min_length=1, description="Pod 名称")
    pending: int = Field(..., ge=0, description="该 Pod 待转视频数")


@app.post("/roster", summary="值班员更新就绪 Pod 名单")
def update_roster(body: RosterIn):
    """名单内容变化时版号 +1，连续低负载累计清零；内容不变则幂等返回。"""
    try:
        return scaler.update_roster(body.pods)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/roster", summary="查询当前就绪名单及版号")
def get_roster():
    return scaler.get_roster()


@app.post("/reports", summary="监控上报某 Pod 一轮的待转数")
def submit_report(body: ReportIn):
    """过期名单 / 重报 / 迟到的旧轮返回 outcome=ignored，不改变目标和累计。"""
    return scaler.submit_report(body.roster_version, body.round, body.pod, body.pending)


@app.get("/target", summary="查询目标副本数及最近一次调整原因")
def get_target():
    return scaler.get_target()


@app.get("/state", summary="查询累计状态（当前轮上报明细、低负载累计）")
def get_state():
    return scaler.get_state()


@app.get("/events", summary="查询伸缩与名单变更历史")
def get_events(limit: int = 50):
    return scaler.get_events(limit)


@app.post("/admin/reset", summary="重置全部状态（演示/测试辅助）")
def reset():
    return scaler.reset()
