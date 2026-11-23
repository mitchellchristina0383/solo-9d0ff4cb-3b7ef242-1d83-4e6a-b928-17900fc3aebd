"""FastAPI 入口：模拟 Kubernetes 转码服务的副本伸缩。

在 /docs 打开 Swagger UI 即可上报和查询。
"""
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.scaler import GAP_TIMEOUT_DEFAULT, MIN_REPLICAS, VIDEOS_PER_REPLICA, Scaler

DB_PATH = os.environ.get("SCALER_DB", "./scaler.db")


def _gap_timeout() -> int:
    raw = os.environ.get("GAP_TIMEOUT_SECONDS", str(GAP_TIMEOUT_DEFAULT))
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"GAP_TIMEOUT_SECONDS 必须是正整数秒，实际为 {raw!r}")
    if value <= 0:
        raise RuntimeError(f"GAP_TIMEOUT_SECONDS 必须是正整数秒，实际为 {value}")
    return value


app = FastAPI(
    title="短视频转码副本伸缩模拟器",
    version="1.1.0",
    description=(
        f"每 {VIDEOS_PER_REPLICA} 个待转视频需一个副本（向上取整，至少 {MIN_REPLICAS} 个）。\n\n"
        "- **POST /roster**：值班员更新就绪 Pod 名单（产生新版号并重新累计）\n"
        "- **POST /reports**：监控每轮上报各 Pod 待转数（附轮号和名单版号）\n"
        "- **GET /target**：查询目标副本数及调整原因\n"
        "- **GET /gap**：查询当前副本缺口（目标、就绪数、缺口、已等时长、超时状态）\n"
        "- **GET /gap/history**：查询缺口片段历史（含已恢复记录的起止时间和超时结果）\n"
        "- **GET /state /events /roster**：查询累计状态、调整历史、当前名单\n\n"
        f"缺口计时规则：首次登记名单后就绪数低于目标即开始计时；缺口持续期间目标或名单变化"
        f"沿用原起点；连续缺副本满 {_gap_timeout()} 秒记为超时；补齐后再不足记为新片段；"
        f"重启沿用原起点，reset 清除全部缺口记录。"
    ),
)
scaler = Scaler(DB_PATH, gap_timeout_secs=_gap_timeout())


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


@app.get("/gap", summary="查询当前副本缺口（还差几个、已等多久、是否超时）")
def get_gap():
    """从同一已提交状态返回目标、就绪数、缺口、已等时长和超时状态，无需重扫历史。"""
    return scaler.get_gap()


@app.get("/gap/history", summary="查询缺口片段历史（进行中/已恢复）")
def get_gap_history(status: str = "all", limit: int = 50):
    """status=all|active|recovered；已恢复片段保留起止时间与超时结果。"""
    try:
        return scaler.get_gap_history(status, limit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/state", summary="查询累计状态（当前轮上报明细、低负载累计）")
def get_state():
    return scaler.get_state()


@app.get("/events", summary="查询伸缩与名单变更历史")
def get_events(limit: int = 50):
    return scaler.get_events(limit)


@app.post("/admin/reset", summary="重置全部状态（演示/测试辅助）")
def reset():
    return scaler.reset()
