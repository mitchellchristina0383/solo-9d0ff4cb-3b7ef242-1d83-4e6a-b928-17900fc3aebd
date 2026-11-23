"""转码服务副本伸缩状态机，状态持久化在 SQLite。

规则：
- 每 VIDEOS_PER_REPLICA 个待转视频需一个副本，向上取整，至少 MIN_REPLICAS 个。
- 漏报扩容：某轮未报齐时，只要已知积压超出当前目标容量即按已知积压扩容。
- 缩容：就绪数等于目标，且轮号连续的两轮都报齐、都建议缩容，取两轮建议的较大值。
- 名单或目标变更后重新累计（连续低负载计数清零）。
- 过期名单 / 重报 / 迟到的旧轮（含并发到达）不改变目标和累计：
  全局锁 + BEGIN IMMEDIATE 事务串行化写入，UNIQUE(version, round, pod) 兜底幂等。

副本缺口观察（gap）：
- 首次登记就绪名单后，就绪数 < 目标即出现缺口，缺口片段从那一刻开始计时。
- 缺口持续期间目标或名单变化，只要缺口没补齐，沿用同一起点；补齐后片段关闭，
  再次不足则新开片段（即使中间没人查询，短暂补齐也会切成两段记录）。
- 连续缺副本满 GAP_TIMEOUT_DEFAULT 秒记为超时；补齐关闭片段时按实际时长落超时标记，
  查询进行中片段时按已等时长动态计算超时。
- 缺口评估与名单/上报写入在同一事务内：写入失败整体回滚，原缺口记录保留。
- 起点持久化在 state / gap_episodes 中，进程重启后沿用原起点；reset 清除全部缺口记录。
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
from datetime import datetime, timezone

VIDEOS_PER_REPLICA = 10  # 每 10 个待转视频需一个副本
MIN_REPLICAS = 1         # 至少留一个
GAP_TIMEOUT_DEFAULT = 30  # 连续缺副本满 30 秒记为超时

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rosters (
    version     INTEGER PRIMARY KEY,
    pods        TEXT NOT NULL,          -- JSON 数组，就绪 Pod 名单
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reports (
    roster_version INTEGER NOT NULL,
    round          INTEGER NOT NULL,
    pod            TEXT NOT NULL,
    pending        INTEGER NOT NULL,
    received_at    TEXT NOT NULL,
    PRIMARY KEY (roster_version, round, pod)   -- 重报幂等兜底
);
CREATE TABLE IF NOT EXISTS state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    target              INTEGER NOT NULL,
    roster_version      INTEGER NOT NULL,
    max_round_seen      INTEGER,        -- 当前名单版本下已见到的最大轮号
    last_complete_round INTEGER,        -- 连续低负载累计：上一报齐轮号
    last_suggestion     INTEGER,        -- 连续低负载累计：上一报齐轮建议副本数
    reason              TEXT NOT NULL,  -- 最近一次目标/名单调整原因
    updated_at          TEXT NOT NULL,
    gap_active          INTEGER NOT NULL DEFAULT 0,  -- 当前是否存在副本缺口
    gap_started_at      TEXT,           -- 当前缺口片段起点（持续期间不随目标/名单变化而重置）
    gap_timeout_secs    INTEGER NOT NULL DEFAULT 30  -- 当前缺口片段的超时阈值（秒）
);
CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             TEXT NOT NULL,
    kind           TEXT NOT NULL,       -- scale_up | scale_down | roster_update
    old_target     INTEGER NOT NULL,
    new_target     INTEGER NOT NULL,
    roster_version INTEGER NOT NULL,
    round          INTEGER,
    reason         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gap_episodes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,      -- 缺口起点
    ended_at        TEXT,               -- 补齐时间；NULL 表示进行中
    start_target    INTEGER NOT NULL,   -- 起点时刻的目标副本数
    start_ready     INTEGER NOT NULL,   -- 起点时刻的就绪数
    start_shortage  INTEGER NOT NULL,   -- 起点时刻的缺口数
    timeout_secs    INTEGER NOT NULL,   -- 超时阈值（秒）
    timed_out       INTEGER NOT NULL DEFAULT 0  -- 连续缺副本是否满超时阈值
);
"""

# 老库迁移：state 表补齐缺口观察相关列（已有库从启用功能时开始观察）
_STATE_GAP_COLUMNS = {
    "gap_active": "ALTER TABLE state ADD COLUMN gap_active INTEGER NOT NULL DEFAULT 0",
    "gap_started_at": "ALTER TABLE state ADD COLUMN gap_started_at TEXT",
    "gap_timeout_secs": "ALTER TABLE state ADD COLUMN gap_timeout_secs INTEGER NOT NULL DEFAULT 30",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _parse_ts(value: str) -> datetime:
    """解析 _now() 产生的 ISO8601 UTC 时间串。"""
    return datetime.fromisoformat(value)


def _elapsed_seconds(started_at: str, now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    return round((now - _parse_ts(started_at)).total_seconds(), 3)


def _suggest(backlog: int) -> int:
    """按积压量计算建议副本数：每 10 个一个，向上取整，至少 1 个。"""
    return max(MIN_REPLICAS, math.ceil(backlog / VIDEOS_PER_REPLICA))


class Scaler:
    """副本伸缩状态机。所有写操作在全局锁 + BEGIN IMMEDIATE 事务内完成。"""

    def __init__(self, db_path: str, gap_timeout_secs: int = GAP_TIMEOUT_DEFAULT):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._gap_timeout_secs = gap_timeout_secs
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            # 老库迁移：补齐缺口观察列；已有库从启用功能这一刻开始观察
            existing = {r["name"] for r in conn.execute("PRAGMA table_info(state)")}
            for column, ddl in _STATE_GAP_COLUMNS.items():
                if column not in existing:
                    conn.execute(ddl)
            if conn.execute("SELECT COUNT(*) AS c FROM state").fetchone()["c"] == 0:
                now = _now()
                conn.execute(
                    "INSERT INTO rosters(version, pods, created_at) VALUES (0, '[]', ?)", (now,))
                conn.execute(
                    "INSERT INTO state(id, target, roster_version, max_round_seen, "
                    "last_complete_round, last_suggestion, reason, updated_at, "
                    "gap_active, gap_started_at, gap_timeout_secs) "
                    "VALUES (1, ?, 0, NULL, NULL, NULL, ?, ?, 0, NULL, ?)",
                    (MIN_REPLICAS, "初始状态：目标副本数 1", now, gap_timeout_secs))
            else:
                # 已有库：按当前已提交状态立即评估一次（功能启用即开始观察）
                self._evaluate_gap(conn)
        finally:
            conn.close()

    # ---------------------------------------------------------------- 基础

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @staticmethod
    def _load(conn) -> tuple[sqlite3.Row, list[str]]:
        st = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
        roster = conn.execute(
            "SELECT pods FROM rosters WHERE version=?",
            (st["roster_version"],)).fetchone()
        return st, json.loads(roster["pods"])

    def _apply(self, conn, kind: str, new_target: int,
               version: int, round_no: int, reason: str) -> dict:
        """变更目标副本数，并按规则重置连续低负载累计（目标变更后重新累计）。"""
        old = conn.execute("SELECT target FROM state WHERE id=1").fetchone()["target"]
        now = _now()
        conn.execute(
            "UPDATE state SET target=?, reason=?, updated_at=?, "
            "last_complete_round=NULL, last_suggestion=NULL WHERE id=1",
            (new_target, reason, now))
        conn.execute(
            "INSERT INTO events(ts, kind, old_target, new_target, roster_version, round, reason) "
            "VALUES (?,?,?,?,?,?,?)",
            (now, kind, old, new_target, version, round_no, reason))
        return {"kind": kind, "old_target": old, "new_target": new_target, "reason": reason}

    # -------------------------------------------------------- 副本缺口观察

    def _evaluate_gap(self, conn) -> None:
        """按当前已提交的目标与就绪名单评估副本缺口，必须在写事务内调用。

        - 就绪数 < 目标即缺口；缺口持续（含目标/名单变化但一直没补齐）沿用原起点。
        - 补齐则关闭当前片段；再次不足新开片段，因此短暂补齐后再不足记为两次。
        - 进行中片段跨过超时阈值时只在 state 上落 timed_out 语义（由查询动态计算），
          补齐关闭时把最终超时标记与起止时间写入 gap_episodes。
        """
        st = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
        # 首次登记就绪名单（version>=1）后才开始观察；初始占位空名单不计缺口
        if st["roster_version"] < 1:
            return
        roster = conn.execute(
            "SELECT pods FROM rosters WHERE version=?", (st["roster_version"],)).fetchone()
        ready = len(json.loads(roster["pods"])) if roster else 0
        target = st["target"]
        shortage = max(0, target - ready)
        now = _now()

        if shortage > 0:
            if st["gap_active"]:
                # 缺口一直在（目标或名单可能变过）：沿用原起点，不重置计时
                return
            # 新缺口片段：首次登记名单后就绪数低于目标，或补齐后再次不足
            conn.execute(
                "UPDATE state SET gap_active=1, gap_started_at=?, gap_timeout_secs=? "
                "WHERE id=1",
                (now, self._gap_timeout_secs))
            conn.execute(
                "INSERT INTO gap_episodes(started_at, ended_at, start_target, start_ready, "
                "start_shortage, timeout_secs, timed_out) VALUES (?,?,?,?,?,?,0)",
                (now, None, target, ready, shortage, self._gap_timeout_secs))
        elif st["gap_active"]:
            # 缺口补齐：关闭当前片段，保留起止时间和超时结果
            started_at = st["gap_started_at"]
            timeout_secs = st["gap_timeout_secs"]
            timed_out = _elapsed_seconds(started_at) >= timeout_secs
            conn.execute(
                "UPDATE gap_episodes SET ended_at=?, timed_out=? "
                "WHERE id=(SELECT MAX(id) FROM gap_episodes)",
                (now, 1 if timed_out else 0))
            conn.execute(
                "UPDATE state SET gap_active=0, gap_started_at=NULL WHERE id=1")

    # ------------------------------------------------------------ 名单管理

    def update_roster(self, pods: list[str]) -> dict:
        """值班员更新就绪 Pod 名单。名单变更产生新版号并重新累计。"""
        clean = list(dict.fromkeys(p.strip() for p in pods if p and p.strip()))
        if not clean:
            raise ValueError("pods 不能为空")
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                st, current = self._load(conn)
                if set(clean) == set(current):
                    self._evaluate_gap(conn)  # 状态未变：缺口在则沿用原起点，无副作用
                    conn.execute("COMMIT")
                    return {
                        "changed": False,
                        "roster_version": st["roster_version"],
                        "pods": current,
                        "target": st["target"],
                        "message": "名单内容未变化，版号不变，累计保留",
                    }
                new_version = st["roster_version"] + 1
                now = _now()
                conn.execute(
                    "INSERT INTO rosters(version, pods, created_at) VALUES (?,?,?)",
                    (new_version, json.dumps(clean, ensure_ascii=False), now))
                reason = (f"就绪名单更新（版本 {st['roster_version']}→{new_version}，"
                          f"{len(clean)} 个 Pod），连续低负载累计清零")
                conn.execute(
                    "UPDATE state SET roster_version=?, max_round_seen=NULL, "
                    "last_complete_round=NULL, last_suggestion=NULL, reason=?, updated_at=? "
                    "WHERE id=1",
                    (new_version, reason, now))
                conn.execute(
                    "INSERT INTO events(ts, kind, old_target, new_target, roster_version, "
                    "round, reason) VALUES (?,?,?,?,?,?,?)",
                    (now, "roster_update", st["target"], st["target"], new_version, None, reason))
                self._evaluate_gap(conn)
                conn.execute("COMMIT")
                return {
                    "changed": True,
                    "roster_version": new_version,
                    "pods": clean,
                    "target": st["target"],
                    "message": reason,
                }
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    # ------------------------------------------------------------ 监控上报

    def submit_report(self, roster_version: int, round_no: int,
                      pod: str, pending: int) -> dict:
        """监控上报某 Pod 一轮的待转数。过期名单/重报/迟到旧轮一律忽略且无副作用。"""
        pod = pod.strip()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                st, pods = self._load(conn)
                target = st["target"]

                def ignored(reason: str) -> dict:
                    conn.execute("COMMIT")  # 未写入任何内容，纯读事务
                    return {
                        "outcome": "ignored",
                        "reason": reason,
                        "target": target,
                        "roster_version": st["roster_version"],
                        "scaling": None,
                    }

                # 1) 过期名单
                if roster_version != st["roster_version"]:
                    return ignored(
                        f"过期名单版本 {roster_version}（当前版本 {st['roster_version']}），已忽略")
                # 2) 未知 Pod
                if pod not in pods:
                    return ignored(f"Pod '{pod}' 不在当前就绪名单中，已忽略")
                # 3) 迟到的旧轮
                if st["max_round_seen"] is not None and round_no < st["max_round_seen"]:
                    return ignored(
                        f"迟到的旧轮 {round_no}（当前名单已处理到第 {st['max_round_seen']} 轮），已忽略")
                # 4) 重报（SELECT 预检 + UNIQUE 约束兜底，并发下也只有一条生效）
                dup = conn.execute(
                    "SELECT 1 FROM reports WHERE roster_version=? AND round=? AND pod=?",
                    (roster_version, round_no, pod)).fetchone()
                if dup:
                    return ignored(
                        f"重复上报（版本 {roster_version} 第 {round_no} 轮 Pod '{pod}'），已忽略")
                conn.execute(
                    "INSERT INTO reports(roster_version, round, pod, pending, received_at) "
                    "VALUES (?,?,?,?,?)",
                    (roster_version, round_no, pod, pending, _now()))

                # 推进当前名单版本下的最大轮号；更旧的未报齐轮就此放弃
                if st["max_round_seen"] is None or round_no > st["max_round_seen"]:
                    conn.execute("UPDATE state SET max_round_seen=? WHERE id=1", (round_no,))

                # 本轮已知积压
                known = conn.execute(
                    "SELECT COALESCE(SUM(pending),0) AS s FROM reports "
                    "WHERE roster_version=? AND round=?",
                    (roster_version, round_no)).fetchone()["s"]

                scaling = None
                notes = []

                # 漏报扩容：不要求报齐，已知积压超出当前目标容量即扩容
                if known > target * VIDEOS_PER_REPLICA:
                    new_target = _suggest(known)
                    reason = (f"第 {round_no} 轮已知积压 {known} 超出目标 {target} 的容量 "
                              f"{target * VIDEOS_PER_REPLICA}（监控可能漏报，按已知积压提前扩容），"
                              f"扩容至 {new_target}")
                    scaling = self._apply(conn, "scale_up", new_target,
                                          roster_version, round_no, reason)
                    target = new_target

                # 报齐判定
                reported = {r["pod"] for r in conn.execute(
                    "SELECT pod FROM reports WHERE roster_version=? AND round=?",
                    (roster_version, round_no))}
                complete = reported == set(pods)
                if complete:
                    suggestion = _suggest(known)
                    st2 = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
                    cur_target = st2["target"]
                    if suggestion > cur_target:
                        reason = (f"第 {round_no} 轮报齐，总积压 {known} 需要 {suggestion} "
                                  f"个副本，扩容")
                        scaling = self._apply(conn, "scale_up", suggestion,
                                              roster_version, round_no, reason)
                        target = suggestion
                    elif suggestion < cur_target:
                        prev_round = st2["last_complete_round"]
                        prev_sugg = st2["last_suggestion"]
                        ready = len(pods)
                        if (prev_round is not None and prev_round == round_no - 1
                                and prev_sugg is not None and prev_sugg < cur_target
                                and ready == cur_target):
                            new_target = max(suggestion, prev_sugg)
                            reason = (f"第 {prev_round}、{round_no} 轮连续报齐且均建议缩容"
                                      f"（建议 {prev_sugg}、{suggestion}），取两轮较大值，"
                                      f"缩容至 {new_target}")
                            scaling = self._apply(conn, "scale_down", new_target,
                                                  roster_version, round_no, reason)
                            target = new_target
                        else:
                            conn.execute(
                                "UPDATE state SET last_complete_round=?, last_suggestion=? "
                                "WHERE id=1", (round_no, suggestion))
                            gate = ""
                            if ready != cur_target:
                                gate = f"；就绪数 {ready} ≠ 目标 {cur_target}，缩容被禁止"
                            elif prev_round is None or prev_round != round_no - 1:
                                gate = "；尚无连续上一轮，开始重新累计"
                            notes.append(
                                f"第 {round_no} 轮报齐，建议缩容至 {suggestion}，已计入累计{gate}")
                    else:
                        conn.execute(
                            "UPDATE state SET last_complete_round=?, last_suggestion=? "
                            "WHERE id=1", (round_no, suggestion))
                        notes.append(
                            f"第 {round_no} 轮报齐，建议 {suggestion} 与目标一致，不计入低负载累计")

                # 目标可能已在本事务内调整：按同一事务的最新目标/名单评估副本缺口
                self._evaluate_gap(conn)
                conn.execute("COMMIT")
                return {
                    "outcome": "accepted",
                    "reason": "; ".join(notes) if notes else "已受理",
                    "round_complete": complete,
                    "known_backlog": known,
                    "target": target,
                    "roster_version": roster_version,
                    "scaling": scaling,
                }
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    # ---------------------------------------------------------------- 查询

    def get_target(self) -> dict:
        with self._lock:
            conn = self._connect()
            try:
                st, pods = self._load(conn)
                return {
                    "target": st["target"],
                    "reason": st["reason"],
                    "updated_at": st["updated_at"],
                    "roster_version": st["roster_version"],
                    "ready_count": len(pods),
                    "capacity": st["target"] * VIDEOS_PER_REPLICA,
                }
            finally:
                conn.close()

    def get_roster(self) -> dict:
        with self._lock:
            conn = self._connect()
            try:
                st, pods = self._load(conn)
                return {"roster_version": st["roster_version"], "pods": pods}
            finally:
                conn.close()

    def get_state(self) -> dict:
        with self._lock:
            conn = self._connect()
            try:
                st, pods = self._load(conn)
                active_round = st["max_round_seen"]
                reports = []
                if active_round is not None:
                    reports = [dict(r) for r in conn.execute(
                        "SELECT pod, pending, received_at FROM reports "
                        "WHERE roster_version=? AND round=? ORDER BY pod",
                        (st["roster_version"], active_round))]
                return {
                    "target": st["target"],
                    "reason": st["reason"],
                    "roster_version": st["roster_version"],
                    "ready_pods": pods,
                    "max_round_seen": active_round,
                    "active_round_reports": reports,
                    "low_load_streak": {
                        "last_complete_round": st["last_complete_round"],
                        "last_suggestion": st["last_suggestion"],
                    },
                }
            finally:
                conn.close()

    def get_events(self, limit: int = 50) -> dict:
        with self._lock:
            conn = self._connect()
            try:
                rows = [dict(r) for r in conn.execute(
                    "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))]
                return {"events": rows}
            finally:
                conn.close()

    def get_gap(self) -> dict:
        """从同一已提交状态返回目标、就绪数、缺口、已等时长和超时状态，不重扫历史。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                st, pods = self._load(conn)
                ready = len(pods)
                target = st["target"]
                # 未登记就绪名单前不观察缺口
                shortage = max(0, target - ready) if st["roster_version"] >= 1 else 0
                active = bool(st["gap_active"])
                started_at = st["gap_started_at"]
                timeout_secs = st["gap_timeout_secs"] or self._gap_timeout_secs
                elapsed = _elapsed_seconds(started_at) if active and started_at else 0.0
                timed_out = active and elapsed >= timeout_secs
                episode_id = None
                if active:
                    row = conn.execute(
                        "SELECT MAX(id) AS id FROM gap_episodes").fetchone()
                    episode_id = row["id"] if row else None
                conn.execute("COMMIT")
                return {
                    "target": target,
                    "ready": ready,
                    "shortage": shortage,
                    "roster_registered": st["roster_version"] >= 1,
                    "gap_active": active,
                    "episode_id": episode_id,
                    "started_at": started_at,
                    "elapsed_seconds": elapsed,
                    "timeout_seconds": timeout_secs,
                    "timed_out": timed_out,
                }
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def get_gap_history(self, status: str = "all", limit: int = 50) -> dict:
        """查询缺口片段历史：all | active（进行中）| recovered（已补齐）。

        进行中片段的已等时长/超时为查询时刻动态计算；已补齐片段保留落库的起止与超时结果。
        """
        if status not in ("all", "active", "recovered"):
            raise ValueError("status 只能是 all、active 或 recovered")
        where = ""
        params: list = []
        if status == "active":
            where = " WHERE ended_at IS NULL"
        elif status == "recovered":
            where = " WHERE ended_at IS NOT NULL"
        with self._lock:
            conn = self._connect()
            try:
                rows = [dict(r) for r in conn.execute(
                    f"SELECT * FROM gap_episodes{where} ORDER BY id DESC LIMIT ?",
                    params + [limit])]
                now = datetime.now(timezone.utc)
                episodes = []
                for r in rows:
                    ended = r["ended_at"] is not None
                    if ended:
                        elapsed = _elapsed_seconds(r["started_at"], _parse_ts(r["ended_at"]))
                        timed_out = bool(r["timed_out"])
                    else:
                        elapsed = _elapsed_seconds(r["started_at"], now)
                        timed_out = elapsed >= r["timeout_secs"]
                    episodes.append({
                        "episode_id": r["id"],
                        "status": "recovered" if ended else "active",
                        "started_at": r["started_at"],
                        "ended_at": r["ended_at"],
                        "start_target": r["start_target"],
                        "start_ready": r["start_ready"],
                        "start_shortage": r["start_shortage"],
                        "timeout_seconds": r["timeout_secs"],
                        "elapsed_seconds": elapsed,
                        "timed_out": timed_out,
                    })
                return {"episodes": episodes}
            finally:
                conn.close()

    # ---------------------------------------------------------------- 维护

    def reset(self) -> dict:
        """清空全部状态回到初始值（演示/测试辅助），缺口记录一并清除。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM reports")
                conn.execute("DELETE FROM events")
                conn.execute("DELETE FROM rosters")
                conn.execute("DELETE FROM gap_episodes")
                now = _now()
                conn.execute(
                    "INSERT INTO rosters(version, pods, created_at) VALUES (0, '[]', ?)", (now,))
                conn.execute(
                    "UPDATE state SET target=?, roster_version=0, max_round_seen=NULL, "
                    "last_complete_round=NULL, last_suggestion=NULL, reason=?, updated_at=?, "
                    "gap_active=0, gap_started_at=NULL, gap_timeout_secs=? WHERE id=1",
                    (MIN_REPLICAS, "已重置：目标副本数 1", now, self._gap_timeout_secs))
                conn.execute("COMMIT")
                return {"reset": True, "target": MIN_REPLICAS, "roster_version": 0}
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
