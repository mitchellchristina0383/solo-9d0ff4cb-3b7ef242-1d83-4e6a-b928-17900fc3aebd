"""转码服务副本伸缩状态机，状态持久化在 SQLite。

规则：
- 每 VIDEOS_PER_REPLICA 个待转视频需一个副本，向上取整，至少 MIN_REPLICAS 个。
- 漏报扩容：某轮未报齐时，只要已知积压超出当前目标容量即按已知积压扩容。
- 缩容：就绪数等于目标，且轮号连续的两轮都报齐、都建议缩容，取两轮建议的较大值。
- 名单或目标变更后重新累计（连续低负载计数清零）。
- 过期名单 / 重报 / 迟到的旧轮（含并发到达）不改变目标和累计：
  全局锁 + BEGIN IMMEDIATE 事务串行化写入，UNIQUE(version, round, pod) 兜底幂等。
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
    updated_at          TEXT NOT NULL
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
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _suggest(backlog: int) -> int:
    """按积压量计算建议副本数：每 10 个一个，向上取整，至少 1 个。"""
    return max(MIN_REPLICAS, math.ceil(backlog / VIDEOS_PER_REPLICA))


class Scaler:
    """副本伸缩状态机。所有写操作在全局锁 + BEGIN IMMEDIATE 事务内完成。"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            if conn.execute("SELECT COUNT(*) AS c FROM state").fetchone()["c"] == 0:
                now = _now()
                conn.execute(
                    "INSERT INTO rosters(version, pods, created_at) VALUES (0, '[]', ?)", (now,))
                conn.execute(
                    "INSERT INTO state(id, target, roster_version, max_round_seen, "
                    "last_complete_round, last_suggestion, reason, updated_at) "
                    "VALUES (1, ?, 0, NULL, NULL, NULL, ?, ?)",
                    (MIN_REPLICAS, "初始状态：目标副本数 1", now))
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

    # ---------------------------------------------------------------- 维护

    def reset(self) -> dict:
        """清空全部状态回到初始值（演示/测试辅助）。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM reports")
                conn.execute("DELETE FROM events")
                conn.execute("DELETE FROM rosters")
                now = _now()
                conn.execute(
                    "INSERT INTO rosters(version, pods, created_at) VALUES (0, '[]', ?)", (now,))
                conn.execute(
                    "UPDATE state SET target=?, roster_version=0, max_round_seen=NULL, "
                    "last_complete_round=NULL, last_suggestion=NULL, reason=?, updated_at=? "
                    "WHERE id=1",
                    (MIN_REPLICAS, "已重置：目标副本数 1", now))
                conn.execute("COMMIT")
                return {"reset": True, "target": MIN_REPLICAS, "roster_version": 0}
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
