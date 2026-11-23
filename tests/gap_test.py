#!/usr/bin/env python3
"""副本缺口观察端到端测试：计时起点、超时、短时恢复切两段、重启沿用起点、重置清除。

用法:
  GAP_TIMEOUT_SECONDS=1 uvicorn app.main:app --port 8000 &   # 先起服务（超时阈值 1 秒）
  python3 tests/gap_test.py [base_url]                        # 默认 http://127.0.0.1:8000

仅依赖标准库；重启场景通过 subprocess 拉起/停止 uvicorn 验证 SQLite 持久化。
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
TIMEOUT_SECS = int(os.environ.get("GAP_TEST_TIMEOUT", "1"))
FAILURES = []


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  | {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def reset():
    call("POST", "/admin/reset")


def roster(pods):
    return call("POST", "/roster", {"pods": pods})


def report(v, rnd, pod, pending):
    return call("POST", "/reports",
                {"roster_version": v, "round": rnd, "pod": pod, "pending": pending})


def gap():
    return call("GET", "/gap")


def history(status="all"):
    return call("GET", f"/gap/history?status={status}")


# ------------------------------------------------ 场景 1：缺口出现、起点沿用、超时
print("场景 1：扩容后缺口开始计时；目标再调沿用原起点；满阈值标记超时")
reset()
v = roster(["p1", "p2"])["roster_version"]
g = gap()
check("登记名单且就绪数>=目标时无缺口",
      not g["gap_active"] and g["shortage"] == 0 and g["roster_registered"] is True,
      str(g))

r = report(v, 1, "p1", 25)  # 漏报扩容到 3，就绪 2 → 缺口 1
check("漏报扩容到 3", r["target"] == 3, f"target={r['target']}")
g = gap()
check("缺口出现：差 1 个、进行中、未超时",
      g["gap_active"] and g["shortage"] == 1 and g["ready"] == 2
      and g["target"] == 3 and not g["timed_out"], str(g))
started_at = g["started_at"]
eid = g["episode_id"]
check("已等时长从 0 附近开始", 0 <= g["elapsed_seconds"] < TIMEOUT_SECS,
      f"elapsed={g['elapsed_seconds']}")

r = report(v, 1, "p2", 30)  # 已知积压 55 → 扩到 6，缺口扩大到 4
check("再扩容到 6", r["target"] == 6, f"target={r['target']}")
g = gap()
check("缺口持续且扩大到 4", g["shortage"] == 4 and g["gap_active"], str(g))
check("目标变化后沿用原起点与同一片段",
      g["started_at"] == started_at and g["episode_id"] == eid, str(g))

# 名单变化（换 2 个新 Pod 名，就绪数仍 2），缺口仍在 → 仍沿用原起点
v2 = roster(["q1", "q2"])["roster_version"]
check("名单升版", v2 == v + 1)
g = gap()
check("名单变化但缺口一直在，沿用原起点",
      g["gap_active"] and g["started_at"] == started_at and g["episode_id"] == eid, str(g))

time.sleep(TIMEOUT_SECS + 0.2)
g = gap()
check(f"连续缺副本满 {TIMEOUT_SECS} 秒标记超时", g["timed_out"]
      and g["elapsed_seconds"] >= TIMEOUT_SECS, str(g))
h = history("active")["episodes"]
check("进行中片段历史也动态显示超时",
      len(h) == 1 and h[0]["timed_out"] and h[0]["episode_id"] == eid, str(h))

# ------------------------------------------------ 场景 2：补齐关闭，起止与超时落库
print("场景 2：补齐后关闭片段，保留起止时间和超时结果")
roster(["q1", "q2", "q3", "q4", "q5", "q6"])
g = gap()
check("补齐后无缺口", not g["gap_active"] and g["shortage"] == 0, str(g))
h = history("recovered")["episodes"]
check("已恢复片段 1 条，起止时间与超时结果已落库",
      len(h) == 1 and h[0]["status"] == "recovered" and h[0]["timed_out"]
      and h[0]["started_at"] == started_at and h[0]["ended_at"] is not None
      and h[0]["start_shortage"] == 1 and h[0]["start_target"] == 3,
      str(h))

# ------------------------------------------------ 场景 3：短暂补齐后再不足 → 两次（无需查询）
print("场景 3：无人查询期间短暂补齐再不足，记为两个片段")
# 当前无缺口；直接缩名单再次制造缺口，中间不查询 /gap
roster(["q1", "q2", "q3", "q4"])  # 目标 6，就绪 4 → 新缺口
g = gap()
check("再次不足产生新片段", g["gap_active"] and g["episode_id"] != eid
      and g["shortage"] == 2, str(g))
allh = history("all")["episodes"]
check("历史共 2 个片段：1 已恢复 + 1 进行中",
      len(allh) == 2
      and {e["status"] for e in allh} == {"active", "recovered"}, str(allh))

# 再补齐一次，紧接着又不足，全程不查 /gap
roster(["q1", "q2", "q3", "q4", "q5", "q6"])
roster(["q1", "q2"])
g = gap()
check("第三次缺口为第三个片段", g["gap_active"] and g["shortage"] == 4, str(g))
allh = history("all")["episodes"]
check("短暂补齐后再不足记为两次（共 3 片段，2 条已恢复）",
      len(allh) == 3 and sum(1 for e in allh if e["status"] == "recovered") == 2,
      str([(e["episode_id"], e["status"]) for e in allh]))

# --------------------------------------- 场景 4：reset 清除缺口记录（先于重启）
print("场景 4：重置演练清除全部缺口记录")
reset()
g = gap()
check("reset 后无缺口、无起点", not g["gap_active"] and g["started_at"] is None
      and g["roster_registered"] is False, str(g))
check("reset 后历史为空", history("all")["episodes"] == [])

# --------------------------------------- 场景 5：写入失败保留原缺口记录
print("场景 5：目标/名单写入失败时原缺口记录保留（SQL 约束故障注入）")
sys.path.insert(0, os.environ.get("APP_DIR", "/workspace"))
sys.path.insert(0, os.environ.get("PY_SITE", "/tmp/pyuser/lib/python3.11/site-packages"))
import sqlite3  # noqa: E402
from app.scaler import Scaler  # noqa: E402

fd, crash_db = tempfile.mkstemp(suffix=".db")
os.close(fd)
sc = Scaler(crash_db, gap_timeout_secs=TIMEOUT_SECS)
sc.update_roster(["a", "b"])
sc.submit_report(1, 1, "a", 25)  # 扩到 3，缺口 1
before = sc.get_gap()
check("故障注入前缺口已建立", before["gap_active"] and before["shortage"] == 1, str(before))

# 预置占用下一版号 2，使下一次名单更新的 INSERT 触发主键冲突 → 整事务回滚
admin = sqlite3.connect(crash_db, timeout=30)
admin.execute("PRAGMA journal_mode=WAL")
admin.execute("INSERT INTO rosters(version, pods, created_at) VALUES (2, '[]', ?)",
              ["2026-01-01T00:00:00+00:00"])
admin.commit()
admin.close()

raised = False
try:
    sc.update_roster(["a", "b", "c"])
except sqlite3.IntegrityError:
    raised = True
check("名单写入失败向上抛出", raised)

after = sc.get_gap()
check("写入失败后原缺口记录保留（同一起点、同一缺口）",
      after["gap_active"] and after["started_at"] == before["started_at"]
      and after["shortage"] == before["shortage"]
      and after["target"] == before["target"], str(after))
check("写入失败未产生碎片片段", len(sc.get_gap_history("all")["episodes"]) == 1)
check("写入失败未升版号", sc.get_roster()["roster_version"] == 1)

# 解除故障后可继续正常写入；换成同样 2 个 Pod 的新名单，缺口仍沿用原起点
admin = sqlite3.connect(crash_db, timeout=30)
admin.execute("DELETE FROM rosters WHERE version=2")
admin.commit()
admin.close()
sc.update_roster(["x", "y"])
g = sc.get_gap()
check("恢复后名单更新成功，版号推进且缺口沿用原起点",
      sc.get_roster()["roster_version"] == 2 and g["gap_active"]
      and g["started_at"] == before["started_at"]
      and g["shortage"] == before["shortage"], str(g))
os.unlink(crash_db)


# --------------------------------------- 场景 6：为重启准备一个进行中的缺口
print("场景 6：准备重启前的进行中缺口（目标上调、Pod 未补齐）")
v = roster(["n1", "n2"])["roster_version"]
report(v, 1, "n1", 25)  # 扩到 3，就绪 2 → 缺口 1
g_before = gap()
check("重启前缺口进行中", g_before["gap_active"] and g_before["shortage"] == 1, str(g_before))
restart_start = g_before["started_at"]
time.sleep(0.3)


# ------------------------------------------------ 场景 7：重启后沿用原起点
def restart_case():
    print("场景 7：进程重启后仍按原起点计时")
    if os.environ.get("GAP_TEST_RESTART") != "1":
        print("  [SKIP] 重启子场景（设置 GAP_TEST_RESTART=1 且由 run 脚本驱动时执行）")
        return
    import urllib.error
    db_path = os.environ["SCALER_DB"]
    port = int(os.environ.get("GAP_TEST_PORT", "8000"))
    # 先停掉当前服务释放端口（数据已在 SQLite 落盘）
    parent_pid = os.environ.get("GAP_TEST_SERVER_PID")
    if parent_pid:
        os.kill(int(parent_pid), signal.SIGTERM)
        for _ in range(50):
            try:
                urllib.request.urlopen(BASE + "/docs", timeout=1)
            except urllib.error.URLError:
                break
            time.sleep(0.1)
        time.sleep(0.5)
    site_packages = os.environ.get("PY_SITE", "/tmp/pyuser/lib/python3.11/site-packages")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=os.environ.get("APP_DIR", "/workspace"),
        env={**os.environ, "SCALER_DB": db_path,
             "GAP_TIMEOUT_SECONDS": str(TIMEOUT_SECS),
             "PYTHONPATH": site_packages},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True)
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(BASE + "/docs", timeout=2)
                break
            except OSError:
                time.sleep(0.2)
        g_after = gap()
        check("重启后缺口仍在且沿用原起点",
              g_after["gap_active"] and g_after["started_at"] == restart_start,
              f"before={restart_start} after={g_after['started_at']}")
        check("重启后已等时长持续累计（未清零）",
              g_after["elapsed_seconds"] >= g_before["elapsed_seconds"],
              str(g_after))
        check("重启后历史片段不丢失",
              len(history("all")["episodes"]) >= 1
              and history("all")["episodes"][0]["started_at"] == restart_start)
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=15)
        time.sleep(0.5)


restart_case()

print()
if FAILURES:
    print(f"失败 {len(FAILURES)} 项: {FAILURES}")
    sys.exit(1)
print("全部缺口场景通过 ✔")
