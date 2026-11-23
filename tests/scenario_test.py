#!/usr/bin/env python3
"""端到端场景测试：漏报扩容、换 Pod 后迟报、连续低负载、并发幂等。

用法: python3 tests/scenario_test.py [base_url]   默认 http://127.0.0.1:8000
仅依赖标准库。
"""
import json
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
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


def target():
    return call("GET", "/target")


# ---------------------------------------------------------------- 场景 0：/docs
print("场景 0：/docs 可用")
with urllib.request.urlopen(BASE + "/docs", timeout=15) as r:
    check("GET /docs 返回 200", r.status == 200)
with urllib.request.urlopen(BASE + "/openapi.json", timeout=15) as r:
    check("GET /openapi.json 返回 200", r.status == 200)

# ------------------------------------------------- 场景 1：漏报扩容（未报齐即扩）
print("场景 1：漏报时已知积压超出目标容量即扩容")
reset()
r = call("POST", "/roster", {"pods": ["p1", "p2", "p3"]})
v = r["roster_version"]
check("名单版本从 1 开始", v == 1, f"version={v}")
check("初始目标为 1", target()["target"] == 1)

r = call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "p1", "pending": 25})
check("第 1 轮仅 p1 上报（漏报 p2/p3）即扩容到 3", r["target"] == 3 and r["scaling"]["kind"] == "scale_up",
      f"target={r['target']} complete={r['round_complete']}")
check("扩容时该轮未报齐", r["round_complete"] is False)

r = call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "p2", "pending": 30})
check("p2 补报后已知积压 55 > 容量 30，再扩到 6", r["target"] == 6, f"target={r['target']}")

r = call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "p3", "pending": 0})
check("报齐后总积压 55 建议 6，与目标一致不再变", r["target"] == 6 and r["scaling"] is None,
      f"target={r['target']}")
ev = call("GET", "/events")["events"]
check("事件流含 2 次扩容", sum(1 for e in ev if e["kind"] == "scale_up") == 2)
check("调整原因可查", "容量" in target()["reason"], target()["reason"])

# 副本缺口：目标 6、就绪 3 → 缺口 3，进行中
g = call("GET", "/gap")
check("缺口查询返回目标/就绪/缺口且正在计时",
      g["target"] == 6 and g["ready"] == 3 and g["shortage"] == 3
      and g["gap_active"] is True and g["started_at"] is not None
      and g["timed_out"] is False, str(g))
check("缺口片段可在历史中查到",
      len(call("GET", "/gap/history?status=active")["episodes"]) == 1)

# ------------------------------ 场景 2：换 Pod 后迟报 / 过期名单 / 重报 / 并发
print("场景 2：换 Pod 后迟报、过期名单、重报（含并发）均不改变目标和累计")
reset()
v1 = call("POST", "/roster", {"pods": ["a", "b"]})["roster_version"]
call("POST", "/reports", {"roster_version": v1, "round": 1, "pod": "a", "pending": 5})
before = call("GET", "/state")

v2 = call("POST", "/roster", {"pods": ["q1", "q2"]})["roster_version"]
check("换 Pod 后版号递增", v2 == v1 + 1, f"v1={v1} v2={v2}")

r = call("POST", "/reports", {"roster_version": v1, "round": 1, "pod": "b", "pending": 99})
check("旧版号上报被忽略", r["outcome"] == "ignored", r["reason"])
r = call("POST", "/reports", {"roster_version": v2, "round": 5, "pod": "q1", "pending": 3})
check("新版号第 5 轮受理", r["outcome"] == "accepted")
r = call("POST", "/reports", {"roster_version": v2, "round": 2, "pod": "q2", "pending": 99})
check("迟到的旧轮（2 < 5）被忽略", r["outcome"] == "ignored", r["reason"])
r = call("POST", "/reports", {"roster_version": v2, "round": 5, "pod": "q1", "pending": 3})
check("重报被忽略", r["outcome"] == "ignored", r["reason"])
r = call("POST", "/reports", {"roster_version": v2, "round": 5, "pod": "stranger", "pending": 1})
check("名单外 Pod 被忽略", r["outcome"] == "ignored", r["reason"])

# 并发：16 个相同上报 + 8 个过期名单上报同时到达
def spam(_):
    return call("POST", "/reports", {"roster_version": v2, "round": 6, "pod": "q1", "pending": 1})
def stale(_):
    return call("POST", "/reports", {"roster_version": v1, "round": 9, "pod": "a", "pending": 999})
with ThreadPoolExecutor(max_workers=24) as pool:
    dup_results = list(pool.map(spam, range(16)))
    stale_results = list(pool.map(stale, range(8)))
check("并发重报仅 1 条受理", sum(1 for r in dup_results if r["outcome"] == "accepted") == 1,
      f"accepted={sum(1 for r in dup_results if r['outcome'] == 'accepted')}")
check("并发过期名单全部忽略", all(r["outcome"] == "ignored" for r in stale_results))
# 并发：同一轮两个不同 Pod 同时上报都应受理
with ThreadPoolExecutor(max_workers=2) as pool:
    pair = list(pool.map(lambda p: call("POST", "/reports",
                                        {"roster_version": v2, "round": 7, "pod": p, "pending": 2}),
                         ["q1", "q2"]))
check("并发不同 Pod 同轮都受理且报齐", all(r["outcome"] == "accepted" for r in pair)
      and pair[0]["round_complete"] != pair[1]["round_complete"])

t = target()
check("全程目标未变（仍为 1）", t["target"] == 1, f"target={t['target']}")
ev = call("GET", "/events")["events"]
check("无任何伸缩事件", all(e["kind"] == "roster_update" for e in ev))
st = call("GET", "/state")
check("累计状态未被污染（低负载累计为空）",
      st["low_load_streak"]["last_complete_round"] is None or
      st["low_load_streak"]["last_complete_round"] >= 5)

# ------------------------------------------------- 场景 3：连续低负载缩容
print("场景 3：连续两轮报齐且建议缩容才缩容，取两轮建议较大值")
reset()
v = call("POST", "/roster", {"pods": ["a", "b"]})["roster_version"]
call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "a", "pending": 15})
r = call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "b", "pending": 0})
check("先扩到 2（积压 15）", r["target"] == 2)
r = call("POST", "/reports", {"roster_version": v, "round": 2, "pod": "a", "pending": 5})
call("POST", "/reports", {"roster_version": v, "round": 2, "pod": "b", "pending": 3})
check("仅一轮低负载不缩容", target()["target"] == 2)
call("POST", "/reports", {"roster_version": v, "round": 3, "pod": "a", "pending": 2})
r = call("POST", "/reports", {"roster_version": v, "round": 3, "pod": "b", "pending": 2})
check("连续第二轮低负载后缩到 1", r["target"] == 1 and r["scaling"]["kind"] == "scale_down",
      f"target={r['target']}")

reset()
v = call("POST", "/roster", {"pods": ["w1", "w2", "w3", "w4", "w5"]})["roster_version"]
call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "w1", "pending": 50})
for p in ["w2", "w3", "w4", "w5"]:
    r = call("POST", "/reports", {"roster_version": v, "round": 1, "pod": p, "pending": 0})
check("先扩到 5（积压 50）", r["target"] == 5)
call("POST", "/reports", {"roster_version": v, "round": 2, "pod": "w1", "pending": 25})
for p in ["w2", "w3", "w4", "w5"]:
    call("POST", "/reports", {"roster_version": v, "round": 2, "pod": p, "pending": 0})
check("第 2 轮建议 3，单轮不缩容", target()["target"] == 5)
call("POST", "/reports", {"roster_version": v, "round": 3, "pod": "w1", "pending": 12})
for p in ["w2", "w3", "w4", "w5"]:
    r = call("POST", "/reports", {"roster_version": v, "round": 3, "pod": p, "pending": 0})
check("两轮建议 3、2，取较大值缩到 3", r["target"] == 3,
      f"target={r['target']} reason={r['scaling']['reason'] if r['scaling'] else None}")

# -------------------- 场景 4：就绪数≠目标禁止缩容；名单/目标变更后重新累计
print("场景 4：就绪数≠目标禁止缩容；变更后重新累计")
reset()
v = call("POST", "/roster", {"pods": ["a", "b"]})["roster_version"]
call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "a", "pending": 25})
r = call("POST", "/reports", {"roster_version": v, "round": 1, "pod": "b", "pending": 0})
check("扩到 3（就绪 2 ≠ 目标 3）", r["target"] == 3)
for rnd, pend in [(2, 5), (3, 3)]:
    call("POST", "/reports", {"roster_version": v, "round": rnd, "pod": "a", "pending": pend})
    call("POST", "/reports", {"roster_version": v, "round": rnd, "pod": "b", "pending": 0})
check("连续两轮低负载但就绪数≠目标，不缩容", target()["target"] == 3)

v = call("POST", "/roster", {"pods": ["a", "b", "c"]})["roster_version"]
for p in ["a", "b", "c"]:
    call("POST", "/reports", {"roster_version": v, "round": 4, "pod": p, "pending": 1})
check("换名单后累计清零，单轮低负载不缩容", target()["target"] == 3)
for p in ["a", "b", "c"]:
    r = call("POST", "/reports", {"roster_version": v, "round": 5, "pod": p, "pending": 1})
check("新名单下连续第二轮低负载，缩到 1", r["target"] == 1, f"target={r['target']}")

print()
if FAILURES:
    print(f"失败 {len(FAILURES)} 项: {FAILURES}")
    sys.exit(1)
print("全部场景通过 ✔")
