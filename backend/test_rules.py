"""后端行为测试：规则校验、默认兜底、熔断/冷却/半开试探/计数清零、规则快照。"""
import sys, os, time, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from fastapi.testclient import TestClient
import main
import random

passed = 0
def check(name, cond, detail=""):
    global passed
    assert cond, f"FAIL: {name} {detail}"
    passed += 1
    print(f"  ✓ {name}")

client = TestClient(main.app)
_orig_generate = main.generate_dag_workflow

# ---------- 1. 默认规则 ----------
r = client.get("/api/rules").json()
check("默认值 3/10/3", r["rules"] == {"failureThreshold": 3, "cooldownSeconds": 10, "maxRetries": 3})
check("返回范围与标签", "failureThreshold" in r["limits"] and "failureThreshold" in r["labels"])

# ---------- 2. 校验：越界 / 类型错误 / 矛盾，逐字段指出 ----------
def put(body):
    return client.put("/api/rules", json=body)

resp = put({"failureThreshold": 0, "cooldownSeconds": 10, "maxRetries": 3})
check("阈值=0 拒绝并指出字段", resp.status_code == 400 and
      {e["field"] for e in resp.json()["detail"]["errors"]} == {"failureThreshold"})

resp = put({"failureThreshold": 3, "cooldownSeconds": 99999, "maxRetries": 3})
check("冷却时间越界拒绝", resp.status_code == 400 and
      {e["field"] for e in resp.json()["detail"]["errors"]} == {"cooldownSeconds"})

resp = put({"failureThreshold": 3, "cooldownSeconds": 10, "maxRetries": 2})
errs = resp.json()["detail"]["errors"]
check("阈值>重试次数 矛盾拒绝", resp.status_code == 400)
check("矛盾同时指出两项", {e["field"] for e in errs} == {"failureThreshold", "maxRetries"})
check("错误信息说明矛盾原因", "永远无法恢复" in errs[0]["message"])

resp = put({"failureThreshold": True, "cooldownSeconds": "10", "maxRetries": 1.5})
fields = {e["field"] for e in resp.json()["detail"]["errors"]}
check("布尔/字符串/小数都拒绝并逐项指出", resp.status_code == 400 and
      fields == {"failureThreshold", "cooldownSeconds", "maxRetries"})

resp = put({"cooldownSeconds": 10, "maxRetries": 3})
check("缺字段拒绝", resp.status_code == 400 and
      "failureThreshold" in {e["field"] for e in resp.json()["detail"]["errors"]})

resp = put({"failureThreshold": 2, "cooldownSeconds": 5, "maxRetries": 4})
check("合法规则保存成功（阈值<=重试）", resp.status_code == 200 and
      resp.json()["rules"]["failureThreshold"] == 2)

# 非法 JSON
resp = client.put("/api/rules", content=b"not-json", headers={"content-type": "application/json"})
check("非法 JSON 返回 400", resp.status_code == 400)

# ---------- 3. 旧设置兜底 ----------
check("None 兜底默认", main.normalize_rules(None) == main.DEFAULT_RULES)
n = main.normalize_rules({"failureThreshold": 99, "cooldownSeconds": None, "maxRetries": 3})
check("越界/错误类型字段逐项兜底",
      n["failureThreshold"] == 3 and n["cooldownSeconds"] == 10 and n["maxRetries"] == 3)
n = main.normalize_rules({"failureThreshold": 5, "maxRetries": 2})
check("矛盾旧设置收敛阈值", n["failureThreshold"] <= n["maxRetries"] and n["failureThreshold"] == 2)

# ---------- 4. 引擎确定性脚本测试 ----------
def single_node_dag():
    dag = _orig_generate("t")
    dag["nodes"] = [dag["nodes"][0]]
    dag["edges"] = []
    dag["durations"] = {"extract": 0.01}
    return dag

class ScriptedRandom:
    def __init__(self, script): self.script, self.i = script, 0
    def random(self):
        fail = self.script[min(self.i, len(self.script) - 1)]
        self.i += 1
        return 0.0 if fail else 0.99  # <0.12 失败, >=0.12 成功

def run_scripted(script, rules):
    orig_random, orig_sleep = main.RNG, main.time.sleep
    main.RNG = ScriptedRandom(script)
    main.time.sleep = lambda *_: None
    dag = single_node_dag()
    try:
        main.execute_workflow(dag, 1, "fifo", 1, dict(rules))
    finally:
        main.RNG, main.time.sleep = orig_random, orig_sleep
    return dag["nodes"][0]

# 4a. 失败2次熔断 → 冷却 → 半开放开 → 成功，计数清零
node = run_scripted([True, True, False],
                    {"failureThreshold": 2, "cooldownSeconds": 1, "maxRetries": 4})
check("熔断→冷却→半开→成功：SUCCESS", node["status"] == "SUCCESS", node["status"])
check("成功时重试计数为2（熔断后试探也算一次尝试）", node["retries"] == 2, node["retries"])

# 4b. 重试名额耗尽 → FAILED（阈值2、重试2：第3次半开试探再失败即终止）
node2 = run_scripted([True, True, True],
                     {"failureThreshold": 2, "cooldownSeconds": 1, "maxRetries": 2})
check("重试耗尽：节点 FAILED", node2["status"] == "FAILED", node2["status"])
check("重试次数不超过上限", node2["retries"] == 2, node2["retries"])

# 4c. 一次成功，不熔断不重试
node3 = run_scripted([False], {"failureThreshold": 2, "cooldownSeconds": 1, "maxRetries": 2})
check("一次成功 SUCCESS 且无重试", node3["status"] == "SUCCESS" and node3["retries"] == 0)

# 4d. 阈值1：第一次失败即熔断，冷却后半开成功
node4 = run_scripted([True, False], {"failureThreshold": 1, "cooldownSeconds": 1, "maxRetries": 3})
check("阈值1首次失败即熔断，半开后恢复", node4["status"] == "SUCCESS" and node4["retries"] == 1)

# ---------- 5. 规则快照：执行期间全局规则变化，本次运行不受影响 ----------
client.put("/api/rules", json={"failureThreshold": 2, "cooldownSeconds": 2, "maxRetries": 2})
dag = single_node_dag()
snapshot = {"failureThreshold": 5, "cooldownSeconds": 7, "maxRetries": 6}
orig_sleep = main.time.sleep
main.time.sleep = lambda *_: None
orig_rng = main.RNG
main.RNG = ScriptedRandom([False])
try:
    main.execute_workflow(dag, 1, "fifo", 99, snapshot)
finally:
    main.time.sleep = orig_sleep
    main.RNG = orig_rng
check("执行线程不改动全局规则",
      client.get("/api/rules").json()["rules"] ==
      {"failureThreshold": 2, "cooldownSeconds": 2, "maxRetries": 2})

# ---------- 6. 端到端 WebSocket：OPEN 帧带 cooldownUntil，规则快照随帧下发，成功后清零 ----------
def _single(name): return single_node_dag()
main.generate_dag_workflow = _single
main.RNG = ScriptedRandom([True, True, False])
main.time.sleep = lambda *_: None
try:
    with TestClient(main.app) as ac:  # startup 事件捕获主事件循环
        client.put = ac.put  # noqa
        ac.put("/api/rules", json={"failureThreshold": 2, "cooldownSeconds": 1, "maxRetries": 4})
        with ac.websocket_connect("/ws") as ws:
            wf = ac.post("/api/workflow", json={"name": "ws"}).json()
            check("新建工作流带当前公共规则",
                  wf["rules"] == {"failureThreshold": 2, "cooldownSeconds": 1, "maxRetries": 4})
            ac.post("/api/run", json={"workflowId": wf["id"], "workers": 1})
            frames, deadline = [], time.time() + 15
            while time.time() < deadline:
                frames.append(json.loads(ws.receive_text()))
                if frames[-1]["completed"] or frames[-1]["aborted"]:
                    break
finally:
    main.generate_dag_workflow = _orig_generate
    main.RNG = random.Random()
    main.time.sleep = time.sleep

check("收到执行推送帧", len(frames) > 0)
check("每帧带规则快照",
      all(f["rules"] == {"failureThreshold": 2, "cooldownSeconds": 1, "maxRetries": 4} for f in frames))
open_frames = [f for f in frames if any(cb["state"] == "OPEN" for cb in f["circuitBreakers"])]
check("出现熔断帧且带 cooldownUntil", len(open_frames) > 0 and
      all(cb["cooldownUntil"] > 0 for f in open_frames for cb in f["circuitBreakers"] if cb["state"] == "OPEN"))
half_open_seen = {cb["state"] for f in frames for cb in f["circuitBreakers"]}
check("经历 HALF_OPEN 试探", "HALF_OPEN" in half_open_seen, half_open_seen)
last = frames[-1]
check("最终完成且失败计数清零、熔断器闭合",
      last["completed"] and
      last["circuitBreakers"][0]["failureCount"] == 0 and
      last["circuitBreakers"][0]["state"] == "CLOSED" and
      last["workflow"]["nodes"][0]["status"] == "SUCCESS",
      json.dumps(last["circuitBreakers"], ensure_ascii=False))

print(f"\n全部 {passed} 项检查通过 ✅")
