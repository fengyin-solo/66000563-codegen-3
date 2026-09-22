import asyncio, time, random, json, threading
from collections import defaultdict, deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0
MAIN_LOOP = None  # uvicorn 主事件循环，供 worker 线程推送 WebSocket

# ---- 熔断/重试公共规则（所有环节共用，对新建与已有工作流生效）----
DEFAULT_RULES = {"failureThreshold": 3, "cooldownSeconds": 10, "maxRetries": 3}
RULE_LIMITS = {
    "failureThreshold": (1, 10),    # 连续出错到达几次触发熔断
    "cooldownSeconds": (1, 300),    # 熔断冷却时间（秒）
    "maxRetries": (1, 10),          # 最多重试几次
}
RULE_FIELD_LABELS = {
    "failureThreshold": "熔断失败阈值",
    "cooldownSeconds": "熔断冷却时间",
    "maxRetries": "最大重试次数",
}
RULE_UNITS = {"failureThreshold": " 次", "cooldownSeconds": " 秒", "maxRetries": " 次"}
rules_lock = threading.Lock()
RULES = dict(DEFAULT_RULES)

# 独立 RNG 实例，便于测试时替换为确定性脚本
RNG = random.Random()


@app.on_event("startup")
def _capture_main_loop():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_event_loop()


def normalize_rules(raw):
    """旧设置兜底：字段缺失/类型不对/越界时逐项回退默认值；
    若组合后仍然矛盾（阈值 > 最大重试），把阈值收敛到合法值。"""
    rules = dict(DEFAULT_RULES)
    if isinstance(raw, dict):
        for key, (lo, hi) in RULE_LIMITS.items():
            v = raw.get(key)
            if isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi:
                rules[key] = v
        if rules["failureThreshold"] > rules["maxRetries"]:
            rules["failureThreshold"] = min(DEFAULT_RULES["failureThreshold"], rules["maxRetries"])
    return rules


def validate_rules(body):
    """保存前严格校验，错误按字段逐项返回，便于前端指出是哪一项有问题。"""
    errors = []
    if not isinstance(body, dict):
        return [{"field": "_root", "message": "规则必须是一个 JSON 对象"}]

    parsed = {}
    type_names = {bool: "布尔值", str: "文本", float: "小数"}
    for key, (lo, hi) in RULE_LIMITS.items():
        label = RULE_FIELD_LABELS[key]
        if key not in body:
            errors.append({"field": key, "message": f"缺少{label}"})
            continue
        v = body[key]
        if isinstance(v, bool) or not isinstance(v, int):
            tn = next((n for t, n in type_names.items() if isinstance(v, t)), "错误类型")
            errors.append({"field": key, "message": f"{label}必须是整数，不能是{tn}"})
            continue
        if not (lo <= v <= hi):
            errors.append({
                "field": key,
                "message": f"{label}超出合理范围，应为 {lo}~{hi}{RULE_UNITS[key]}（当前为 {v}）",
            })
            continue
        parsed[key] = v

    # 互相矛盾：阈值大于最大重试次数时，熔断后已没有重试名额去做半开试探，环节将永远无法恢复
    if "failureThreshold" in parsed and "maxRetries" in parsed:
        threshold, retries = parsed["failureThreshold"], parsed["maxRetries"]
        if threshold > retries:
            msg = (f"熔断失败阈值({threshold})不能大于最大重试次数({retries})："
                   f"熔断后需要消耗重试次数放开试探，阈值过大会导致环节永远无法恢复")
            errors.append({"field": "failureThreshold", "message": msg})
            errors.append({"field": "maxRetries", "message": msg})
    return errors


@app.get("/api/rules")
def get_rules():
    with rules_lock:
        snap = dict(RULES)
    return {
        "rules": snap,
        "defaults": dict(DEFAULT_RULES),
        "limits": {k: {"min": lo, "max": hi} for k, (lo, hi) in RULE_LIMITS.items()},
        "labels": RULE_FIELD_LABELS,
    }


@app.put("/api/rules")
async def update_rules(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail={"errors": [
            {"field": "_root", "message": "请求体不是合法的 JSON"}]})
    errors = validate_rules(body)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    with rules_lock:
        RULES.update({k: body[k] for k in DEFAULT_RULES})
        snap = dict(RULES)
    return {"rules": snap}


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"

class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


def generate_dag_workflow(name: str):
    """Create a realistic DAG pipeline"""
    nodes = [
        {"id": "extract", "name": "数据提取", "deps": [], "duration": 2.0},
        {"id": "validate", "name": "数据校验", "deps": ["extract"], "duration": 1.5},
        {"id": "clean_a", "name": "清洗分支A", "deps": ["validate"], "duration": 1.8},
        {"id": "clean_b", "name": "清洗分支B", "deps": ["validate"], "duration": 1.2},
        {"id": "transform", "name": "数据转换", "deps": ["clean_a"], "duration": 3.0},
        {"id": "enrich", "name": "数据增强", "deps": ["clean_a", "clean_b"], "duration": 2.0},
        {"id": "aggregate", "name": "聚合计算", "deps": ["transform", "enrich"], "duration": 2.5},
        {"id": "quality", "name": "质量检查", "deps": ["aggregate"], "duration": 1.0},
        {"id": "export_db", "name": "入库", "deps": ["quality"], "duration": 1.8},
        {"id": "export_report", "name": "报表生成", "deps": ["quality"], "duration": 2.2},
        {"id": "notify", "name": "通知", "deps": ["export_db", "export_report"], "duration": 0.5},
    ]
    positions = [
        (0, 0), (0, 1), (-1, 2), (1, 2), (-1, 3),
        (0.5, 3), (-0.3, 4), (-0.3, 5), (-1, 6), (0.5, 6), (-0.3, 7)
    ]
    for i, n in enumerate(nodes):
        n["x"] = positions[i][0] * 2.5 + 2.5
        n["y"] = positions[i][1] * 0.9
        n["status"] = "PENDING"
        n["retries"] = 0
        n["startTime"] = None
        n["endTime"] = None

    edges = []
    for n in nodes:
        for d in n["deps"]:
            edges.append([d, n["id"]])

    return {"nodes": [{
        "id": n["id"], "name": n["name"], "deps": n["deps"],
        "x": n["x"], "y": n["y"], "status": n["status"],
        "startTime": None, "endTime": None, "retries": n["retries"]
    } for n in nodes], "edges": edges, "durations": {n["id"]: n["duration"] for n in nodes}}


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    with rules_lock:
        rules = dict(RULES)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"], "rules": rules}


@app.post("/api/run")
def run_workflow(req: RunRequest):
    dag = generate_dag_workflow("workflow")
    # 规则快照：本次执行（含尚未开始的环节）全程按启动时的约束走完，
    # 执行期间即使公共规则被调整也不影响本次运行
    with rules_lock:
        rules = dict(RULES)
    t = threading.Thread(target=execute_workflow,
                         args=(dag, req.workers, req.strategy, req.workflowId, rules),
                         daemon=True)
    t.start()
    return {
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "rules": normalize_rules(rules),
        "completed": False, "aborted": False,
    }


def execute_workflow(dag, workers, strategy, workflow_id, rules):
    rules = normalize_rules(rules)  # 旧设置/异常快照按默认值兜底
    threshold = rules["failureThreshold"]
    cooldown_seconds = rules["cooldownSeconds"]
    max_retries = rules["maxRetries"]

    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)

    # BFS topological sort
    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    node_map = {n["id"]: n for n in nodes}
    logs = []
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0})
    running_tasks = {}
    completed = set()

    def broadcast(completed_flag=False, aborted_flag=False):
        payload = {
            "workflow": {"id": workflow_id, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-30:],
            "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
            "rules": rules,
            "completed": completed_flag,
            "aborted": aborted_flag,
        }
        text = json.dumps(payload, ensure_ascii=False)
        if MAIN_LOOP is not None:
            for ws in list(ACTIVE_CLIENTS):
                fut = asyncio.run_coroutine_threadsafe(ws.send_text(text), MAIN_LOOP)
                fut.add_done_callback(lambda f: f.exception())
        time.sleep(0.3)

    while ready or running_tasks:
        # Start tasks
        now = time.time()
        initial_ready = len(ready)
        checked = 0
        while ready and len(running_tasks) < workers and checked < initial_ready:
            tid = ready.popleft()
            checked += 1
            node = node_map[tid]
            cb = cb_state[tid]
            if cb["state"] == "OPEN":
                if now < cb["cooldownUntil"]:
                    ready.append(tid)  # 冷却中：放回队尾，等冷却结束由后端重新放开
                    continue
                # 冷却结束：进入半开，放开一次试探执行
                cb["state"] = "HALF_OPEN"
                logs.append({"taskId": tid, "status": "CIRCUIT_HALF_OPEN", "timestamp": now,
                             "message": f"冷却结束，放开试探 {node['name']}"})

            node["status"] = "RUNNING"
            node["startTime"] = now

            # Simulate task execution (random success/failure)
            will_fail = RNG.random() < 0.12  # 12% failure rate
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {"end_time": now + runtime, "will_fail": will_fail}
            logs.append({"taskId": tid, "status": "RUNNING", "timestamp": now,
                         "message": f"开始执行 {node['name']}"})

        # Check completed tasks
        now = time.time()
        finished = []
        for tid, info in running_tasks.items():
            if now < info["end_time"]:
                continue
            node = node_map[tid]
            cb = cb_state[tid]
            finished.append(tid)

            if info["will_fail"]:
                cb["failureCount"] += 1
                if node["retries"] < max_retries:
                    node["retries"] += 1
                    node["status"] = "PENDING"
                    ready.appendleft(tid)
                    if cb["state"] == "HALF_OPEN":
                        # 半开试探失败：重新熔断，等待下一轮冷却
                        cb["state"] = "OPEN"
                        cb["cooldownUntil"] = now + cooldown_seconds
                        logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                     "message": f"试探失败，重新熔断冷却 {cooldown_seconds}s"})
                    else:
                        logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                     "message": f"执行失败，准备第 {node['retries']}/{max_retries} 次重试"})
                        if cb["failureCount"] >= threshold:
                            cb["state"] = "OPEN"
                            cb["cooldownUntil"] = now + cooldown_seconds
                            logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now,
                                         "message": f"连续失败 {cb['failureCount']} 次达到阈值 {threshold}，熔断冷却 {cooldown_seconds}s"})
                else:
                    # 重试名额用尽：环节彻底失败（其下游不会被调度，工作流终止）
                    node["status"] = "FAILED"
                    node["endTime"] = now
                    logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                 "message": f"已重试 {node['retries']} 次仍失败，环节终止"})
            else:
                node["status"] = "SUCCESS"
                node["endTime"] = now
                completed.add(tid)
                # 正常跑完：失败计数清零，熔断器回到闭合
                cb["failureCount"] = 0
                cb["state"] = "CLOSED"
                cb["cooldownUntil"] = 0
                logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now,
                             "message": f"完成 {node['name']}"})
                for next_tid in adj[tid]:
                    in_degree[next_tid] -= 1
                    if in_degree[next_tid] == 0:
                        ready.append(next_tid)

        for tid in finished:
            del running_tasks[tid]

        all_done = len(completed) == len(nodes)
        stalled = not running_tasks and not ready and not all_done
        broadcast(all_done, stalled)
        if all_done or stalled:
            break


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)
