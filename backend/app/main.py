import asyncio, time, random, json, threading, os
from collections import defaultdict, deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0

# ---------------------------------------------------------------------------
# 熔断 / 重试规则（全平台各环节公共一套）
#
# failureThreshold : 同一环节连续出错达到几次后触发熔断
# cooldownSeconds  : 熔断后冷却多久，冷却结束由后端重新放开一次试探
# maxRetries       : 同一环节最多自动重试几次
#
# 取值范围（超出范围即视为“不合理”，不允许保存）：
#   failureThreshold : [1, 10] 的整数
#   cooldownSeconds  : [1, 600] 的整数（秒）
#   maxRetries       : [0, 10] 的整数
#
# 矛盾约束：failureThreshold <= maxRetries + 1
#   每个环节最多重试 maxRetries 次（即最多尝试 maxRetries + 1 次），
#   若熔断阈值比这还大，则熔断永远不可能被触发——彼此矛盾。
# ---------------------------------------------------------------------------
RULE_LIMITS = {
    "failureThreshold": {"min": 1, "max": 10},
    "cooldownSeconds": {"min": 1, "max": 600},
    "maxRetries": {"min": 0, "max": 10},
}
DEFAULT_RULES = {"failureThreshold": 3, "cooldownSeconds": 5, "maxRetries": 3}
RULE_FIELDS = ("failureThreshold", "cooldownSeconds", "maxRetries")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
RULES_FILE = os.path.join(DATA_DIR, "rules.json")

# 规则更新后加锁；正在执行的工作流持有启动时的规则快照，不受影响
_rules_lock = threading.Lock()
RULES = dict(DEFAULT_RULES)


def _is_int(value):
    # 显式拒绝 bool（isinstance(True, int) 为 True）
    return isinstance(value, int) and not isinstance(value, bool)


def _sanitize_rules(raw):
    """旧设置 / 残缺设置按默认值兜底，保证加载出来的规则一定合法。"""
    rules = dict(DEFAULT_RULES)
    if isinstance(raw, dict):
        for field in RULE_FIELDS:
            value = raw.get(field)
            lo, hi = RULE_LIMITS[field]["min"], RULE_LIMITS[field]["max"]
            if _is_int(value) and lo <= value <= hi:
                rules[field] = value
    # 旧设置可能出现“阈值 > 最大重试+1”的矛盾组合，整体回退默认值
    if rules["failureThreshold"] > rules["maxRetries"] + 1:
        rules = dict(DEFAULT_RULES)
    return rules


def load_rules():
    global RULES
    try:
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            RULES = _sanitize_rules(json.load(f))
    except (OSError, ValueError):
        # 文件不存在 / JSON 损坏：旧设置缺失，按默认值兜底
        RULES = dict(DEFAULT_RULES)


def validate_rules(raw):
    """逐字段校验，返回 (rules, errors)。errors 形如 {字段: 原因}。"""
    errors = {}
    rules = {}
    if not isinstance(raw, dict):
        return None, {"_": "请求体必须是 JSON 对象"}

    labels = {
        "failureThreshold": "熔断失败次数",
        "cooldownSeconds": "冷却时间",
        "maxRetries": "最大重试次数",
    }
    for field in RULE_FIELDS:
        value = raw.get(field)
        lo, hi = RULE_LIMITS[field]["min"], RULE_LIMITS[field]["max"]
        if not _is_int(value):
            errors[field] = f"{labels[field]}必须是整数"
        elif value < lo or value > hi:
            unit = " 秒" if field == "cooldownSeconds" else " 次"
            errors[field] = f"{labels[field]}超出合理范围（{lo}~{hi}{unit}）"
        else:
            rules[field] = value

    if not errors and rules["failureThreshold"] > rules["maxRetries"] + 1:
        errors["failureThreshold"] = (
            f"熔断失败次数({rules['failureThreshold']})不能大于"
            f"最大重试次数+1({rules['maxRetries'] + 1})，否则重试耗尽前熔断永远无法触发"
        )

    return (rules if not errors else None), errors


def rules_snapshot():
    """返回当前规则快照，供一次工作流执行从头用到结束。"""
    with _rules_lock:
        return dict(RULES)


@app.on_event("startup")
def _on_startup():
    load_rules()
    # 在启动事件里捕获主事件循环，供执行线程推送 WebSocket 更新
    app.state.loop = asyncio.get_event_loop()


@app.get("/api/rules")
def get_rules():
    return {"rules": rules_snapshot(), "limits": RULE_LIMITS, "defaults": DEFAULT_RULES}


@app.put("/api/rules")
async def update_rules(request: Request):
    try:
        raw = await request.json()
    except ValueError:
        raw = None
    rules, errors = validate_rules(raw)
    if errors:
        # 明确指出是哪一项有问题，不允许保存
        return JSONResponse(status_code=400, content={"ok": False, "errors": errors})

    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_file = RULES_FILE + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False)
    os.replace(tmp_file, RULES_FILE)
    with _rules_lock:
        RULES.clear()
        RULES.update(rules)
    # 注意：本次调整只影响之后新启动的工作流，已在跑的环节继续按旧快照走完
    return {"ok": True, "rules": rules, "limits": RULE_LIMITS, "defaults": DEFAULT_RULES}


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


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


@app.post("/api/run")
def run_workflow(req: dict):
    workers = int(req.get("workers", 3))
    strategy = str(req.get("strategy", "fifo"))
    workflow_id = int(req.get("workflowId", 0) or 0)
    dag = generate_dag_workflow("workflow")
    # 启动瞬间固定规则快照：规则之后被调整，本工作流仍按调整前的约束执行
    rules = rules_snapshot()
    t = threading.Thread(target=execute_workflow,
                         args=(dag, workers, strategy, workflow_id, rules), daemon=True)
    t.start()
    initial_breakers = [
        {"taskId": n["id"], "failureCount": 0, "state": "CLOSED", "cooldownUntil": 0}
        for n in dag["nodes"]
    ]
    return {
        "workflow": {"id": workflow_id, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [],
        "circuitBreakers": initial_breakers,
        "rules": rules,
        "completed": False
    }


def execute_workflow(dag, workers, strategy, workflow_id, rules):
    del strategy  # 调度策略不影响熔断/重试语义
    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)

    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    node_map = {n["id"]: n for n in nodes}
    logs = []

    failure_threshold = rules["failureThreshold"]
    cooldown_seconds = rules["cooldownSeconds"]
    max_retries = rules["maxRetries"]

    # 每个环节（节点）一个熔断器
    cb_state = {
        n["id"]: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0}
        for n in nodes
    }
    running_tasks = {}
    succeeded = set()
    failed = set()

    def make_payload(completed_flag=False):
        return {
            "workflow": {"id": workflow_id, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-50:],
            "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
            "rules": dict(rules),
            "completed": completed_flag
        }

    def send_update(completed_flag=False):
        payload = json.dumps(make_payload(completed_flag))
        loop = getattr(app.state, "loop", None)
        if loop is None:
            return
        for ws in list(ACTIVE_CLIENTS):
            try:
                asyncio.run_coroutine_threadsafe(ws.send_text(payload), loop)
            except Exception:
                pass

    def log(task_id, status, ts, message):
        logs.append({"taskId": task_id, "status": status, "timestamp": ts, "message": message})

    def pop_available(now):
        """从就绪队列取出当前可执行的环节；熔断冷却中的环节跳过但保留在队中，
        避免其堵住后面就绪的环节（队头阻塞 / 忙等）。"""
        for _ in range(len(ready)):
            tid = ready.popleft()
            cb = cb_state[tid]
            if cb["state"] == "OPEN":
                if now < cb["cooldownUntil"]:
                    ready.append(tid)  # 还在冷却，排到队尾等下一轮
                    continue
                # 冷却结束：由后端重新放开一次试探
                cb["state"] = "HALF_OPEN"
                log(tid, "CIRCUIT_HALF_OPEN", now,
                    f"冷却结束，熔断器进入半开，放试探性执行一次")
            return tid
        return None

    while ready or running_tasks:
        # 启动可执行任务
        while len(running_tasks) < workers:
            tid = pop_available(time.time())
            if tid is None:
                break
            node = node_map[tid]
            node["status"] = "RUNNING"
            node["startTime"] = node.get("startTime") or time.time()

            # 模拟环节执行（约 12% 失败率）
            will_fail = random.random() < 0.12
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {"end_time": time.time() + runtime, "will_fail": will_fail}
            log(tid, "RUNNING", time.time(), f"开始执行 {node['name']}")

        # 没有可执行任务（全部在熔断冷却中）时，推进到最近一个冷却结束点
        if not running_tasks and ready:
            next_open = min(cb_state[t]["cooldownUntil"] for t in ready)
            wait = next_open - time.time()
            if wait > 0:
                time.sleep(wait)
            # 冷却结束后由后端重新放开（下一轮 pop_available 中转为 HALF_OPEN）
            continue

        # 检查已结束的任务
        now = time.time()
        finished = []
        for tid, info in running_tasks.items():
            if now < info["end_time"]:
                continue
            node = node_map[tid]
            cb = cb_state[tid]
            finished.append(tid)

            if not info["will_fail"]:
                # 正常跑完：计数清零，熔断器关闭
                node["status"] = "SUCCESS"
                node["endTime"] = now
                succeeded.add(tid)
                reset_count = cb["failureCount"]
                cb["failureCount"] = 0
                cb["state"] = "CLOSED"
                cb["cooldownUntil"] = 0
                if reset_count:
                    log(tid, "CIRCUIT_CLOSED", now,
                        f"{node['name']} 执行成功，累计失败计数 {reset_count} 已清零，熔断器关闭")
                else:
                    log(tid, "SUCCESS", now, f"完成 {node['name']}")
                for next_tid in adj[tid]:
                    in_degree[next_tid] -= 1
                    if in_degree[next_tid] == 0:
                        ready.append(next_tid)
                continue

            # ---- 本次执行出错 ----
            cb["failureCount"] += 1

            # 重试次数已用完：环节终态失败（熔断器打开无实际意义，标记 FAILED）
            if node["retries"] >= max_retries:
                node["status"] = "FAILED"
                node["endTime"] = now
                failed.add(tid)
                cb["state"] = "FAILED"
                cb["cooldownUntil"] = 0
                log(tid, "FAILED", now,
                    f"{node['name']} 再次失败，已达最大重试次数 {max_retries}，环节失败")
                continue

            # 半开试探失败：无论计数多少都重新熔断，冷却时间重置
            if cb["state"] == "HALF_OPEN":
                node["status"] = "PENDING"
                node["retries"] += 1
                ready.appendleft(tid)
                cb["state"] = "OPEN"
                cb["cooldownUntil"] = now + cooldown_seconds
                log(tid, "CIRCUIT_OPEN", now,
                    f"半开试探失败，重新熔断，冷却 {cooldown_seconds} 秒")
            # 连续失败达到阈值：触发熔断，进入冷却，冷却结束前不再尝试
            elif cb["failureCount"] >= failure_threshold:
                node["status"] = "PENDING"
                node["retries"] += 1
                ready.appendleft(tid)
                cb["state"] = "OPEN"
                cb["cooldownUntil"] = now + cooldown_seconds
                log(tid, "FAILED", now,
                    f"{node['name']} 执行失败（重试 {node['retries']}/{max_retries}）")
                log(tid, "CIRCUIT_OPEN", now,
                    f"熔断触发：连续 {cb['failureCount']} 次失败，冷却 {cooldown_seconds} 秒")
            else:
                # 未达熔断阈值：按配置的最大重试次数继续重试
                node["status"] = "PENDING"
                node["retries"] += 1
                ready.appendleft(tid)
                log(tid, "FAILED", now,
                    f"{node['name']} 执行失败，重试 {node['retries']}/{max_retries}")

        for tid in finished:
            del running_tasks[tid]

        send_update()

        # 所有非失败环节都已跑完（失败环节下游永远不会就绪）
        if not ready and not running_tasks:
            break
        if len(succeeded) + len(failed) == len(nodes):
            break

        time.sleep(0.3)

    send_update(True)


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
