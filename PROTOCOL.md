# 客户端同步协议 v1

> SQLite Event Log —— **event-log-sync-protocol v1** —— 多端同步事件流协议。
> 本文档面向实现客户端 SDK 的工程师。
> 配套服务代码：`app.py` · 服务端测试：`test_app.py`。

---

## 1. 模型总览

服务端是一个 **append-only 事件流**，针对一个 `topic` 维护一串有序事件。事件由任意设备 `POST` 写入，永不修改、永不删除。客户端通过 `GET` 拉取自己错过的窗口，本地折叠成实体当前状态。

```
┌──────────┐  POST /events  ┌──────────┐
│ Device A │ ─────────────▶ │          │
└──────────┘                │  Server  │
┌──────────┐  POST /events  │ (SQLite) │
│ Device B │ ─────────────▶ │          │
└──────────┘                └─────┬────┘
┌──────────┐    GET /events        │
│ Device C │ ◀─────────────────────┘
└──────────┘
            (本地折叠成实体当前状态)
```

### 1.1 关键不变量

| # | 不变量 | 含义 |
|---|--------|------|
| 1 | 事件不可变 | 已写入的事件永远不会被服务端修改或删除 |
| 2 | `recorded_time` 单调递增 | 同主题内服务端入库时间严格递增 |
| 3 | 重复写入幂等 | 同 `(topic, device_id, event_time, entity_id, action)` 的请求返回同一记录 |
| 4 | 客户端是状态权威 | 服务端不折叠，客户端拉到自己本地合成当前状态 |

### 1.2 术语

| 术语 | 含义 |
|------|------|
| **topic** | 一组相关实体的命名空间，类似 Kafka topic（无分区） |
| **entity** | 一条业务数据，由 `entity_id` 唯一标识 |
| **event** | 对一个 entity 的一次变更（create/update/delete） |
| **recorded_time** | 服务端入库时间（UTC ISO-8601，毫秒精度 + `Z`） |
| **event_time** | 客户端声称的事件发生时间，原样保存，不参与同步判断 |
| **sync_point** | 客户端本地记录的"已经处理到的 `recorded_time`" |

---

## 2. 鉴权

所有请求（`POST /events` 与 `GET /events`）必须带：

```
Authorization: <EVENT_LOG_SECRET>
```

值与服务端环境变量 `EVENT_LOG_SECRET` 完全一致（字符串相等）。缺失或不匹配 → `401`。

---

## 3. 事件模型

### 3.1 事件字段

| 字段 | 类型 | 谁写 | 说明 |
|------|------|------|------|
| `id` | string (16 hex) | 服务端 | sha256(`topic|device_id|event_time|entity_id|action`) 前 16 位 |
| `topic` | string | 客户端 | 不传则服务端用 `__default__` |
| `recorded_time` | string (ISO-8601 UTC) | 服务端 | 入库瞬间时间戳，永不修改 |
| `event_time` | string (ISO-8601) | 客户端 | 客户端声称的发生时间，原样保存 |
| `device_id` | string | 客户端 | 发起写入的设备 ID |
| `entity_id` | string | 客户端 | 实体 ID；同一 topic 内应全局唯一 |
| `action` | enum | 客户端 | `create` \| `update` \| `delete` |
| `data` | object \| null | 客户端 | 实体全量快照；`delete` 时为 `null` |

### 3.2 action 语义

| action | `data` 约束 | 含义 |
|--------|------------|------|
| `create` | 必填，对象 | 实体首次出现，data 是完整初始状态 |
| `update` | 必填，对象 | 实体内容变更，data 是变更后完整状态（全量替换） |
| `delete` | 必须为 `null` | 实体被删除（tombstone）；保留在事件流中 |

> **全量更新模型**：每次 `update` 都携带 entity 的**完整当前状态**，不是 diff。客户端无需 diff 计算。

---

## 4. API

### 4.1 `POST /events`

写入一条事件。重复请求（由 id 决定）服务端返回原记录而非新增。

**请求：**

```http
POST /events
Authorization: <secret>
Content-Type: application/json

{
  "device_id":  "phone-1",
  "topic":      "notes",
  "entity_id":  "note-42",
  "action":     "create",
  "event_time": "2026-09-03T12:00:00.123Z",
  "data": {
    "title": "Hello",
    "body":  "First sync"
  }
}
```

**字段校验：**

| 缺失/错误 | 状态码 |
|-----------|--------|
| 任何必填字段（`device_id`/`entity_id`/`action`/`event_time`）缺失或空串 | `400` |
| `action` 不在 `{create, update, delete}` | `400` |
| `action ∈ {create, update}` 但 `data` 缺失 | `400` |
| `action = delete` 但 `data` 非 null | 服务端**静默忽略** data 并置为 null |

**响应：**

| 状态码 | 含义 | 何时 |
|--------|------|------|
| `201 Created` | 新事件已写入 | id 首次出现 |
| `200 OK` | 幂等命中，返回原记录 | id 已存在（重复请求） |
| `400 Bad Request` | 字段错误 | 见上 |
| `401 Unauthorized` | 鉴权失败 | Authorization 不匹配 |

**响应体：**

```json
{
  "id":            "228703dbe5e78fec",
  "topic":         "notes",
  "recorded_time": "2026-09-03T14:53:26.652190Z",
  "event_time":    "2026-09-03T12:00:00.123Z",
  "device_id":     "phone-1",
  "entity_id":     "note-42",
  "action":        "create",
  "data":          { "title": "Hello", "body": "First sync" }
}
```

### 4.2 `GET /events`

按时间窗拉取事件。

**请求：**

```http
GET /events?since=<recorded_time>&topic=<name>&limit=<N>
Authorization: <secret>
```

| 参数 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `since` | 否 | `""`（即拉最早开始所有） | 严格大于此 `recorded_time` 的事件 |
| `topic` | 否 | 不过滤 | 只返回该 topic 的事件 |
| `limit` | 否 | `1000`，最大 `10000` | 返回条数上限 |

**响应：**

`200 OK`，body 为数组，按 `recorded_time` **升序**：

```json
[
  { "id": "...", "recorded_time": "...", ... },
  { "id": "...", "recorded_time": "...", ... }
]
```

**响应状态：**

| 状态码 | 含义 |
|--------|------|
| `200 OK` | 成功（可能为空数组） |
| `400 Bad Request` | `limit` 非整数 |
| `401 Unauthorized` | 鉴权失败 |

---

## 5. 客户端同步协议

### 5.1 本地状态

每个 topic 客户端维护：

```text
sync_point       : string | null    // 已处理到的 max(recorded_time)
entities         : map<entity_id, EntityState>
EntityState      = {
  last_event     : Event,
  last_event_idx : int              // 在事件序列里的下标（可选，用于调试）
}
```

> `sync_point` 持久化到本地存储。客户端每次启动都加载；`null` 表示从未同步过。

### 5.2 冷启动（cold start）

本地 `sync_point == null` 时执行：

```
function cold_start(topic):
    events = GET /events?since=&topic={topic}&limit=10000
    while len(events) == 10000:                    # 可能分页
        events += GET /events?since={events[-1].recorded_time}&topic={topic}&limit=10000
    apply_events(events)                           # 见 §5.4
    sync_point = max(recorded_time for e in events) or sync_point
    persist(sync_point, entities)
```

> 不分页实现：循环直到返回少于 limit 条为止。所有事件一次性折叠到内存。

### 5.3 增量同步

本地已有 `sync_point` 时：

```
function sync(topic):
    new_events = []
    while True:
        batch = GET /events?since={sync_point}&topic={topic}&limit=10000
        new_events += batch
        if len(batch) < 10000: break
        sync_point = batch[-1].recorded_time       # 推进游标避免内存爆炸
    apply_events(new_events)                       # 见 §5.4
    if new_events:
        sync_point = max(e.recorded_time for e in new_events)
    persist(sync_point, entities)
```

> 循环里推进 `sync_point` 是为了让长拉取不至于一次性把服务端全部历史吃进内存。

### 5.4 本地折叠（apply_events）

```
function apply_events(events):
    for e in events:
        cur = entities.get(e.entity_id)
        if cur is None:
            # 首次见这个 entity
            if e.action == "delete":
                pass                              # 删除先于任何 create，忽略
            else:
                entities[e.entity_id] = EntityState(last_event=e)
        else:
            if e.recorded_time > cur.last_event.recorded_time:
                if e.action == "delete":
                    del entities[e.entity_id]     # tombstone 生效
                else:
                    entities[e.entity_id] = EntityState(last_event=e)
            # 否则旧事件被忽略（不变量：服务端保证拉到的序列是单调的，
            # 但客户端可能因为冷启动后再增量拉，重复处理一遍，需要这条判断）
```

**冲突策略：** Last-Write-Wins by `recorded_time`（服务端时间）。同一 `entity_id` 的多个事件，取 `recorded_time` 最大者。

> `event_time` 不参与判断。即使客户端时钟错乱，只要服务端入库顺序稳定，最终一致。

### 5.5 本地变更写回（put）

客户端本地状态发生变更时：

```
function put(topic, entity_id, new_state | None):
    # new_state is None 表示删除
    action = "delete" if new_state is None else ("update" if entity_id in entities else "create")
    event = POST /events {
        device_id:  <this_device>,
        topic:      topic,
        entity_id:  entity_id,
        action:     action,
        event_time: <client clock now, ISO-8601>,
        data:       new_state
    }
    # 立即本地应用：保证 UI 立即反映，且与服务端响应一致
    apply_events([event])
    sync_point = max(sync_point, event.recorded_time)
    persist(...)
```

> **建议：** `put` 成功后立即调用 `apply_events` 而不是等下一次 sync。否则 UI 要等到下次 `GET` 才看到自己刚写的内容。

### 5.6 周期性同步

客户端应周期性（或事件驱动：联网恢复、前后台切换）执行 §5.3。

---

## 6. 错误处理

| 情况 | 客户端应做 |
|------|------------|
| `401` | 密钥错误，停止同步，上报用户（不应重试） |
| `400` | 请求本身非法，记录日志，不重试 |
| `5xx` / 网络错误 | 指数退避重试，`sync_point` 不前进 |
| 服务端时钟回拨 | 不会出现（见不变量 2）；若出现应以服务端响应为准 |

---

## 7. 完整流程示例

假设 topic = `notes`，三个设备。

```
T0  DeviceA 冷启动:
    GET /events?since=&topic=notes   → []
    sync_point = null (没有事件)

T1  DeviceA 写入 note-1:
    POST /events {device:A, entity:note-1, action:create,
                  event_time:T1, data:{title:"A1"}}
    ← 201 {id:01..., recorded_time:R1, ...}
    本地: entities[note-1] = {data:{title:"A1"}}, sync_point=R1

T2  DeviceB 冷启动:
    GET /events?since=&topic=notes   → [event01]
    apply → entities[note-1] = {data:{title:"A1"}}
    sync_point=R1

T3  DeviceB 改 note-1:
    POST /events {device:B, entity:note-1, action:update,
                  event_time:T3, data:{title:"B-edit", body:"added"}}
    ← 201 {id:02..., recorded_time:R3, ...}
    本地: entities[note-1] = {data:{title:"B-edit", body:"added"}}, sync_point=R3

T4  DeviceA 同步:
    GET /events?since=R1&topic=notes → [event02]
    apply → recorded_time R3>R1 → entities[note-1] = {data:{title:"B-edit", body:"added"}}
    sync_point=R3

T5  DeviceC 冷启动（中间加入）:
    GET /events?since=&topic=notes   → [event01, event02]
    apply → entities[note-1] = {data:{title:"B-edit", body:"added"}}
    sync_point=R3
```

---

## 8. 不保证 / 限制

| 项 | 说明 |
|----|------|
| 事件 GC | 不支持。事件流只增。客户端可按 topic 自行删除本地缓存 |
| 服务端折叠 | 不提供。客户端必须自己按 entity_id 折叠 |
| `event_time` 一致性 | 不校验。客户端时钟漂移不会被纠正 |
| 分页边界 | `since=<last_recorded_time>` 在循环中可能漏掉同毫秒事件；详见附录 A |
| 多 topic | 每次同步一个 topic；如需跨 topic 请在客户端循环 |
| 鉴权强度 | 仅共享密钥；无 per-device 区分、无传输加密（生产请加 TLS） |

---

## 附录 A：分页边界处理

服务端 `recorded_time` 精度为微秒（`%f`），实际并发写同微秒内可能仍有冲突（极小概率）。客户端循环拉取的标准做法：

```
cursor = sync_point
while True:
    batch = GET ?since={cursor}&limit=N
    if not batch: break
    apply_events(batch)
    cursor = batch[-1].recorded_time
    if len(batch) < N: break
```

注意：使用 `batch[-1].recorded_time` 作为下一轮 `since` 时，**严格大于**比较保证不会丢事件，但可能将同一 `recorded_time` 的事件算两次。`apply_events` 的 LWW 判断会保证最终一致（重复事件会被丢弃）。

如需严格不重复，可在响应中加 `cursor-token` 服务端字段；当前 v1 不提供。

---

## 附录 B：客户端 SDK 最小接口（建议）

```python
class EventLogClient:
    def __init__(self, base_url: str, secret: str, device_id: str): ...
    def cold_start(self, topic: str) -> None: ...
    def sync(self, topic: str) -> int: ...      # 返回新事件数
    def put(self, topic: str, entity_id: str,
            state: dict | None) -> Event: ...   # None 表示 delete
    def get(self, topic: str, entity_id: str) -> dict | None: ...
    def list(self, topic: str) -> dict[str, dict]: ...
    @property
    def sync_point(self, topic: str) -> str | None: ...
```

> 各语言实现可基于此接口生成。`sync_point` 持久化由 SDK 调用方决定（文件/SQLite/Preferences）。
