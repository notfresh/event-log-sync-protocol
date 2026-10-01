# 客户端同步协议 v1

> Event Log —— **event-log-sync-protocol v1** —— 多端同步事件流协议。
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
| 1 | 事件不可变 | 已写入的事件永远不会被修改。但允许**部署级**显式清空（如离线客户端 UI 提供清空按钮）。详见 §10.1 |
| 2 | `process_time` 在同 topic 内单调递增 | 多端场景下服务端保证严格递增；单端场景下允许同毫秒内产生相同 `process_time` |
| 3 | 重复写入幂等 | 同 `(topic, device_id, event_time, entity_id, action)` 的请求返回同一记录（id 公式稳定） |
| 4 | 客户端是状态权威 | 服务端不折叠，客户端拉到自己本地合成当前状态 |

### 1.2 术语

| 术语 | 含义 |
|------|------|
| **topic** | 一组相关实体的命名空间，类似 Kafka topic（无分区） |
| **entity** | 一条业务数据，由 `entity_id` 唯一标识 |
| **event** | 对一个 entity 的一次变更（create/update/delete） |
| **process_time** | 本次写入的处理时刻（UTC ISO-8601，毫秒精度 + `Z`）。多端场景下由服务端入库时覆盖；单端场景下为客户端调用时刻。 |
| **event_time** | 客户端声称的事件发生时间，原样保存。多端场景下不参与同步判断；单端场景下兼做翻页游标。 |
| **sync_point** | 客户端本地记录的"已经处理到的 `process_time`"。多端场景下使用；单端场景下可省略，改用 `event_time` 推进游标。 |

---

## 2. 鉴权

所有请求（`POST /events` 与 `GET /events`）必须带：

```
Authorization: <EVENT_LOG_SECRET>
```

值与服务端环境变量 `EVENT_LOG_SECRET` 完全一致（字符串相等）。缺失或不匹配 → `401`。

---

## 2.5 时间字段设计（重要）

> **在阅读 §3 字段表之前，请先读完本节。** 本节是事件流的核心设计决策之一：两个时间字段的语义区分。

事件流协议里有**两个时间字段**：`event_time` 和 `process_time`。它们**职责清晰、不重复、不混淆**。

### 核心定义

| 字段 | 一句话 | 时区 |
|------|--------|------|
| `event_time` | **实体真实创建时间**——业务事件"实际发生的时刻" | **本地时区**（ISO-8601 带 `+HH:MM` 偏移） |
| `process_time` | **日志生成时刻**——这条事件"被写入事件流"的时刻 | **UTC**（ISO-8601 带 `Z`） |

### 为什么需要两个字段？

业务调用方往往**延迟**写日志：
- 离线模式：用户操作 → 设备重启 → 联网后才补录
- 批处理：累积 N 条变更 → 一次性 POST
- 缓存：UI 即时响应 → 后台异步落盘

延迟写日志时，"事件实际发生的时刻"和"日志写入的时刻"不一致。两个字段分别记录这两个时刻：

```
event_time  = 实体"出生证"——这条事件对应的真实世界动作发生在何时
process_time = 日志"签发时间"——这条记录被写入事件流的时刻
```

### 时区选择

| 字段 | 时区 | 理由 |
|------|------|------|
| `event_time` | **本地时区**（如 `+08:00`） | 用户阅读本地时间友好；反映用户感知的事件时间 |
| `process_time` | **UTC**（`Z`） | 服务端/跨端一致性；避免时区转换歧义 |

### 三个典型场景

#### 场景 A：同步调用

用户在 `14:23:25.123 (本地时间)` 保存一条 link，立即触发 `EventLogClient.create`：

```
event_time   = 2026-09-03T14:23:25.123+08:00   ← 本地时区
process_time = 2026-09-03T06:23:25.123Z        ← UTC（同一时刻）
```

两者数值上对应同一物理时刻，**但格式不同**——分别带 `+08:00` 和 `Z`。

#### 场景 B：离线后补录

用户在 `14:23:25 (本地时间)` 保存 link 时设备离线；`2026-09-04T10:00:00Z` 设备联网后补录：

```
event_time   = 2026-09-03T14:23:25.123+08:00   ← 实体真实创建时间（不变）
process_time = 2026-09-04T10:00:00.000Z        ← 补录时刻（晚于 event_time）
```

`event_time` 反映业务实际时间，`process_time` 反映日志实际写入时间——**两者明显不同**。

#### 场景 C：bootstrap 灌历史

设备首次接入协议栈，把本地已有的 N 条 link 作为 CREATE 事件写入事件流：

```
对每条 link:
  event_time   = links.timestamp（link 当时保存的时刻，本地时区）
  process_time = now()（本次 bootstrap 启动时刻，UTC）
```

`event_time` 是 link 自己的"出生证"，`process_time` 是 bootstrap 这次批量灌入的"签发时间"——**所有 link 共用一个 process_time，event_time 各不相同**。

### 与翻页游标的关系

单端场景下翻页游标**只用 `event_time`**：
- bootstrap 灌历史时 `event_time = links.timestamp`，各 link 不同 → 游标稳定推进
- 业务调用时 `event_time = entity 创建时刻`，正常情况下不同操作时间不同 → 游标能区分

`process_time` 不参与翻页（bootstrap 时全相同会导致游标卡死——见 §10.1）。

### 与协议核心设计原则的关系

| 原则 | event_time 体现 | process_time 体现 |
|------|-----------------|-------------------|
| 事件不可变 | ✓ 原样保存，不修改 | ✓ 写入时刻一次性确定 |
| 服务端权威 | ⚠ 多端场景下不修改；单端下等于客户端调用时刻 | ✓ 多端场景下服务端可覆盖 |
| 客户端是状态权威 | ✓ 实体的真实时间 | ⚠ 客户端控制写入时刻 |

### SDK 实现要点

客户端 SDK 必须在写入事件时**由调用方传入 `event_time`**（不能 SDK 自己用 `now()`）：

```java
// 正确
EventLogClient.get().create(
    topic, entityId,
    entity.getCreationTimeMillis(),  // ← 调用方传入实体的真实创建时间
    dataJson
);

// 错误
EventLogClient.get().create(topic, entityId, dataJson);  // SDK 默认 now() 会丢失语义
```

`process_time` 由 SDK 内部自动填 `now()`（或在多端场景下由服务端覆盖），调用方不应传入。

详见 §3.3 字段表与 §10.2 单端场景。

---

## 3. 事件模型

### 3.1 事件字段

| 字段 | 类型 | 谁写 | 说明 |
|------|------|------|------|
| `id` | string (16 hex) | 写入方 | sha256(`topic|device_id|event_time|entity_id|action`) 前 16 位 |
| `topic` | string | 客户端 | 不传则服务端用 `__default__` |
| `process_time` | string (ISO-8601 UTC, `Z` 后缀) | 写入方 | 本次写入的处理时刻（**始终 UTC**）；多端场景下服务端覆盖为入库时间，单端场景下为客户端调用时刻。永不修改。 |
| `event_time` | string (ISO-8601 本地时区偏移) | 客户端 | 客户端声称的**实体真实创建时间**（**本地时区**，带 `+HH:MM` 偏移），原样保存 |
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

### 3.3 event_time 与 process_time 的区分

两个时间字段在协议里有清晰区分：

| 字段 | 含义 | 时区 | 取值 |
|------|------|------|------|
| `event_time` | **实体真实创建时间** | **本地时区**（ISO-8601 带 `+HH:MM` 偏移） | 业务操作触发时由调用方传入；bootstrap 灌历史时取实体原时间戳 |
| `process_time` | **日志生成时刻**（写入方处理时刻） | **UTC**（ISO-8601 带 `Z`） | 写入方处理时刻；多端下服务端可覆盖 |

**关键区别**：业务调用方往往**延迟**写日志（缓存、批处理、离线模式）。`event_time` 是实体本身发生的时刻，与日志写入时机无关；`process_time` 是日志真正落盘的时刻。

举例：
- 用户在 14:23:25 (本地时间) 保存一条 link → 立即同步触发 `EventLogClient.create`，event_time = `14:23:25.123+08:00`，process_time = `06:23:25.123Z`（同一时刻的 UTC 表示）
- 离线模式下保存 link → 设备重启联网后调用 `EventLogClient.create`，event_time 仍是 `14:23:25.123+08:00`（实体真实创建时刻），process_time 是 `2026-09-04T10:00:00.000Z`（补录时刻）
- bootstrap 灌历史：`event_time = links.timestamp`（历史真实创建时间），`process_time = now()`（灌入时刻）

**两者允许相等**（同步调用场景），但语义始终不同：一个是实体的"出生证"，一个是日志的"签发时间"。

**时区区分**：`event_time` 用本地偏移（如 `+08:00`）便于用户阅读本地时间；`process_time` 用 UTC 便于服务端/跨端一致。

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
  "id":          "228703dbe5e78fec",     ← 客户端计算（可选，详见下文）
  "device_id":   "phone-1",
  "topic":       "notes",
  "entity_id":   "note-42",
  "action":      "create",
  "event_time":  "2026-09-03T12:00:00.123+08:00",  ← 本地时区
  "data": {
    "title": "Hello",
    "body":  "First sync"
  }
}
```

**字段校验：**

| 缺失/错误 | 状态码 |
|-----------|--------|
| 任何必填字段（`device_id`/`topic`/`entity_id`/`action`/`event_time`）缺失或空串 | `400` (`missing_fields`) |
| `action` 不在 `{create, update, delete}` | `400` (`invalid_action`) |
| `action ∈ {create, update}` 但 `data` 缺失 | `400` (`data_required`) |
| `action = delete` 但 `data` 非 null | 服务端**必须**将 `data` 置为 `null`（不忽略） |
| `event_time` 不是合法 ISO-8601 格式 | `400` (`invalid_event_time`) |
| 客户端上传 `id` 与五元组 sha256 不匹配 | `400` (`id_mismatch`) |
| 客户端没上传 `id` | 服务端按五元组计算 id 后写入（兼容模式） |

**`id` 计算责任：**

客户端**必须**按 `sha256(topic|device_id|event_time|entity_id|action)` 前 16 位计算并上传 `id`。服务端**校验**该 id 与五元组匹配，不匹配则返 `400 id_mismatch`。

客户端不上传 `id` 时服务端代为计算并写入（兼容模式）。生产客户端应始终上传 id 以暴露客户端 bug（id 算错=客户端逻辑错）。

**id 冲突处理：**

按 id 查表：
- 命中且 `(topic, device_id, event_time, entity_id, action)` 完全一致 → 幂等命中，返 `200` + 原记录
- 命中但五元组不一致 → 服务端**不应该**遇到（id 公式稳定），返 `500 internal_error`（说明服务端或客户端 bug）
- 不命中 → 新事件，分配 `process_time = now() UTC`，写表，返 `201`

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
  "process_time":  "2026-09-03T14:53:26.652190Z",
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
GET /events?since=<process_time>&topic=<name>&limit=<N>
Authorization: <secret>
```

| 参数 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `since` | 否 | `""`（拉最早开始所有） | 严格大于此 `process_time` 的事件。**默认按 `process_time` 过滤**（多端场景）。 |
| `topic` | 否 | 不过滤 | 只返回该 topic 的事件 |
| `limit` | 否 | `1000`，最大 `10000` | 返回条数上限 |
| `order` | 否 | `process_time` | 取值 `process_time`（默认）或 `event_time`（单端场景）。决定过滤和排序的字段 |

**`since` 与 `order` 语义：**

- **`order=process_time`**（默认，多端场景）：`since` 按 `process_time` 严格大于过滤，服务端使用 `idx_events_topic_process` 索引
- **`order=event_time`**（单端场景）：`since` 按 `event_time` 严格大于过滤，服务端使用 `idx_events_topic_event` 索引

`since` 字符串格式必须与 `order` 选择的字段格式一致：
- `order=process_time`：`since` 是 ISO-8601 UTC（`Z` 后缀），如 `2026-09-03T14:23:25.123Z`
- `order=event_time`：`since` 是 ISO-8601 本地时区（`+HH:MM` 偏移），如 `2026-09-03T14:23:25.123+08:00`

混用格式（`order=process_time` 但 `since` 带本地偏移）服务端应返 `400 invalid_since_format`。

**响应：**

`200 OK`，body 为数组。按 `order` 字段升序。

```json
[
  { "id": "...", "process_time": "...", "event_time": "...", ... },
  { "id": "...", "process_time": "...", "event_time": "...", ... }
]
```

**触底判定：**

客户端拉流循环（§5.3）：
```
while True:
    batch = GET ?since={cursor}&limit=N
    if len(batch) == 0: break       # 已经到底
    apply_events(batch)
    cursor = batch[-1].<order field>  # 推进游标
    if len(batch) < N: break       # 已经到底
```

服务端**不返回**"已到底"标志位——客户端靠 `len(batch) < limit` 或 `len(batch) == 0` 判断。

**响应状态：**

| 状态码 | 含义 | error code |
|--------|------|-----------|
| `200 OK` | 成功（可能为空数组） | — |
| `400 Bad Request` | `limit` 非整数 | `invalid_limit` |
| `400 Bad Request` | `since` 与 `order` 格式不匹配 | `invalid_since_format` |
| `401 Unauthorized` | 鉴权失败 | `unauthorized` |
| `500 Internal Server Error` | 内部错误 | `internal_error` |

---

## 5. 客户端同步协议

### 5.1 本地状态

每个 topic 客户端维护：

```text
sync_point       : string | null    // 已处理到的 max(<cursor_field>)，多端为 process_time，单端为 event_time（详见 §10.2）
entities         : map<entity_id, EntityState>
EntityState      = {
  last_event     : Event,
  last_event_idx : int              // 在事件序列里的下标（可选，用于调试）
}
```

> `sync_point` 持久化到本地存储。客户端每次启动都加载；`null` 表示从未同步过。

### 5.2 冷启动（cold start）

本地 `sync_point == null` 时执行。多端场景下用 `process_time` 推进游标；单端场景下用 `event_time` 推进游标（详见 §10.2）。

```
function cold_start(topic):
    events = GET /events?since=&topic={topic}&limit=10000
    while len(events) == 10000:                    # 可能分页
        events += GET /events?since={events[-1].<cursor_field>}&topic={topic}&limit=10000
    apply_events(events)                           # 见 §5.4
    sync_point = max(<cursor_field> for e in events) or sync_point
    persist(sync_point, entities)
```

`<cursor_field>` 在多端场景为 `process_time`；单端场景为 `event_time`（即 `id` 公式中已经稳定的那个字段）。

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
        sync_point = batch[-1].<cursor_field>       # 推进游标避免内存爆炸
    apply_events(new_events)                       # 见 §5.4
    if new_events:
        sync_point = max(e.<cursor_field> for e in new_events)
    persist(sync_point, entities)
```

`<cursor_field>` 在多端场景为 `process_time`；单端场景为 `event_time`（详见 §10.2）。

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
            if e.<cursor_field> > cur.last_event.<cursor_field>:
                if e.action == "delete":
                    del entities[e.entity_id]     # tombstone 生效
                else:
                    entities[e.entity_id] = EntityState(last_event=e)
            # 否则旧事件被忽略（不变量：服务端保证拉到的序列是单调的，
            # 但客户端可能因为冷启动后再增量拉，重复处理一遍，需要这条判断）
```

`<cursor_field>` 在多端场景为 `process_time`；单端场景为 `event_time`（详见 §10.2）。

**冲突策略：** Last-Write-Wins by `<cursor_field>`。同一 `entity_id` 的多个事件，取 `<cursor_field>` 最大者。

> 多端场景下 `event_time` 不参与判断（即使客户端时钟错乱，只要服务端入库顺序稳定，最终一致）；单端场景下 `event_time` 是客户端声称的真实事件时间，作为唯一排序依据。

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
    sync_point = max(sync_point, event.<cursor_field>)
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

> **本节演示多端场景**（三个设备 + 服务端）。单端场景下的同步示例见 §10。

假设 topic = `notes`，三个设备。

```
T0  DeviceA 冷启动:
    GET /events?since=&topic=notes   → []
    sync_point = null (没有事件)

T1  DeviceA 写入 note-1:
    POST /events {device:A, entity:note-1, action:create,
                  event_time:T1, data:{title:"A1"}}
    ← 201 {id:01..., process_time:R1, ...}
    本地: entities[note-1] = {data:{title:"A1"}}, sync_point=R1

T2  DeviceB 冷启动:
    GET /events?since=&topic=notes   → [event01]
    apply → entities[note-1] = {data:{title:"A1"}}
    sync_point=R1

T3  DeviceB 改 note-1:
    POST /events {device:B, entity:note-1, action:update,
                  event_time:T3, data:{title:"B-edit", body:"added"}}
    ← 201 {id:02..., process_time:R3, ...}
    本地: entities[note-1] = {data:{title:"B-edit", body:"added"}}, sync_point=R3

T4  DeviceA 同步:
    GET /events?since=R1&topic=notes → [event02]
    apply → process_time R3>R1 → entities[note-1] = {data:{title:"B-edit", body:"added"}}
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
| 事件 GC | 服务端不支持；客户端按 topic 自行管理本地缓存。**部署级允许显式清空**（如离线客户端 UI 提供清空按钮）——见 §10.1 |
| 服务端折叠 | 不提供。客户端必须自己按 entity_id 折叠 |
| `event_time` 一致性 | 不校验。客户端时钟漂移不会被纠正 |
| 分页边界 | `since=<last_process_time>` 在循环中可能漏掉同毫秒事件；详见附录 A。单端场景下若使用 `event_time` 推进游标且 bootstrap 时 `event_time` 取真实历史时间，可规避此问题 |
| 多 topic | 每次同步一个 topic；如需跨 topic 请在客户端循环 |
| 鉴权强度 | 仅共享密钥；无 per-device 区分、无传输加密（生产请加 TLS） |

---

## 附录 A：分页边界处理

服务端 `process_time` 精度为微秒（`%f`），实际并发写同微秒内可能仍有冲突（极小概率）。客户端循环拉取的标准做法：

```
cursor = sync_point
while True:
    batch = GET ?since={cursor}&limit=N
    if not batch: break
    apply_events(batch)
    cursor = batch[-1].<cursor_field>
    if len(batch) < N: break
```

`<cursor_field>` 在多端场景为 `process_time`；单端场景为 `event_time`（详见 §10.2）。

注意：使用 `batch[-1].<cursor_field>` 作为下一轮 `since` 时，**严格大于**比较保证不会丢事件，但可能将同一 `<cursor_field>` 的事件算两次。`apply_events` 的 LWW 判断会保证最终一致（重复事件会被丢弃）。

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

---

## 10. 部署差异与单端场景

本协议最初为多端同步场景设计（多个客户端 + 一个权威服务端）。某些部署场景偏离此假设，本节记录这些偏离以及协议中受影响条款的处理方式。

### 10.1 离线单端场景

**场景**：客户端没有服务端，所有事件写入与读取都在本地 SQLite 表内完成。本质上是"事件流作为本地审计日志"。

#### 偏离的不变量

- **不变量 1（事件不可变）**：部署级允许显式清空。客户端 UI 可提供"清空事件"按钮（等价于"重新初始化本地状态"）。清空后下次启动触发 cold_start 重灌历史，等同于 `sync_point = null`。
- **不变量 2（`process_time` 单调递增）**：单端场景下不强制保证。同一毫秒内多次 `EventLogClient.create()` 可能产生相同 `process_time`。翻页游标改用 `event_time`（见 §10.2）规避此问题。

#### 偏离的字段语义

- **`process_time`**：不再是"服务端入库时间"，而是"本地处理时间"。字段保留以便将来恢复多端场景。

#### 偏离的协议行为

- **`deleteAll()`**：客户端 SDK 暴露此方法，触发本地清空。**不在协议 SDK 必选接口内**，是离线部署的扩展。

#### 不实现的部分（合理省略）

- 服务端 API（`POST/GET /events`）：不需要
- 鉴权（`Authorization`）：不需要
- 错误处理（`401`/`400`/`5xx`）：不需要
- 周期性 / 事件驱动同步：不需要
- 客户端折叠（`apply_events`）：折叠是 sync layer 职责，不在事件流 SDK 协议范围内。如果需要做 sync layer，可基于本 SDK 上层封装。

### 10.2 单端场景下的翻页游标

#### 推荐方案：用 `event_time` 推进游标

`event_time` 在单端场景下有两个稳定来源：

1. **bootstrap 灌历史**：`event_time = links.timestamp`（历史 link 的真实保存时间）。每条 link 的 timestamp 各不相同 → 翻页游标稳定推进。
2. **业务调用**：`event_time = now()`。短时间内多次调用同毫秒概率低；如果出现同毫秒同 entity_id，会被 id 公式识别为同一事件，`INSERT OR IGNORE` 兜底。

#### 索引字段

单端场景下索引 `(topic, event_time)`，替代多端场景的 `(topic, process_time)`。

#### 与多端场景的切换

如果将来从单端升级到多端：

1. 服务端覆盖 `process_time` 为入库时间
2. 索引切换为 `(topic, process_time)`
3. 翻页游标改回 `process_time`
4. 不需要 schema 迁移——`process_time` 列已存在

### 10.3 已知部署案例

| 部署 | 协议偏离条款 | 备注 |
|---|---|---|
| `notfresh/reading-share-android`（事件日志模块） | §10.1 + §10.2 | 无服务端；UI 暴露"清空事件"按钮；翻页用 `event_time`；bootstrap 灌历史时 `event_time = links.timestamp` |

如有新部署案例，按相同格式补充。

### 10.4 事件日志观测 UI（单端场景专属）

多端场景下事件流是 sync layer 的内部数据结构，普通用户不直接接触。单端场景下事件流**暴露给用户作为本地审计日志**，因此需要配套观测 UI。

> 本节为单端场景建议，描述一个最小可用的事件查看界面。多端场景不需要此 UI。

#### 功能要点

1. **列表展示**：按 `<cursor_field>` 倒序展示事件，最新事件在最上方（首屏加载最晚 50 条）。
2. **每行字段**：action（CREATE/UPDATE/DELETE 大写彩色标签）+ topic + entity_id + process_time + event_time。
3. **翻页交互**：显式"加载更多"按钮（不靠触底监听）。点击后用 `until(topic, cursor, limit)` 拿比当前 cursor 更早的 50 条；按钮文案随状态切换：加载中… / 加载更多 / 已经到底（禁用）。
4. **详情查看**：点击任一行 → AlertDialog 显示完整字段（id / topic / action / entity_id / device_id / process_time / event_time / data JSON），文字可选可复制便于排查。
5. **进度指示**：底部状态栏持续显示 "已加载 X / 总 Y 条"，用 `count(topic)` 查总数。
6. **清空入口**（可选，见 §10.1）：底部"清空事件"按钮 → 二次确认 → `deleteAll()` + `resetBootstrapFlag()`。清空后下次启动触发 bootstrap 重灌历史。

#### 与 §5.2 / §5.3 的关系

UI 只做"读"和"清空"——不参与同步流程。客户端业务代码在调用 `EventLogClient.create/update/delete` 时已经隐式完成了"本地折叠"（直接调 `LinkDao.insertLink` 等），UI 只是观察这些事件的窗口。

#### API 调用映射

| UI 操作 | 调用的协议 API | 协议条款 |
|---|---|---|
| 首屏加载 | `until(topic, null, 50)` | §10.2 单端翻页游标 |
| 加载更多 | `until(topic, cursor, 50)` | §10.2 单端翻页游标 |
| 进度显示 | `count(topic)` | §10.1 单端场景扩展 |
| 清空 | `deleteAll()` + `resetBootstrapFlag()` | §10.1 单端场景扩展 |

#### 不做的事

- 不暴露 `id` 之外的协议内部状态（如 sync_point、cursor）给用户
- 不修改事件（创建/修改/删除由业务代码经 `EventLogClient.create/update/delete` 完成，不经此 UI）
- 不参与 sync_point 推进（UI 是只读窗口，不影响协议同步状态）

#### 参考实现

`notfresh/reading-share-android/EventLogActivity.java` — Android 实现，约 160 行，复用 `EventLogClient` API。

---

## 11. 服务端

本章面向**部署服务端**的工程师。前 10 章主要描述客户端行为；本章给出服务端**应该实现什么、不应该实现什么、接口契约**。

### 11.1 服务端职责范围

服务端的核心职责是**事件流的权威存储**：

| 服务端应该做 | 服务端不应该做 |
|---|---|
| 接收 `POST /events` 并持久化事件 | 折叠/合并事件成 entity 状态 |
| 给每个事件分配服务端权威 `process_time` | 校验 `event_time` 是否"合理" |
| 按 `id` 做幂等去重 | 主动推送事件给客户端 |
| 提供 `GET /events` 按游标拉流 | 提供"当前 entity 状态"查询 |
| 鉴权（共享密钥） | 校验 `device_id` 合法性 |
| 持久化事件流（append-only） | 修改/删除已写入事件 |

#### 客户端 SDK 必须自己做的事

- **计算 `id`**：客户端按 `sha256(topic\|device_id\|event_time\|entity_id\|action)` 前 16 位上传，服务端只校验不重算
- **指定 `event_time`**：实体的真实创建时间，客户端传入，服务端原样保存
- **本地折叠**：服务端不折叠，客户端 SDK 拉回事件流后自己按 `process_time` / `event_time` 折叠
- **断点续传**：客户端 SDK 自己维护 `sync_point`（最大 `process_time` 已处理），重启后从断点拉
- **客户端时钟管理**：客户端 SDK 自己负责 `event_time` 用本地时区（`+HH:MM` 偏移）

#### 服务端的"不做"是设计意图

服务端不做折叠/不校验 device_id/不提供 entity 状态查询——**有意把状态计算和信任管理推给客户端**。原因：

- 多设备场景下，"谁的 entity 状态是权威" 是个难题，让客户端自己解决（Last-Write-Wins 等策略）
- 服务端不验证 `device_id` 让恶意客户端能伪造身份——这是**信任模型的明确取舍**：本协议默认客户端可信（部署在受控环境），不解决身份冒用问题

### 11.2 鉴权与信任

#### 鉴权机制

所有请求必须带：

```
Authorization: <EVENT_LOG_SECRET>
```

值与服务端环境变量 `EVENT_LOG_SECRET` 完全一致（字符串相等）。缺失或不匹配 → `401`。

#### 信任模型

服务端**信任所有持有共享密钥的客户端**。这意味着：

| 服务端做 | 服务端不做 |
|---|---|
| 校验共享密钥 | 校验 `device_id` 是否真实存在 |
| 持久化事件（带 device_id） | 验证 device_id 是否合法或已注册 |
| 给事件分配权威 `process_time` | 拒绝或审查可疑的 `device_id` |

**信任边界**：服务端代码 + 运行环境 + 持有密钥的所有客户端。

#### 共享密钥泄露的后果

任何持有密钥的客户端都能：
- 写入任意 `device_id` 事件（可冒充其他客户端）
- 写入任意 `topic` / `entity_id` / `event_time` 事件（可污染事件流）
- 删除已知 `(topic, device_id, event_time, entity_id, action)` 的事件（幂等命中，不写新行但可阻断）
- **无法**：修改已写入事件的 `id`（PRIMARY KEY）、`process_time`（服务端权威）、`event_time`（id 公式决定）

#### 生产部署建议

- 通过 TLS 提供 `EVENT_LOG_SECRET` 传输保护
- 服务端不记录明文密钥，只比较
- 服务端审计日志应记录所有 401 失败和成功的写入请求（含 device_id）便于事后追溯

### 11.3 服务端存储

#### 事件表 schema

```sql
CREATE TABLE events (
    id            TEXT PRIMARY KEY,
    topic         TEXT NOT NULL,
    process_time  TEXT NOT NULL,    -- 服务端入库瞬间（UTC，Z 后缀）
    event_time    TEXT NOT NULL,    -- 客户端声称时间（本地时区，+HH:MM 偏移）
    device_id     TEXT NOT NULL,
    entity_id     TEXT NOT NULL,
    action        TEXT NOT NULL,
    data          TEXT              -- JSON 字符串；delete 时为 NULL
);

-- 多端场景翻页游标索引（默认）
CREATE INDEX idx_events_topic_process
    ON events(topic, process_time);

-- 单端场景翻页游标索引（备用，按需建立）
CREATE INDEX idx_events_topic_event
    ON events(topic, event_time);
```

#### 字段语义差异（服务端 vs 客户端）

| 字段 | 客户端事件表 | 服务端事件表 |
|---|---|---|
| `event_time` | 客户端声称的本地时区时间 | 原样保存客户端上传值，不归一化 |
| `process_time` | 客户端本地处理时间（仅参考） | **服务端入库瞬间的 UTC 时间**（权威） |
| `id` | 客户端算的 sha256 前 16 位 | 原样保存客户端上传值 |

#### 双索引的原因

服务端需要支持两种翻页模式：

- **多端场景**（默认）：`GET /events?since=<process_time>` 按 `process_time` 过滤
- **单端场景**（特殊）：客户端传 `since=<event_time>` 按 `event_time` 过滤

详见 §11.4 since 字段。

#### 保留策略

本协议**不规定**事件保留时长：

- 服务端可以永久保留（最简单，磁盘增长）
- 服务端可以按时间归档（如 1 年后移到冷存储）
- 服务端可以按 topic 分区存储
- **不建议**：服务端删除已写入事件——破坏不变量 1（事件不可变）

#### 存储实现选择

协议不限定存储实现。常见选择：

- **SQLite**：单机部署，简单（参考实现 `app.py` 用 SQLite）
- **PostgreSQL**：多机部署，支持更好并发
- **Kafka / Pulsar**：事件流原生支持，但只暴露 `event_time` / `process_time` 语义可能多余
- **对象存储（S3）+ JSON 行**：冷数据归档

任何满足"按 (topic, time) 范围查询 + 按 id 幂等去重"的存储都能实现。

### 11.4 错误响应格式

服务端所有 4xx/5xx 响应统一为 JSON 对象：

```json
{
    "error": "<machine-readable error code>",
    "message": "<human-readable description>",
    "<field>": "<extra context>"
}
```

#### 标准错误码

| HTTP | error code | 含义 | 何时 | 额外字段 |
|---|---|---|---|---|
| 400 | `missing_fields` | 请求缺少必填字段 | `device_id` / `entity_id` / `action` / `event_time` 任一为空 | `fields`: 缺失字段名列表 |
| 400 | `invalid_action` | action 不在白名单 | 不是 create/update/delete | `action`, `allowed` |
| 400 | `data_required` | 非 delete 操作缺 data | create/update 没带 data 字段 | — |
| 400 | `id_mismatch` | 客户端上传的 id 与五元组不符 | sha256 校验失败 | `expected`, `received` |
| 400 | `invalid_limit` | limit 不是整数 | GET 请求的 limit 参数非数字 | — |
| 401 | `unauthorized` | 鉴权失败 | Authorization 头缺失或不匹配 | — |
| 500 | `internal_error` | 服务端内部错误 | 数据库失败、异常未捕获 | `request_id`（用于排查） |
| 503 | `unavailable` | 服务端暂时不可用 | 维护、过载 | `retry_after`（秒） |

#### 成功响应不带 `error` 字段

200 / 201 响应是**事件本身**的 JSON 对象（`POST`）或事件数组（`GET`），不带 `error` 字段。客户端通过 HTTP 状态码判断成功/失败。

#### 客户端 SDK 错误处理

参见 §6（错误处理）。补充：

- 401 → **不要重试**（密钥错误，停同步）
- 400 → 记录日志，**不要重试**（请求本身非法）
- 5xx → 指数退避重试，`sync_point` 不前进
- 503 → 服务端明确告诉客户端何时重试（`retry_after`）

### 11.5 服务端不实现的部分（明确）

服务端**不提供**以下接口：

| 客户端可能想要的 | 本协议的处理 |
|---|---|
| `GET /entities/{id}` 当前状态 | 不提供。客户端拉事件流自己折叠 |
| 实时推送（WebSocket / SSE） | 不提供。客户端周期性 GET |
| 事件修改 / 删除 | 不提供。事件不可变 |
| 事件 GC / 归档 | 不规定保留时长，由部署决定（§11.3） |
| 多 topic 原子事务 | 不提供。每个 POST 写入单 topic 单事件 |
| 用户/设备注册 | 不提供。任何持密钥者都是合法客户端 |

### 11.6 单端场景的服务端可选项

§10.1 描述了**无服务端**的单端场景。如果部署想要服务端辅助（事件流备份、多设备观察等），可以：

- 实现本协议 §1-§11 的服务端
- 客户端用同一 SDK，但翻页用 `event_time`（服务端走 `idx_events_topic_event` 索引）
- 这样同一份服务端代码支持两种场景

**服务端代码无需区分**多端/单端——它按 `since` 字段对应到正确的索引即可。

### 11.7 部署案例

| 部署 | 用途 | 协议差异 |
|---|---|---|
| `notfresh/event-log-sync-protocol/app.py` | 参考实现，单机 Flask + SQLite | 无 |
| 第三方云服务（待定） | 多用户共享 | 建议加 TLS、密钥轮换 |

如有新部署案例，按相同格式补充。
