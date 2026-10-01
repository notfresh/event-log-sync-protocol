# event-log-sync-protocol

一个基于 SQLite 的多端同步事件流协议。设备通过 `POST /events` 追加事件，通过 `GET /events` 拉流回放。

[English version](./README.EN.md)

## 运行

```bash
pip install flask
EVENT_LOG_SECRET="你的长随机字符串" python app.py
# 监听 http://127.0.0.1:5000
```

可选环境变量：

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `EVENT_LOG_SECRET` | 占位符（生产必须改） | 必填的鉴权 token |
| `EVENT_LOG_DB` | `events.db`（当前目录） | SQLite 文件路径 |

## API

所有请求必须带 `Authorization: <EVENT...RET>`。

### `POST /events`

```json
{
  "device_id":  "phone-1",
  "entity_id":  "note-42",
  "action":     "create | update | delete",
  "event_time": "2026-09-03T12:00:00Z",
  "data":       { "title": "hello" },
  "topic":      "notes"
}
```

- `topic` 可选，默认 `__default__`
- `create`/`update` 必须带 `data`；`delete` 必须为 null 或不填
- 响应 `201`（新建）或 `200`（幂等命中）
- 服务端补 `id`（客户端上传的 sha256 前 16 位，服务端校验）与 `process_time`（服务端 UTC 入库时间）

### `GET /events?since=<process_time>&topic=<name>&limit=<N>`

返回严格晚于 `since` 的事件，按 `process_time` 升序。
`limit` 默认 1000，上限 10000。不带 `topic` 则查所有 topic。
加 `order=event_time` 可改用 `event_time` 字段过滤（单端场景，详见 PROTOCOL §11.6）。

## 测试

```bash
python -m pytest test_app.py -v
```

## 同步原理

1. 每台设备把本地变更（create/update/delete）连同实体的真实创建时间 `event_time` 一起 `POST`。
2. 设备要追赶进度时，调用 `GET /events?since=<last_seen_process_time>`。
3. 设备在本地把事件流对自己的快照回放，得到最新状态。

服务端时间戳 `process_time` 是追赶进度的唯一权威——这就是 `since` 默认以它为过滤字段的原因。
`event_time` 是实体的真实创建时间（本地时区带 `+HH:MM` 偏移），仅作审计，不参与多端排序。
两种时间字段的语义区分见 PROTOCOL.md §2.5。

完整协议见 [PROTOCOL.md](./PROTOCOL.md)。
