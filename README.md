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
- 服务端补 `id`（sha256 哈希）与 `recorded_time`（服务端 UTC 当前时间）

### `GET /events?since=<recorded_time>&topic=<name>&limit=N`

返回严格晚于 `since` 的事件，按 `recorded_time` 升序。
`limit` 默认 1000，上限 10000。不带 `topic` 则查所有 topic。

## 测试

```bash
python -m pytest test_app.py -v
```

## 同步原理

1. 每台设备把本地变更（create/update/delete）连同自己的时钟时间 `event_time` 一起 `POST`。
2. 设备要追赶进度时，调用 `GET /events?since=<last_seen_recorded_time>`。
3. 设备在本地把事件流对自己的快照回放，得到最新状态。

服务端时间戳 `recorded_time` 是追赶进度的唯一权威——这就是 `since` 以它为过滤字段的原因。`event_time` 仅作审计，不参与排序。

完整协议见 [PROTOCOL.md](./PROTOCOL.md)。
