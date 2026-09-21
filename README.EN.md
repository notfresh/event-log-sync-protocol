# event-log-sync-protocol

A Kafka-flavored multi-device sync log backed by SQLite. Append events
with `POST /events`, pull them back as a stream with `GET /events`.

[中文版本](./README.md)

## Run

```bash
pip install flask
EVENT_LOG_SECRET="your-long-secret-here" python app.py
# listens on http://127.0.0.1:5000
```

Optional env vars:

| var | default | meaning |
| --- | --- | --- |
| `EVENT_LOG_SECRET` | placeholder (change it) | required auth token |
| `EVENT_LOG_DB` | `events.db` (cwd) | SQLite file path |

## API

All requests need `Authorization: <EVENT...T>`.

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

- `topic` optional; defaults to `__default__`.
- `data` required for create/update, must be null/absent for delete.
- Response `201` (new) or `200` (idempotent replay).
- Server adds `id` (sha256 hash) and `recorded_time` (UTC now).

### `GET /events?since=<recorded_time>&topic=<name>&limit=N`

Returns events strictly after `since`, ordered by `recorded_time` ASC.
`limit` default 1000, max 10000. Omit `topic` to read all topics.

## Tests

```bash
python -m pytest test_app.py -v
```

## How sync works

1. Each device `POST`s every local change (create/update/delete) with
   its own clock time as `event_time`.
2. To catch up, a device calls `GET /events?since=<last_seen_recorded_time>`.
3. Locally, the device replays the stream against its own snapshot to
   reach the latest state.

Server timestamps (`recorded_time`) are the source of truth for
catching up — that's why `since` filters on it. Client `event_time`
is preserved for audit but not used for ordering.

See [PROTOCOL.md](./PROTOCOL.md) for the full wire-format spec.
