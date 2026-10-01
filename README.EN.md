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
- Server adds `id` (client-supplied sha256 first 16 hex, server validates) and `process_time` (server-side UTC now).

### `GET /events?since=<process_time>&topic=<name>&limit=<N>`

Returns events strictly after `since`, ordered by `process_time` ASC.
`limit` default 1000, max 10000. Omit `topic` to read all topics.
Add `order=event_time` to filter by `event_time` instead (single-end scenario, see PROTOCOL §11.6).

## Tests

```bash
python -m pytest test_app.py -v
```

## How sync works

1. Each device `POST`s every local change (create/update/delete) with
   the entity's true creation time as `event_time`.
2. To catch up, a device calls `GET /events?since=<last_seen_process_time>`.
3. Locally, the device replays the stream against its own snapshot to
   reach the latest state.

Server timestamps (`process_time`) are the source of truth for
catching up — that's why `since` filters on it by default. Client
`event_time` is the entity's true creation time (local timezone with
`+HH:MM` offset), preserved for audit but not used for multi-device
ordering. See PROTOCOL.md §2.5 for the semantic distinction between
the two time fields.

See [PROTOCOL.md](./PROTOCOL.md) for the full wire-format spec.
