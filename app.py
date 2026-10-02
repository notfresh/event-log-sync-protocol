"""event-log-sync-protocol: SQLite-backed event log HTTP service.

Append events with POST, pull them back as a stream with GET. Identical
POSTs (same topic + device + entity + action + event_time) are
idempotent: the server returns the existing record instead of duplicating.

See PROTOCOL.md §11 for the server-side contract.
"""
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, g, jsonify, request

DB_PATH = Path(os.environ.get("EVENT_LOG_DB", "events.db"))
SECRET = os.environ.get(
    "EVENT_LOG_SECRET",
    "CHANGE-ME-set-the-EVENT_LOG_SECRET-env-var-before-running",
)
DEFAULT_TOPIC = "__default__"
VALID_ACTIONS = {"create", "update", "delete"}
ID_LEN = 16
MAX_LIMIT = 10000

app = Flask(__name__)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id            TEXT PRIMARY KEY,
                topic         TEXT NOT NULL,
                process_time  TEXT NOT NULL,
                event_time    TEXT NOT NULL,
                device_id     TEXT NOT NULL,
                entity_id     TEXT NOT NULL,
                action        TEXT NOT NULL,
                data          TEXT
            )
            """
        )
        g.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_topic_process "
            "ON events(topic, process_time)"
        )
        g.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_topic_event "
            "ON events(topic, event_time)"
        )
        g.db.commit()
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def require_auth() -> bool:
    return request.headers.get("Authorization", "") == SECRET


def compute_id(topic: str, device_id: str, event_time: str,
               entity_id: str, action: str) -> str:
    raw = f"{topic}|{device_id}|{event_time}|{entity_id}|{action}"
    return hashlib.sha256(raw.encode()).hexdigest()[:ID_LEN]


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def row_to_dict(row: sqlite3.Row) -> dict:
    out = dict(row)
    if out["data"] is not None:
        out["data"] = json.loads(out["data"])
    return out


def err(code: str, message: str, status: int, **extra) -> tuple:
    body = {"error": code, "message": message}
    body.update(extra)
    return jsonify(body), status


@app.get("/health")
def health():
    return jsonify(status="ok")


@app.get("/stats")
def stats():
    """Simple observability — total event count and per-topic breakdown.

    Open http://<host>/stats with curl + Authorization header (or any
    HTTP client that lets you set headers). Intended for human inspection
    during local testing, not for client use.
    """
    if not require_auth():
        return err("unauthorized", "missing or invalid Authorization", 401)
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    by_topic = db.execute(
        "SELECT topic, COUNT(*) AS n FROM events GROUP BY topic ORDER BY n DESC"
    ).fetchall()
    return jsonify(
        total=total,
        by_topic=[{"topic": r["topic"], "count": r["n"]} for r in by_topic],
    )


@app.post("/events")
def post_events():
    """Single or batch write — per PROTOCOL §4.1.

    Body shape decides branch:
      - {"events": [...]}  → batch (length 1..1000)
      - <event object>      → single event
    """
    if not require_auth():
        return err("unauthorized", "missing or invalid Authorization", 401)

    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return err("missing_fields", "request body must be a JSON object", 400)

    if isinstance(body.get("events"), list):
        return _write_batch(body["events"])
    return _write_single(body)


def _validate_event_fields(ev: dict) -> tuple | None:
    """Return an error response tuple if invalid, else None."""
    if not isinstance(ev, dict):
        return err("missing_fields", "event entry must be an object", 400)
    missing = [
        k for k, v in {
            "device_id": ev.get("device_id"),
            "entity_id": ev.get("entity_id"),
            "action": ev.get("action"),
            "event_time": ev.get("event_time"),
        }.items()
        if not v
    ]
    if missing:
        return err("missing_fields", "required fields are empty",
                   400, fields=missing)
    action = ev.get("action")
    if action not in VALID_ACTIONS:
        return err("invalid_action", "action not allowed",
                   400, action=action,
                   allowed=sorted(VALID_ACTIONS))
    if action == "delete":
        return None
    if ev.get("data") is None:
        return err("data_required",
                   "data required for non-delete actions", 400)
    return None


def _write_single(body: dict):
    """Insert one event (or return existing on id hit). Returns 201 / 200."""
    fail = _validate_event_fields(body)
    if fail is not None:
        return fail

    topic = body.get("topic") or DEFAULT_TOPIC
    device_id = body["device_id"]
    entity_id = body["entity_id"]
    action = body["action"]
    event_time = body["event_time"]
    data = None if action == "delete" else body.get("data")

    eid = compute_id(topic, device_id, event_time, entity_id, action)
    # NOTE(id_mismatch 豁免): 不再强制要求 client_id == eid。
    # 原因 — 客户端 ANDROID_ID 重装后会变化,五元组 hash 因此洗牌;
    # 服务端只关心事件数据本身,不需要客户端 device_id 与算出的 eid 完全一致。
    # 服务端按接收到的 device_id 算 eid,做幂等查询/插入。

    db = get_db()
    existing = db.execute(
        "SELECT * FROM events WHERE id = ?", (eid,)
    ).fetchone()
    if existing is not None:
        return jsonify(row_to_dict(existing)), 200

    db.execute(
        "INSERT INTO events "
        "(id, topic, process_time, event_time, device_id, entity_id, "
        "action, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (eid, topic, now_utc(), event_time, device_id, entity_id,
         action, json.dumps(data) if data is not None else None),
    )
    db.commit()

    return jsonify(
        id=eid,
        topic=topic,
        process_time=now_utc(),
        event_time=event_time,
        device_id=device_id,
        entity_id=entity_id,
        action=action,
        data=data,
    ), 201


def _write_batch(events: list):
    """Write each event independently. Any single failure → whole batch
    non-2xx so client leaves its push cursor untouched (idempotent retry)."""
    if not events:
        return err("missing_fields", "events array is required", 400)
    if len(events) > 1000:
        return err("batch_too_large", "events array exceeds 1000", 400)

    # Up-front whole-batch validation (any required field missing →
    # reject whole batch, no partial writes).
    for ev in events:
        fail = _validate_event_fields(ev)
        if fail is not None:
            return fail

    db = get_db()
    results = []
    total_new = 0
    total_duplicate = 0
    # Single timestamp for the whole batch — keeps re-send of the same
    # batch stable for the response shape (process_time is server-authoritative).
    pt = now_utc()
    for ev in events:
        topic = ev.get("topic") or DEFAULT_TOPIC
        device_id = ev["device_id"]
        entity_id = ev["entity_id"]
        action = ev["action"]
        event_time = ev["event_time"]
        data = None if action == "delete" else ev.get("data")

        eid = compute_id(topic, device_id, event_time, entity_id, action)
        # NOTE: 同样豁免 id_mismatch(原因见 _write_single)

        existing = db.execute(
            "SELECT * FROM events WHERE id = ?", (eid,)
        ).fetchone()
        if existing is not None:
            results.append({
                "index": len(results),
                "status": 200,
                "id": eid,
                "process_time": existing["process_time"],
            })
            total_duplicate += 1
            continue

        db.execute(
            "INSERT INTO events "
            "(id, topic, process_time, event_time, device_id, entity_id, "
            "action, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (eid, topic, pt, event_time, device_id, entity_id,
             action, json.dumps(data) if data is not None else None),
        )
        results.append({
            "index": len(results),
            "status": 201,
            "id": eid,
            "process_time": pt,
        })
        total_new += 1

    db.commit()
    status_code = 201 if total_new > 0 and total_duplicate == 0 else 200
    return jsonify(
        results=results,
        total_new=total_new,
        total_duplicate=total_duplicate,
    ), status_code


@app.post("/events/batch")
def post_events_batch():
    """DEPRECATED — kept only as a transient alias during the §4.1
    merger. Use POST /events with a {"events": [...]} body instead.
    """
    if not require_auth():
        return err("unauthorized", "missing or invalid Authorization", 401)
    body = request.get_json(silent=True) or {}
    events = body.get("events")
    if not isinstance(events, list) or not events:
        return err("missing_fields", "events array is required", 400)
    if len(events) > 1000:
        return err("batch_too_large",
                   "events array exceeds 1000", 400)
    return _write_batch(events)


@app.get("/events")
def get_events():
    if not require_auth():
        return err("unauthorized", "missing or invalid Authorization", 401)

    since = request.args.get("since", "")
    topic = request.args.get("topic")
    order = request.args.get("order", "process_time")
    if order not in ("process_time", "event_time"):
        return err("invalid_order",
                   "order must be 'process_time' or 'event_time'",
                   400, order=order)

    try:
        limit = int(request.args.get("limit", "1000"))
    except ValueError:
        return err("invalid_limit", "limit must be an integer", 400)
    limit = max(1, min(limit, MAX_LIMIT))

    sql = f"SELECT * FROM events WHERE {order} > ?"
    params: list = [since]
    if topic:
        sql += " AND topic = ?"
        params.append(topic)
    sql += f" ORDER BY {order} ASC LIMIT ?"
    params.append(limit)

    rows = get_db().execute(sql, params).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


if __name__ == "__main__":
    # Listen on all interfaces so the server is reachable from outside
    # the box (port 80 is exposed on this host per project convention).
    # Run as root or under sudo to bind a privileged port.
    app.run(host="0.0.0.0", port=80, debug=False)