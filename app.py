"""event-log-sync-protocol: SQLite-backed event log HTTP service.

A Kafka-flavored multi-device sync log. Append events with POST, pull
them back as a stream with GET. Identical POSTs (same topic + device +
entity + action + event_time) are idempotent: the server returns the
existing record instead of duplicating.

See PROTOCOL.md for the wire-format spec (v1).
"""
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, g, jsonify, request

# ---- Config ----------------------------------------------------------------

DB_PATH = Path(os.environ.get("EVENT_LOG_DB", "events.db"))
SECRET = os.environ.get(
    "EVENT_LOG_SECRET",
    "CHANGE-ME-set-the-EVENT_LOG_SECRET-env-var-before-running",
)
DEFAULT_TOPIC = "__default__"

VALID_ACTIONS = {"create", "update", "delete"}

app = Flask(__name__)


# ---- DB --------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id            TEXT PRIMARY KEY,
                topic         TEXT NOT NULL,
                recorded_time TEXT NOT NULL,
                event_time    TEXT NOT NULL,
                device_id     TEXT NOT NULL,
                entity_id     TEXT NOT NULL,
                action        TEXT NOT NULL,
                data          TEXT
            )
            """
        )
        g.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_topic_recorded "
            "ON events(topic, recorded_time)"
        )
        g.db.commit()
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---- Auth ------------------------------------------------------------------

def require_auth() -> bool:
    """Return True if the request carries a matching Authorization header."""
    return request.headers.get("Authorization", "") == SECRET


# ---- Helpers ---------------------------------------------------------------

def compute_id(topic: str, device_id: str, event_time: str,
               entity_id: str, action: str) -> str:
    raw = f"{topic}|{device_id}|{event_time}|{entity_id}|{action}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def now_iso() -> str:
    # Server "wall clock" in UTC ISO-8601 with a Z suffix for clarity.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def row_to_dict(row: sqlite3.Row) -> dict:
    out = dict(row)
    if out["data"] is not None:
        out["data"] = json.loads(out["data"])
    return out


# ---- Routes ----------------------------------------------------------------

@app.get("/health")
def health():
    return jsonify(status="ok")


@app.post("/events")
def post_event():
    if not require_auth():
        return jsonify(error="unauthorized"), 401

    body = request.get_json(silent=True) or {}
    topic = body.get("topic") or DEFAULT_TOPIC
    device_id = body.get("device_id")
    entity_id = body.get("entity_id")
    action = body.get("action")
    event_time = body.get("event_time")
    data = body.get("data")

    missing = [
        k for k, v in {
            "device_id": device_id,
            "entity_id": entity_id,
            "action": action,
            "event_time": event_time,
        }.items()
        if not v
    ]
    if missing:
        return jsonify(error="missing fields", fields=missing), 400

    if action not in VALID_ACTIONS:
        return jsonify(
            error="invalid action",
            action=action,
            allowed=sorted(VALID_ACTIONS),
        ), 400

    if action == "delete":
        data = None
    elif data is None:
        return jsonify(error="data is required for non-delete actions"), 400

    eid = compute_id(topic, device_id, event_time, entity_id, action)
    recorded = now_iso()

    db = get_db()
    existing = db.execute(
        "SELECT * FROM events WHERE id = ?", (eid,)
    ).fetchone()
    if existing is not None:
        # Idempotent: same event posted twice => same record.
        return jsonify(row_to_dict(existing)), 200

    db.execute(
        "INSERT INTO events "
        "(id, topic, recorded_time, event_time, device_id, entity_id, "
        "action, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (eid, topic, recorded, event_time, device_id, entity_id,
         action, json.dumps(data) if data is not None else None),
    )
    db.commit()

    return jsonify(
        id=eid,
        topic=topic,
        recorded_time=recorded,
        event_time=event_time,
        device_id=device_id,
        entity_id=entity_id,
        action=action,
        data=data,
    ), 201


@app.get("/events")
def get_events():
    if not require_auth():
        return jsonify(error="unauthorized"), 401

    since = request.args.get("since", "")
    topic = request.args.get("topic")
    try:
        limit = int(request.args.get("limit", "1000"))
    except ValueError:
        return jsonify(error="limit must be an integer"), 400
    limit = max(1, min(limit, 10000))

    sql = "SELECT * FROM events WHERE recorded_time > ?"
    params: list = [since]
    if topic:
        sql += " AND topic = ?"
        params.append(topic)
    sql += " ORDER BY recorded_time ASC LIMIT ?"
    params.append(limit)

    rows = get_db().execute(sql, params).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
