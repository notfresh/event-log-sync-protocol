"""Tests for the SQLite event log HTTP service."""
import hashlib
import json
import os
import sqlite3
import tempfile

import pytest

# Use a temp DB and a known secret BEFORE importing the app.
TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
TMP_DB.close()
os.environ["EVENT_LOG_DB"] = TMP_DB.name
os.environ["EVENT_LOG_SECRET"] = "test-secret-123"

import app as app_module  # noqa: E402


@pytest.fixture
def client():
    app_module.app.config["TESTING"] = True
    with app_module.app.test_client() as c:
        # Each test starts from an empty events table so they don't bleed.
        conn = sqlite3.connect(TMP_DB.name)
        conn.execute("DROP TABLE IF EXISTS events")
        conn.commit()
        conn.close()
        yield c


@pytest.fixture
def headers():
    return {"Authorization": "test-secret-123"}


def _post(client, headers, body):
    return client.post("/events", json=body, headers=headers)


# ---- Health & auth ---------------------------------------------------------

def test_health_no_auth(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json == {"status": "ok"}


def test_post_without_auth_is_401(client):
    r = client.post("/events", json={
        "device_id": "d1", "entity_id": "e1", "action": "create",
        "event_time": "2026-01-01T00:00:00Z", "data": {"x": 1},
    })
    assert r.status_code == 401


def test_get_without_auth_is_401(client):
    r = client.get("/events?since=")
    assert r.status_code == 401


def test_wrong_secret_is_401(client):
    r = client.post(
        "/events",
        json={"device_id": "d", "entity_id": "e", "action": "create",
              "event_time": "t", "data": {}},
        headers={"Authorization": "wrong"},
    )
    assert r.status_code == 401


# ---- POST basics -----------------------------------------------------------

def test_post_create_returns_201_and_fields(client, headers):
    body = {
        "device_id": "devA",
        "entity_id": "note-1",
        "action": "create",
        "event_time": "2026-09-03T10:00:00Z",
        "data": {"title": "hello"},
    }
    r = _post(client, headers, body)
    assert r.status_code == 201
    j = r.json
    assert j["device_id"] == "devA"
    assert j["entity_id"] == "note-1"
    assert j["action"] == "create"
    assert j["data"] == {"title": "hello"}
    assert j["recorded_time"]  # server-assigned
    assert j["event_time"] == "2026-09-03T10:00:00Z"
    assert j["topic"] == "__default__"  # default topic
    assert len(j["id"]) == 16  # sha256 truncated


def test_post_missing_field_400(client, headers):
    r = _post(client, headers, {
        "device_id": "d1", "entity_id": "e1", "action": "create",
        "data": {},
        # event_time missing
    })
    assert r.status_code == 400
    assert "event_time" in r.json["fields"]


def test_post_invalid_action_400(client, headers):
    r = _post(client, headers, {
        "device_id": "d1", "entity_id": "e1", "action": "yeet",
        "event_time": "2026-09-03T10:00:00Z", "data": {},
    })
    assert r.status_code == 400
    assert r.json["error"] == "invalid action"


def test_post_create_without_data_400(client, headers):
    r = _post(client, headers, {
        "device_id": "d1", "entity_id": "e1", "action": "create",
        "event_time": "2026-09-03T10:00:00Z",
    })
    assert r.status_code == 400


def test_post_delete_allows_null_data(client, headers):
    r = _post(client, headers, {
        "device_id": "d1", "entity_id": "e1", "action": "delete",
        "event_time": "2026-09-03T10:00:00Z",
    })
    assert r.status_code == 201
    assert r.json["data"] is None
    assert r.json["action"] == "delete"


# ---- Idempotency -----------------------------------------------------------

def test_duplicate_post_returns_same_record(client, headers):
    body = {
        "device_id": "devA", "entity_id": "note-1", "action": "update",
        "event_time": "2026-09-03T10:00:00Z",
        "data": {"title": "v1"},
    }
    r1 = _post(client, headers, body)
    assert r1.status_code == 201
    r2 = _post(client, headers, body)
    assert r2.status_code == 200  # idempotent
    assert r1.json["id"] == r2.json["id"]
    assert r1.json["recorded_time"] == r2.json["recorded_time"]

    # Only one row in DB.
    rows = client.get("/events?since=", headers=headers).json
    matching = [x for x in rows
                if x["entity_id"] == "note-1" and x["action"] == "update"]
    assert len(matching) == 1


def test_id_includes_topic(client, headers):
    body_a = {
        "device_id": "devA", "entity_id": "e", "action": "create",
        "event_time": "t", "data": {}, "topic": "alpha",
    }
    body_b = dict(body_a, topic="beta")
    r_a = _post(client, headers, body_a)
    r_b = _post(client, headers, body_b)
    assert r_a.status_code == 201
    assert r_b.status_code == 201
    assert r_a.json["id"] != r_b.json["id"]


# ---- GET -------------------------------------------------------------------

def _seed(client, headers):
    # Three events on default topic, one on "alpha".
    events = [
        {"device_id": "d1", "entity_id": "a", "action": "create",
         "event_time": "2026-09-03T10:00:00Z", "data": {"v": 1}},
        {"device_id": "d1", "entity_id": "a", "action": "update",
         "event_time": "2026-09-03T10:00:01Z", "data": {"v": 2}},
        {"device_id": "d2", "entity_id": "b", "action": "create",
         "event_time": "2026-09-03T10:00:02Z", "data": {"v": 99}},
        {"device_id": "d2", "entity_id": "c", "action": "create",
         "event_time": "2026-09-03T10:00:03Z", "data": {},
         "topic": "alpha"},
    ]
    for e in events:
        r = _post(client, headers, e)
        assert r.status_code in (200, 201)
    return events


def test_get_returns_all_events_after_since(client, headers):
    _seed(client, headers)
    r = client.get("/events?since=", headers=headers)
    assert r.status_code == 200
    assert isinstance(r.json, list)
    assert len(r.json) == 4


def test_get_filters_by_topic(client, headers):
    _seed(client, headers)
    r = client.get("/events?since=&topic=alpha", headers=headers)
    assert r.status_code == 200
    assert len(r.json) == 1
    assert r.json[0]["topic"] == "alpha"
    assert r.json[0]["entity_id"] == "c"


def test_get_since_filters_by_recorded_time(client, headers):
    _seed(client, headers)
    # Pick the recorded_time of event #2; everything after must be 2 rows.
    all_rows = client.get("/events?since=", headers=headers).json
    cutoff = all_rows[1]["recorded_time"]
    r = client.get(f"/events?since={cutoff}", headers=headers)
    assert r.status_code == 200
    # Strict >: rows after the cutoff.
    remaining = r.json
    assert all(x["recorded_time"] > cutoff for x in remaining)
    assert len(remaining) == 2


def test_get_limit_caps_results(client, headers):
    _seed(client, headers)
    r = client.get("/events?since=&limit=2", headers=headers)
    assert r.status_code == 200
    assert len(r.json) == 2


def test_get_limit_validation(client, headers):
    r = client.get("/events?since=&limit=abc", headers=headers)
    assert r.status_code == 400


def test_get_orders_chronologically(client, headers):
    _seed(client, headers)
    rows = client.get("/events?since=", headers=headers).json
    times = [r["recorded_time"] for r in rows]
    assert times == sorted(times)


# ---- Cleanup ---------------------------------------------------------------

def teardown_module(module):
    try:
        os.unlink(TMP_DB.name)
    except OSError:
        pass
