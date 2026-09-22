"""Protocol tests for the PostgreSQL-backed relay.

These tests intentionally exercise storage calls from multiple threads: that
is the closest local equivalent to several worker processes racing to claim an
inbox.  The production guarantee comes from row-level ``SELECT ... FOR UPDATE
SKIP LOCKED`` locking in :mod:`database`/:mod:`storage`, not from a Python
lock.
"""

from __future__ import annotations

import os

# Default to a separate database so `pytest` (which drops and recreates all
# tables around every test) never resets a dev/compose instance's data.
# Respect an explicit RELAY_DATABASE_URL/DATABASE_URL, but otherwise point at
# the `agent_relay_test` database `compose.yaml` provisions alongside the
# app's own `agent_relay` database.
os.environ.setdefault(
    "RELAY_DATABASE_URL", "postgresql+psycopg://agent_relay:agent_relay@localhost:5432/agent_relay_test"
)

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import main
from database import Attempt, Base, Task, as_db_time, db_session, engine, utcnow
from storage import claim_one
from worker import load_credentials, save_credentials


@pytest.fixture(autouse=True)
def empty_database():
    # Resets whatever DB RELAY_DATABASE_URL points at. Defaults to the
    # scratch /tmp file above; never run against a DB with data you need.
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


def register(client: TestClient, name: str) -> tuple[dict, dict[str, str]]:
    response = client.post("/api/v1/agents", json={"name": name})
    assert response.status_code == 201
    data = response.json()
    return data, {"Authorization": f"Bearer {data['token']}"}


def test_protocol_idempotency_terminal_retry_and_auth_boundary():
    with TestClient(main.app) as client:
        sender, sender_headers = register(client, "sender")
        recipient, recipient_headers = register(client, "uppercase")
        sent = client.post(
            "/api/v1/tasks",
            headers={**sender_headers, "Idempotency-Key": "demo-1"},
            json={"to": recipient["agent_id"], "input": "hello relay"},
        )
        assert sent.status_code == 201
        duplicate = client.post(
            "/api/v1/tasks",
            headers={**sender_headers, "Idempotency-Key": "demo-1"},
            json={"to": recipient["agent_id"], "input": "hello relay"},
        )
        assert duplicate.status_code == 201
        assert duplicate.json() == sent.json()
        conflict = client.post(
            "/api/v1/tasks",
            headers={**sender_headers, "Idempotency-Key": "demo-1"},
            json={"to": recipient["agent_id"], "input": "different"},
        )
        assert conflict.status_code == 409

        task_id = sent.json()["task_id"]
        claim = client.post(
            "/api/v1/tasks/claim",
            headers=recipient_headers,
            json={"worker_id": "worker-a", "wait_seconds": 0},
        )
        assert claim.status_code == 200
        claim_data = claim.json()
        assert "claim_token" in claim_data
        complete = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=recipient_headers,
            json={"claim_token": claim_data["claim_token"], "output": "HELLO RELAY"},
        )
        assert complete.status_code == 200
        retry = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=recipient_headers,
            json={"claim_token": claim_data["claim_token"], "output": "HELLO RELAY"},
        )
        assert retry.status_code == 200
        assert client.get(f"/api/v1/tasks/{task_id}", headers=recipient_headers).status_code == 200
        forbidden = client.get(f"/api/v1/tasks/{task_id}", headers={"Authorization": f"Bearer {sender['token']}"})
        assert forbidden.status_code == 200  # sender is an authorized participant
        no_credentials = client.get("/api/v1/agents")
        assert no_credentials.status_code == 401
        attempts = client.get(f"/api/v1/tasks/{task_id}/attempts", headers=sender_headers).json()
        assert attempts["items"][0]["outcome"] == "completed"
        assert "claim_token" not in attempts["items"][0]


def test_atomic_claims_distribute_without_overlap():
    with TestClient(main.app) as client:
        _sender, sender_headers = register(client, "sender")
        recipient, _recipient_headers = register(client, "recipient")
        for index in range(16):
            response = client.post(
                "/api/v1/tasks",
                headers=sender_headers,
                json={"to": recipient["agent_id"], "input": f"task-{index}"},
            )
            assert response.status_code == 201
        with ThreadPoolExecutor(max_workers=16) as pool:
            claims = list(pool.map(lambda index: claim_one(recipient["agent_id"], f"worker-{index}"), range(16)))
        claims = [claim for claim in claims if claim is not None]
        assert len(claims) == 16
        assert len({claim["task_id"] for claim in claims}) == 16
        with db_session() as db:
            processing = list(db.query(Task).filter(Task.status == "processing"))
            assert len(processing) == 16
            assert all(task.attempt_count == 1 for task in processing)


def test_expiry_requeues_and_old_token_is_stale_before_recovery():
    with TestClient(main.app) as client:
        _sender, sender_headers = register(client, "sender")
        recipient, recipient_headers = register(client, "recipient")
        task = client.post(
            "/api/v1/tasks",
            headers=sender_headers,
            json={"to": recipient["agent_id"], "input": "recover me"},
        ).json()
        task_id = task["task_id"]
        first = client.post(
            "/api/v1/tasks/claim", headers=recipient_headers, json={"worker_id": "dead", "wait_seconds": 0}
        ).json()
        with db_session() as db:
            attempt = db.query(Attempt).filter(Attempt.task_id == task_id).one()
            attempt.lease_expires_at = as_db_time(utcnow() - timedelta(seconds=1))
        stale = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=recipient_headers,
            json={"claim_token": first["claim_token"], "output": "TOO LATE"},
        )
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "stale_claim"
        assert main.recover_expired() == 1
        second = client.post(
            "/api/v1/tasks/claim", headers=recipient_headers, json={"worker_id": "replacement", "wait_seconds": 0}
        )
        assert second.status_code == 200
        assert second.json()["attempt"] == 2
        assert second.json()["claim_token"] != first["claim_token"]


def test_task_stays_queued_until_worker_starts_with_saved_credentials(tmp_path):
    # SPEC.md acceptance scenario 2: register an agent without starting a
    # worker, confirm its task sits queued, then confirm a worker process
    # picking up saved credentials from disk can claim and complete it.
    with TestClient(main.app) as client:
        recipient, _recipient_headers = register(client, "recipient-no-worker")
        credentials_path = tmp_path / "credentials.json"
        save_credentials(credentials_path, {"agent_id": recipient["agent_id"], "token": recipient["token"]})

        sender, sender_headers = register(client, "sender")
        task = client.post(
            "/api/v1/tasks",
            headers=sender_headers,
            json={"to": recipient["agent_id"], "input": "scenario 2 payload"},
        )
        assert task.status_code == 201
        task_id = task.json()["task_id"]

        queued = client.get(f"/api/v1/tasks/{task_id}", headers=sender_headers).json()
        assert queued["status"] == "queued"
        assert queued["attempt_count"] == 0

        loaded = load_credentials(credentials_path)
        assert loaded == {"agent_id": recipient["agent_id"], "token": recipient["token"]}
        worker_headers = {"Authorization": f"Bearer {loaded['token']}"}

        claim = client.post(
            "/api/v1/tasks/claim",
            headers=worker_headers,
            json={"worker_id": "scenario2-worker", "wait_seconds": 0},
        )
        assert claim.status_code == 200
        claim_data = claim.json()
        assert claim_data["attempt"] == 1

        complete = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=worker_headers,
            json={"claim_token": claim_data["claim_token"], "output": claim_data["input"].upper()},
        )
        assert complete.status_code == 200

        final = client.get(f"/api/v1/tasks/{task_id}", headers=sender_headers).json()
        assert final["status"] == "completed"
        assert final["output"] == "SCENARIO 2 PAYLOAD"
        assert final["attempt_count"] == 1


def test_dashboard_is_asset_and_invalid_input_is_documented_error():
    with TestClient(main.app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "sessionStorage" in page.text
        missing_name = client.post("/api/v1/agents", json={})
        assert missing_name.status_code == 400
        assert missing_name.json()["error"]["code"] == "invalid_input"
