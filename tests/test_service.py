from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alertsvc.app import create_app  # noqa: E402
from alertsvc.engine import EngineState, advance, simulate  # noqa: E402


# ---------------------------------------------------------------------------
# Pure engine
# ---------------------------------------------------------------------------


def test_strictly_greater_than_threshold():
    assert advance(EngineState(), 500, 500, 1).breached is False
    assert advance(EngineState(), 500.01, 500, 1).breached is True


def test_requires_consecutive_breaches_and_single_reset_recovers():
    state = EngineState()
    state = advance(state, 900, 500, 3).state
    state = advance(state, 900, 500, 3).state
    assert state.in_alert is False
    out = advance(state, 900, 500, 3)
    assert out.event == "alert"
    assert out.state.in_alert is True

    # A further breach while alerting emits nothing.
    out = advance(out.state, 900, 500, 3)
    assert out.event is None
    assert out.state.in_alert is True

    # One below-threshold sample recovers immediately.
    out = advance(out.state, 100, 500, 3)
    assert out.event == "recovery"
    assert out.state.consecutive == 0
    assert out.state.in_alert is False


def test_interrupted_burst_does_not_alert():
    samples = [
        {"id": 1, "duration_ms": 900, "received_at": 1},
        {"id": 2, "duration_ms": 900, "received_at": 2},
        {"id": 3, "duration_ms": 100, "received_at": 3},
        {"id": 4, "duration_ms": 900, "received_at": 4},
        {"id": 5, "duration_ms": 900, "received_at": 5},
    ]
    result = simulate(samples, 500, 3)
    assert result.alert_count == 0
    assert result.max_consecutive == 2


def test_simulate_reports_events_and_final_state():
    samples = [{"id": i, "duration_ms": d, "received_at": i} for i, d in enumerate(
        [900, 900, 100, 900, 900, 900, 100, 900], start=1
    )]
    result = simulate(samples, 500, 2)
    assert [e.kind for e in result.events] == ["alert", "recovery", "alert", "recovery"]
    assert result.alert_count == 2
    assert result.recovery_count == 2
    assert result.state.in_alert is False
    assert result.state.consecutive == 1


# ---------------------------------------------------------------------------
# Service fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def app(tmp_path):
    application = create_app(
        {
            "DATABASE": str(tmp_path / "test.db"),
            "RETENTION": 100_000,
            "DEFAULT_THRESHOLD_MS": 500.0,
            "DEFAULT_CONSECUTIVE": 3,
        }
    )
    application.config["INIT_DB"]()
    application.config["TESTING"] = True
    return application


@pytest.fixture()
def client(app):
    return app.test_client()


def post_samples(client, values):
    res = client.post("/api/samples/seq", json={"values": values})
    assert res.status_code == 201, res.get_json()
    return res.get_json()


def status(client):
    res = client.get("/api/status")
    assert res.status_code == 200
    return res.get_json()


def events(client, kind=None):
    qs = "?limit=500" + (f"&kind={kind}" if kind else "")
    res = client.get(f"/api/events{qs}")
    assert res.status_code == 200
    return res.get_json()["items"]


# ---------------------------------------------------------------------------
# Live alerting
# ---------------------------------------------------------------------------


def test_alert_only_after_consecutive_breaches_and_recover_on_one_ok(client):
    post_samples(client, [900, 900])
    assert status(client)["state"]["in_alert"] is False
    assert events(client) == []

    post_samples(client, [900])
    st = status(client)
    assert st["state"]["in_alert"] is True
    assert len(events(client, "alert")) == 1

    post_samples(client, [900, 900, 900])
    assert len(events(client)) == 1, "no duplicate alerts while staying in alert"

    post_samples(client, [120])
    st = status(client)
    assert st["state"]["in_alert"] is False
    assert [e["kind"] for e in events(client)] == ["recovery", "alert"]
    assert events(client)[0]["episode_id"] == events(client)[1]["episode_id"]


def test_episode_records_start_and_end_durations(client):
    post_samples(client, [900, 900, 900, 42])
    recovery, alert = events(client)  # newest first
    assert alert["duration_ms"] == 900
    assert recovery["duration_ms"] == 42
    assert recovery["episode_id"] == alert["episode_id"]


def test_invalid_payloads_rejected(client):
    assert client.post("/api/samples", json={"duration_ms": -1}).status_code == 400
    assert client.post("/api/samples", json={}).status_code == 400
    assert client.post("/api/samples", data="nope", content_type="text/plain").status_code == 400
    assert client.post("/api/trials", json={"threshold_ms": 0, "consecutive": 2}).status_code == 400
    assert client.post("/api/trials", json={"threshold_ms": 10, "consecutive": 0}).status_code == 400


# ---------------------------------------------------------------------------
# Trial preview
# ---------------------------------------------------------------------------


def test_trial_preview_is_withheld_and_live_rule_still_alerts(client):
    post_samples(client, [900, 900, 900])  # live v1 alert at sample 3
    assert len(events(client, "alert")) == 1

    res = client.post("/api/trials", json={"threshold_ms": 800, "consecutive": 2})
    assert res.status_code == 201
    body = res.get_json()
    trial_id = body["rule"]["id"]
    assert body["rule"]["status"] == "trial"
    assert body["preview"]["sample_count"] == 3
    assert body["preview"]["alert_count"] == 1
    assert body["preview"]["ends_in_alert"] is True
    assert len(body["preview_events"]) == 1

    # Trial events are visible only as withheld previews.
    withheld = client.get(f"/api/trials/{trial_id}/events").get_json()
    assert withheld["withheld_events"]["total"] == 1

    # The issued log must not contain anything from the trial.
    assert {e["rule_version"] for e in events(client)} == {1}
    assert len(events(client)) == 1

    # Ingest while trial pending: old rule still alerts, trial shadows silently.
    post_samples(client, [900, 900, 900, 100])
    st = status(client)
    assert st["active_rule"]["version"] == 1
    assert st["trial"]["rule"]["version"] == 2
    assert st["trial"]["rule"]["shadow"]["sample_count"] == 4
    # v1 recovers; v2 (800ms ×2) also recovers. One issued recovery from v1 only.
    assert [e["kind"] for e in events(client)] == ["recovery", "alert"]
    assert {e["rule_version"] for e in events(client)} == {1}


def test_second_trial_rejected_while_one_pending(client):
    assert client.post("/api/trials", json={"threshold_ms": 800, "consecutive": 2}).status_code == 201
    res = client.post("/api/trials", json={"threshold_ms": 700, "consecutive": 2})
    assert res.status_code == 409


def test_trial_preview_on_empty_dataset(client):
    res = client.post("/api/trials", json={"threshold_ms": 800, "consecutive": 2})
    assert res.status_code == 201
    body = res.get_json()
    assert body["preview"]["sample_count"] == 0
    assert body["preview"]["alert_count"] == 0
    assert body["preview"]["ends_in_alert"] is False


# ---------------------------------------------------------------------------
# Activation semantics
# ---------------------------------------------------------------------------


def test_activation_continues_alert_without_new_issued_event(client):
    post_samples(client, [900, 900, 900])
    before = events(client)
    assert len(before) == 1 and before[0]["kind"] == "alert"
    episode_id = before[0]["episode_id"]

    trial = client.post("/api/trials", json={"threshold_ms": 950, "consecutive": 3}).get_json()
    # v2 with 950ms threshold needs 3 consecutive >950; 900ms samples don't breach.
    # Ingest breach for both rules: 1200 x3 -> v1 already alerting (no event);
    # v2 reaches alert.
    post_samples(client, [1200, 1200, 1200])
    st = status(client)
    assert st["state"]["in_alert"] is True
    assert st["trial"]["state"]["in_alert"] is True

    res = client.post(f"/api/trials/{trial['rule']['id']}/activate")
    assert res.status_code == 200
    body = res.get_json()
    assert body["transition"] == "continued"
    assert body["event"] is None
    assert body["catch_up_samples"] == 0, "must not re-scan history on activation"
    assert body["state"]["open_episode_id"] == episode_id

    # Still exactly one issued alert, and it is the original episode.
    issued = events(client)
    assert len(issued) == 1
    assert issued[0]["episode_id"] == episode_id
    assert status(client)["active_rule"]["version"] == 2

    # Subsequent recovery is issued under the new version and closes the old episode.
    post_samples(client, [10])
    latest = events(client)[0]
    assert latest["kind"] == "recovery"
    assert latest["episode_id"] == episode_id
    assert latest["rule_version"] == 2
    assert latest["cause"] == "live"


def test_activation_issues_recovery_when_new_rule_not_alerting(client):
    post_samples(client, [900, 900, 900])
    assert status(client)["state"]["in_alert"] is True
    episode_id = events(client)[0]["episode_id"]

    trial = client.post("/api/trials", json={"threshold_ms": 5000, "consecutive": 3}).get_json()
    assert trial["preview"]["ends_in_alert"] is False

    body = client.post(f"/api/trials/{trial['rule']['id']}/activate").get_json()
    assert body["transition"] == "recovered"
    assert body["event"]["kind"] == "recovery"

    issued = events(client)
    assert [e["kind"] for e in issued] == ["recovery", "alert"]
    recovery = issued[0]
    assert recovery["cause"] == "activation"
    assert recovery["episode_id"] == episode_id
    assert recovery["rule_version"] == 2
    assert recovery["details"] == {"previous_rule_version": 1}
    assert status(client)["state"]["in_alert"] is False


def test_activation_issues_alert_when_new_rule_is_alerting(client):
    post_samples(client, [900])  # below 500x3 not alerting
    assert status(client)["state"]["in_alert"] is False
    # v2 threshold 100, consecutive 1 -> preview is alerting at the last sample.
    trial = client.post("/api/trials", json={"threshold_ms": 100, "consecutive": 1}).get_json()
    assert trial["preview"]["ends_in_alert"] is True

    body = client.post(f"/api/trials/{trial['rule']['id']}/activate").get_json()
    assert body["transition"] == "alerted"
    assert body["event"]["kind"] == "alert"

    issued = events(client)
    assert len(issued) == 1
    assert issued[0]["cause"] == "activation"
    assert issued[0]["rule_version"] == 2
    assert issued[0]["duration_ms"] == 900
    assert status(client)["state"]["in_alert"] is True


def test_activation_neither_alerting_emits_nothing(client):
    post_samples(client, [100, 100])
    trial = client.post("/api/trials", json={"threshold_ms": 500, "consecutive": 3}).get_json()
    body = client.post(f"/api/trials/{trial['rule']['id']}/activate").get_json()
    assert body["transition"] == "unchanged"
    assert body["event"] is None
    assert events(client) == []
    assert status(client)["active_rule"]["version"] == 2


def test_activation_uses_latest_shadow_state_not_history_rescan(client):
    # Build a long alternating history under v1.
    values = []
    for i in range(80):
        values.extend([900, 900, 900, 50])
    post_samples(client, values)
    old_events = events(client)
    assert old_events

    trial = client.post("/api/trials", json={"threshold_ms": 850, "consecutive": 2}).get_json()
    shadow_before = trial["rule"]["shadow"]["sample_count"]

    # Continue reporting while the trial is pending.
    post_samples(client, [2000, 2000, 10])
    st = status(client)
    assert st["trial"]["rule"]["shadow"]["sample_count"] == shadow_before + 3

    body = client.post(f"/api/trials/{trial['rule']['id']}/activate").get_json()
    assert body["catch_up_samples"] == 0
    assert body["last_sample_id"] == status(client)["samples"]["last_id"]
    # Preview events are discarded; only the transition (if any) is issued.
    assert all(e["cause"] in ("live", "activation") for e in events(client))


def test_activation_marks_old_rule_retired_and_clears_trial_state(client):
    trial = client.post("/api/trials", json={"threshold_ms": 600, "consecutive": 2}).get_json()
    client.post(f"/api/trials/{trial['rule']['id']}/activate")
    rules = client.get("/api/rules").get_json()["items"]
    statuses = {r["version"]: r["status"] for r in rules}
    assert statuses[1] == "retired"
    assert statuses[2] == "active"
    assert client.get(f"/api/trials/{trial['rule']['id']}/events").get_json()["withheld_events"]["total"] == 0
    # Activating twice is a conflict.
    assert client.post(f"/api/trials/{trial['rule']['id']}/activate").status_code == 409


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


def test_cancel_keeps_original_rule_and_alert_state(client):
    post_samples(client, [900, 900, 900])
    assert status(client)["state"]["in_alert"] is True
    trial = client.post("/api/trials", json={"threshold_ms": 5000, "consecutive": 3}).get_json()

    res = client.post(f"/api/trials/{trial['rule']['id']}/cancel")
    assert res.status_code == 200
    body = res.get_json()
    assert body["cancelled"]["status"] == "cancelled"
    assert body["active"]["version"] == 1
    assert status(client)["state"]["in_alert"] is True
    assert status(client)["trial"] is None

    # Cancelled rule is still queryable in history; its withheld events are gone.
    rules = client.get("/api/rules").get_json()["items"]
    assert any(r["version"] == 2 and r["status"] == "cancelled" for r in rules)
    assert client.get(f"/api/trials/{trial['rule']['id']}/events").get_json()["withheld_events"]["total"] == 0
    # Issued log untouched by the trial.
    assert [e["rule_version"] for e in events(client)] == [1]


def test_cancel_wrong_status_conflicts(client):
    assert client.post("/api/trials/999/cancel").status_code == 404
    assert client.post("/api/trials/1/cancel").status_code == 409


# ---------------------------------------------------------------------------
# Retention & persistence
# ---------------------------------------------------------------------------


def test_retention_keeps_most_recent_samples(tmp_path):
    application = create_app(
        {
            "DATABASE": str(tmp_path / "retain.db"),
            "RETENTION": 10,
            "DEFAULT_THRESHOLD_MS": 500.0,
            "DEFAULT_CONSECUTIVE": 3,
        }
    )
    application.config["INIT_DB"]()
    c = application.test_client()
    post_samples(c, list(range(1, 26)))
    st = status(c)
    assert st["samples"]["count"] == 10
    assert st["samples"]["last_id"] == 25
    assert st["samples"]["first_id"] == 16


def test_state_survives_new_connection(tmp_path):
    path = str(tmp_path / "persist.db")
    app1 = create_app({"DATABASE": path, "DEFAULT_THRESHOLD_MS": 500.0, "DEFAULT_CONSECUTIVE": 3})
    app1.config["INIT_DB"]()
    post_samples(app1.test_client(), [900, 900, 900])

    app2 = create_app({"DATABASE": path, "DEFAULT_THRESHOLD_MS": 500.0, "DEFAULT_CONSECUTIVE": 3})
    app2.config["INIT_DB"]()
    st = status(app2.test_client())
    assert st["state"]["in_alert"] is True
    assert st["active_rule"]["version"] == 1
    assert st["samples"]["count"] == 3


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------


def test_health_and_index(client):
    assert client.get("/healthz").get_json()["ok"] is True
    res = client.get("/")
    assert res.status_code == 200
    assert b"<html" in res.data


def test_bulk_sample_endpoint_and_cap(client):
    res = client.post("/api/samples", json={"samples": [{"duration_ms": 1}, {"duration_ms": 2}]})
    assert res.status_code == 201
    assert res.get_json()["ingested"] == 2

    res = client.post("/api/samples", json={"samples": [{"duration_ms": 1}] * 1001})
    assert res.status_code == 413


def test_demo_reset(client):
    post_samples(client, [900, 900, 900])
    assert events(client)
    assert client.post("/api/demo/reset").status_code == 200
    st = status(client)
    assert st["samples"]["count"] == 0
    assert st["events_total"] == 0
    assert st["active_rule"]["version"] == 1
    assert events(client) == []


def test_activation_on_empty_dataset_with_alerting_rule_is_consistent(client):
    # No samples at all, but v2 (threshold 0 rules are rejected, so use 1ms x1)
    # can only be "alerting" relative to a sample.  It must not fabricate an episode.
    trial = client.post("/api/trials", json={"threshold_ms": 1, "consecutive": 1}).get_json()
    assert trial["preview"]["ends_in_alert"] is False
    body = client.post(f"/api/trials/{trial['rule']['id']}/activate").get_json()
    assert body["transition"] == "unchanged"
    assert body["state"]["in_alert"] is False
    assert events(client) == []
    st = status(client)
    assert st["state"]["in_alert"] is False
    assert st["state"]["open_episode"] is None


def test_batch_ingest_matches_sequential_results(client):
    values = [900, 900, 900, 100, 1200, 1200, 5]
    batch = client.post("/api/samples/seq", json={"values": values}).get_json()
    assert batch["ingested"] == len(values)
    kinds = [r["event"]["kind"] if r["event"] else None for r in batch["results"]]
    assert kinds == [None, None, "alert", "recovery", None, None, None]
    st = status(client)
    assert st["samples"]["count"] == len(values)
    assert st["state"]["in_alert"] is False
    assert st["state"]["consecutive"] == 0  # last sample 5ms is not a breach


def test_trial_created_from_snapshot_folds_in_samples_ingested_by_another_connection(tmp_path):
    """The replay snapshot is read outside the write lock.  A sample committed by
    another connection in that window must be included exactly once."""
    from alertsvc import db as dbmod

    path = str(tmp_path / "race.db")
    app = create_app(
        {"DATABASE": path, "DEFAULT_THRESHOLD_MS": 500.0, "DEFAULT_CONSECUTIVE": 3}
    )
    app.config["INIT_DB"]()
    client = app.test_client()
    post_samples(client, [900, 900])

    conn = dbmod.connect(path)
    original = dbmod.engine.simulate

    def simulate_then_race(samples, threshold, consecutive):
        result = original(samples, threshold, consecutive)
        # Another request commits a sample after the snapshot, before the lock.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO samples (duration_ms, received_at, source) VALUES (900, ?, 'race')",
            (1_700_000_000.0,),
        )
        conn.execute("COMMIT")
        return result

    dbmod.engine.simulate = simulate_then_race
    try:
        res = client.post("/api/trials", json={"threshold_ms": 800, "consecutive": 3})
    finally:
        dbmod.engine.simulate = original
        conn.close()

    assert res.status_code == 201
    body = res.get_json()
    # Preview covered 2 samples; the race sample is caught up, so the trial is
    # alerting after 3 breaches and no sample is double counted.
    rule = body["rule"]
    assert rule["preview"]["sample_count"] == 2
    assert rule["shadow"]["sample_count"] == 1
    status_body = status(client)
    assert status_body["trial"]["rule"]["shadow"]["sample_count"] == 1
    assert status_body["trial"]["state"]["in_alert"] is True
    assert status_body["samples"]["count"] == 3


def test_demo_seed_exercises_default_rule(client):
    res = client.post("/api/demo/seed", json={"rounds": 2})
    assert res.status_code == 201
    body = res.get_json()
    assert body["ingested"] == 12
    kinds = [e["kind"] for e in body["issued"]]
    assert kinds == ["alert", "recovery"] * 2, kinds
    assert status(client)["events_total"] == 4
