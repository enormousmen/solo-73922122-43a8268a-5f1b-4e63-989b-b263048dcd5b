"""SQLite persistence and rule lifecycle.

Design notes
------------
* ``runtime_state`` is the authoritative live engine state.  Ingest advances it
  one sample at a time; it is never rebuilt from history.
* A trial rule is evaluated in *shadow* mode on every ingest, so when it is
  activated its state is already current with the latest sample.  Activation
  is therefore O(1) with respect to history and never re-scans samples.
* Trial preview/shadow events live in ``trial_events`` and are deliberately
  kept out of ``alert_events`` (the issued log) until the transition itself.
* One sample may be reported while a trial is pending: the old active rule
  continues to alert, while the trial shadows silently.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import replace
from typing import Any, Iterable

from . import engine

RETENTION_SAMPLES = 100_000
DEFAULT_THRESHOLD_MS = 500.0
DEFAULT_CONSECUTIVE = 3
SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rules (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    version               INTEGER NOT NULL UNIQUE,
    threshold_ms          REAL    NOT NULL,
    consecutive           INTEGER NOT NULL,
    status                TEXT    NOT NULL
                          CHECK (status IN ('active', 'retired', 'trial', 'cancelled')),
    created_at            REAL    NOT NULL,
    activated_at          REAL,
    retired_at            REAL,
    cancelled_at          REAL,
    preview_sample_count  INTEGER NOT NULL DEFAULT 0,
    preview_alert_count   INTEGER NOT NULL DEFAULT 0,
    preview_recovery_count INTEGER NOT NULL DEFAULT 0,
    preview_max_consecutive INTEGER NOT NULL DEFAULT 0,
    preview_cursor_sample_id INTEGER,
    preview_until_ts      REAL,
    shadow_sample_count   INTEGER NOT NULL DEFAULT 0,
    shadow_alert_count    INTEGER NOT NULL DEFAULT 0,
    shadow_recovery_count INTEGER NOT NULL DEFAULT 0,
    shadow_max_consecutive INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    duration_ms REAL    NOT NULL,
    received_at REAL    NOT NULL,
    source      TEXT    NOT NULL DEFAULT 'probe'
);

CREATE TABLE IF NOT EXISTS runtime_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    rule_id         INTEGER NOT NULL REFERENCES rules(id),
    consecutive     INTEGER NOT NULL DEFAULT 0,
    in_alert        INTEGER NOT NULL DEFAULT 0,
    open_episode_id INTEGER,
    last_sample_id  INTEGER,
    updated_at      REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS trial_state (
    rule_id        INTEGER PRIMARY KEY REFERENCES rules(id) ON DELETE CASCADE,
    consecutive    INTEGER NOT NULL DEFAULT 0,
    in_alert       INTEGER NOT NULL DEFAULT 0,
    last_sample_id INTEGER,
    updated_at     REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_episodes (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id            INTEGER NOT NULL REFERENCES rules(id),
    started_ts         REAL    NOT NULL,
    started_sample_id  INTEGER,
    started_duration_ms REAL,
    ended_ts           REAL,
    ended_sample_id    INTEGER,
    ended_duration_ms  REAL,
    end_cause          TEXT
);

CREATE INDEX IF NOT EXISTS idx_episodes_open
    ON alert_episodes (ended_ts) WHERE ended_ts IS NULL;

CREATE TABLE IF NOT EXISTS alert_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    kind          TEXT    NOT NULL CHECK (kind IN ('alert', 'recovery')),
    rule_id       INTEGER NOT NULL,
    rule_version  INTEGER NOT NULL,
    episode_id    INTEGER,
    sample_id     INTEGER,
    ts            REAL    NOT NULL,
    duration_ms   REAL,
    threshold_ms  REAL    NOT NULL,
    consecutive   INTEGER NOT NULL,
    cause         TEXT    NOT NULL DEFAULT 'live',
    details       TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_events_ts ON alert_events (ts DESC, id DESC);

CREATE TABLE IF NOT EXISTS trial_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id      INTEGER NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
    kind         TEXT    NOT NULL,
    sample_id    INTEGER,
    ts           REAL    NOT NULL,
    duration_ms  REAL,
    threshold_ms REAL    NOT NULL,
    consecutive  INTEGER NOT NULL,
    origin       TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trial_events_rule ON trial_events (rule_id, id);
"""


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db(
    conn: sqlite3.Connection,
    threshold_ms: float = DEFAULT_THRESHOLD_MS,
    consecutive: int = DEFAULT_CONSECUTIVE,
    now: float | None = None,
) -> None:
    now = time.time() if now is None else now
    conn.executescript(SCHEMA)

    # The bootstrap below is a check-then-insert; guard it so that several
    # workers starting at once cannot both create the initial active rule.
    transaction(conn)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )

        row = conn.execute("SELECT COUNT(*) AS n FROM rules").fetchone()
        if row["n"] == 0:
            conn.execute(
                "INSERT INTO rules (version, threshold_ms, consecutive, status, created_at, activated_at)"
                " VALUES (1, ?, ?, 'active', ?, ?)",
                (float(threshold_ms), int(consecutive), now, now),
            )
            rule = conn.execute("SELECT * FROM rules WHERE version = 1").fetchone()
            latest = conn.execute("SELECT MAX(id) AS mid FROM samples").fetchone()["mid"]
            conn.execute(
                "INSERT INTO runtime_state"
                " (id, rule_id, consecutive, in_alert, open_episode_id, last_sample_id, updated_at)"
                " VALUES (1, ?, 0, 0, NULL, ?, ?)",
                (rule["id"], latest, now),
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def transaction(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


def active_rule(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM rules WHERE status = 'active'").fetchone()


def pending_trial(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM rules WHERE status = 'trial'").fetchone()


def runtime_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM runtime_state WHERE id = 1").fetchone()


def open_episode(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM alert_episodes WHERE ended_ts IS NULL ORDER BY id DESC LIMIT 1"
    ).fetchone()


def rule_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    if row is None:
        return None
    keys = row.keys()
    return {
        "id": row["id"],
        "version": row["version"],
        "threshold_ms": row["threshold_ms"],
        "consecutive": row["consecutive"],
        "status": row["status"],
        "created_at": row["created_at"],
        "activated_at": row["activated_at"] if "activated_at" in keys else None,
        "retired_at": row["retired_at"] if "retired_at" in keys else None,
        "cancelled_at": row["cancelled_at"] if "cancelled_at" in keys else None,
        "preview": {
            "sample_count": row["preview_sample_count"],
            "alert_count": row["preview_alert_count"],
            "recovery_count": row["preview_recovery_count"],
            "max_consecutive": row["preview_max_consecutive"],
            "cursor_sample_id": row["preview_cursor_sample_id"],
            "until_ts": row["preview_until_ts"],
        },
        "shadow": {
            "sample_count": row["shadow_sample_count"],
            "alert_count": row["shadow_alert_count"],
            "recovery_count": row["shadow_recovery_count"],
            "max_consecutive": row["shadow_max_consecutive"],
        },
    }


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


def _insert_event(
    conn: sqlite3.Connection,
    *,
    kind: str,
    rule: sqlite3.Row,
    episode_id: int | None,
    sample_id: int | None,
    ts: float,
    duration_ms: float | None,
    consecutive: int,
    cause: str,
    details: dict | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO alert_events"
        " (kind, rule_id, rule_version, episode_id, sample_id, ts, duration_ms,"
        "  threshold_ms, consecutive, cause, details)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            kind,
            rule["id"],
            rule["version"],
            episode_id,
            sample_id,
            ts,
            duration_ms,
            rule["threshold_ms"],
            consecutive,
            cause,
            json.dumps(details, ensure_ascii=False) if details else None,
        ),
    )
    return int(cur.lastrowid)


def _open_episode_row(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    ts: float,
    sample_id: int | None,
    duration_ms: float | None,
) -> int:
    cur = conn.execute(
        "INSERT INTO alert_episodes"
        " (rule_id, started_ts, started_sample_id, started_duration_ms)"
        " VALUES (?, ?, ?, ?)",
        (rule["id"], ts, sample_id, duration_ms),
    )
    return int(cur.lastrowid)


def _close_episode_row(
    conn: sqlite3.Connection,
    episode_id: int,
    ts: float,
    sample_id: int | None,
    duration_ms: float | None,
    cause: str,
) -> None:
    conn.execute(
        "UPDATE alert_episodes"
        " SET ended_ts = ?, ended_sample_id = ?, ended_duration_ms = ?, end_cause = ?"
        " WHERE id = ? AND ended_ts IS NULL",
        (ts, sample_id, duration_ms, cause, episode_id),
    )


def ingest_samples(
    conn: sqlite3.Connection,
    durations: Iterable[float],
    *,
    now: float | None = None,
    source: str = "probe",
    retention: int = RETENTION_SAMPLES,
) -> list[dict[str, Any]]:
    """Ingest a batch in one transaction, in order, exactly as if reported
    one at a time.  Pruning happens once for the whole batch.
    """
    now = time.time() if now is None else now
    values = [float(v) for v in durations]
    if not values:
        return []

    transaction(conn)
    try:
        rule = active_rule(conn)
        if rule is None:  # defensive: never expected after init
            raise RuntimeError("no active rule")
        state_row = runtime_state(conn)
        state = engine.EngineState(
            consecutive=state_row["consecutive"],
            in_alert=bool(state_row["in_alert"]),
            open_episode_id=state_row["open_episode_id"],
        )
        rule_dict = rule_to_dict(rule)
        trial_ctx = _load_trial_context(conn)
        results = []
        last_sample_id = state_row["last_sample_id"]
        for duration_ms in values:
            result = _ingest_one(
                conn, rule, rule_dict, state, duration_ms, now, source,
                trial_ctx=trial_ctx,
            )
            state = result.pop("_new_state")
            last_sample_id = result["sample"]["id"]
            results.append(result)
        _flush_trial_context(conn, trial_ctx, now)
        final_state = state
        conn.execute(
            "UPDATE runtime_state SET consecutive = ?, in_alert = ?, open_episode_id = ?,"
            " last_sample_id = ?, updated_at = ? WHERE id = 1",
            (
                final_state.consecutive,
                1 if final_state.in_alert else 0,
                final_state.open_episode_id,
                last_sample_id,
                now,
            ),
        )
        if retention > 0:
            conn.execute(
                "DELETE FROM samples WHERE id <= (SELECT MAX(id) FROM samples) - ?",
                (retention,),
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

    return results


def _load_trial_context(conn: sqlite3.Connection) -> dict[str, Any] | None:
    trial = pending_trial(conn)
    if trial is None:
        return None
    st = conn.execute(
        "SELECT * FROM trial_state WHERE rule_id = ?", (trial["id"],)
    ).fetchone()
    if st is None:
        return None
    return {
        "rule": trial,
        "consecutive": st["consecutive"],
        "in_alert": bool(st["in_alert"]),
        "samples": trial["shadow_sample_count"],
        "alerts": trial["shadow_alert_count"],
        "recoveries": trial["shadow_recovery_count"],
        "max_consecutive": trial["shadow_max_consecutive"],
        "last_sample_id": st["last_sample_id"],
        "dirty": False,
    }


def _flush_trial_context(
    conn: sqlite3.Connection, ctx: dict[str, Any] | None, now: float
) -> None:
    if ctx is None or not ctx["dirty"]:
        return
    trial = ctx["rule"]
    conn.execute(
        "UPDATE trial_state SET consecutive = ?, in_alert = ?, last_sample_id = ?,"
        " updated_at = ? WHERE rule_id = ?",
        (
            ctx["consecutive"],
            1 if ctx["in_alert"] else 0,
            ctx["last_sample_id"],
            now,
            trial["id"],
        ),
    )
    conn.execute(
        "UPDATE rules SET shadow_sample_count = ?, shadow_alert_count = ?,"
        " shadow_recovery_count = ?, shadow_max_consecutive = ? WHERE id = ?",
        (
            ctx["samples"],
            ctx["alerts"],
            ctx["recoveries"],
            ctx["max_consecutive"],
            trial["id"],
        ),
    )


def _ingest_one(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    rule_dict: dict[str, Any],
    state: engine.EngineState,
    duration_ms: float,
    now: float,
    source: str,
    trial_ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Advance the live rule by one sample.  Caller owns the transaction and
    threads the returned ``new_state`` into the next call.
    """
    cur = conn.execute(
        "INSERT INTO samples (duration_ms, received_at, source) VALUES (?, ?, ?)",
        (float(duration_ms), now, source),
    )
    sample_id = int(cur.lastrowid)

    outcome = engine.advance(
        state, float(duration_ms), float(rule["threshold_ms"]), int(rule["consecutive"])
    )

    event_payload = None
    new_episode_id = outcome.state.open_episode_id

    if outcome.event == "alert":
        episode_id = _open_episode_row(
            conn, rule, now, sample_id, float(duration_ms)
        )
        new_episode_id = episode_id
        _insert_event(
            conn,
            kind="alert",
            rule=rule,
            episode_id=episode_id,
            sample_id=sample_id,
            ts=now,
            duration_ms=float(duration_ms),
            consecutive=outcome.state.consecutive,
            cause="live",
        )
        event_payload = {
            "kind": "alert",
            "episode_id": episode_id,
            "sample_id": sample_id,
            "rule_version": rule["version"],
        }
    elif outcome.event == "recovery":
        episode_id = state.open_episode_id
        if episode_id is not None:
            _close_episode_row(
                conn,
                episode_id,
                now,
                sample_id,
                float(duration_ms),
                "below_threshold",
            )
        _insert_event(
            conn,
            kind="recovery",
            rule=rule,
            episode_id=episode_id,
            sample_id=sample_id,
            ts=now,
            duration_ms=float(duration_ms),
            consecutive=0,
            cause="live",
        )
        new_episode_id = None
        event_payload = {
            "kind": "recovery",
            "episode_id": episode_id,
            "sample_id": sample_id,
            "rule_version": rule["version"],
        }

    trial_summary = (
        _shadow_trial(conn, float(duration_ms), now, sample_id, ctx=trial_ctx)
        if trial_ctx is not None
        else None
    )

    return {
        "sample": {
            "id": sample_id,
            "duration_ms": float(duration_ms),
            "received_at": now,
            "source": source,
        },
        "breached": outcome.breached,
        "event": event_payload,
        "state": {
            "consecutive": outcome.state.consecutive,
            "in_alert": outcome.state.in_alert,
            "open_episode_id": new_episode_id,
        },
        "rule": rule_dict,
        "trial": trial_summary,
        "_new_state": replace(outcome.state, open_episode_id=new_episode_id),
    }


def _shadow_trial(
    conn: sqlite3.Connection,
    duration_ms: float,
    now: float,
    sample_id: int,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Advance the pending trial silently.  Never emits issued events."""
    owned = ctx is None
    if owned:
        ctx = _load_trial_context(conn)
    if ctx is None:
        return None
    trial = ctx["rule"]

    state = engine.EngineState(
        consecutive=ctx["consecutive"], in_alert=ctx["in_alert"]
    )
    outcome = engine.advance(
        state, duration_ms, float(trial["threshold_ms"]), int(trial["consecutive"])
    )

    ctx["consecutive"] = outcome.state.consecutive
    ctx["in_alert"] = outcome.state.in_alert
    ctx["samples"] += 1
    ctx["dirty"] = True
    if outcome.event == "alert":
        ctx["alerts"] += 1
    elif outcome.event == "recovery":
        ctx["recoveries"] += 1
    if outcome.state.consecutive > ctx["max_consecutive"]:
        ctx["max_consecutive"] = outcome.state.consecutive
    ctx["last_sample_id"] = sample_id

    if outcome.event:
        conn.execute(
            "INSERT INTO trial_events"
            " (rule_id, kind, sample_id, ts, duration_ms, threshold_ms, consecutive, origin)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'shadow')",
            (
                trial["id"],
                outcome.event,
                sample_id,
                now,
                duration_ms,
                float(trial["threshold_ms"]),
                outcome.state.consecutive,
            ),
        )

    if owned:
        _flush_trial_context(conn, ctx, now)

    return {
        "rule_id": trial["id"],
        "consecutive": outcome.state.consecutive,
        "in_alert": outcome.state.in_alert,
        "shadow_alert_count": ctx["alerts"],
        "shadow_recovery_count": ctx["recoveries"],
    }


# ---------------------------------------------------------------------------
# Trial lifecycle
# ---------------------------------------------------------------------------


def create_trial(
    conn: sqlite3.Connection,
    threshold_ms: float,
    consecutive: int,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    now = time.time() if now is None else now

    # Replay reads a snapshot *outside* the write transaction: a 100k-sample
    # preview must not block ingestion behind SQLite's single writer.
    if pending_trial(conn) is not None:
        raise TrialConflict("a trial rule is already pending")
    rows = conn.execute(
        "SELECT id, duration_ms, received_at FROM samples ORDER BY id ASC"
    ).fetchall()
    result = engine.simulate(rows, float(threshold_ms), int(consecutive))

    transaction(conn)
    try:
        if pending_trial(conn) is not None:
            raise TrialConflict("a trial rule is already pending")
        max_version = conn.execute("SELECT COALESCE(MAX(version), 0) AS v FROM rules").fetchone()["v"]

        cur = conn.execute(
            "INSERT INTO rules (version, threshold_ms, consecutive, status, created_at,"
            " preview_sample_count, preview_alert_count, preview_recovery_count,"
            " preview_max_consecutive, preview_cursor_sample_id, preview_until_ts,"
            " shadow_max_consecutive)"
            " VALUES (?, ?, ?, 'trial', ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(max_version) + 1,
                float(threshold_ms),
                int(consecutive),
                now,
                result.sample_count,
                result.alert_count,
                result.recovery_count,
                result.max_consecutive,
                result.last_sample_id,
                result.last_sample_ts,
                result.state.consecutive,
            ),
        )
        trial_id = int(cur.lastrowid)

        conn.execute(
            "INSERT INTO trial_state"
            " (rule_id, consecutive, in_alert, last_sample_id, updated_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                trial_id,
                result.state.consecutive,
                1 if result.state.in_alert else 0,
                result.last_sample_id,
                now,
            ),
        )

        for ev in result.events:
            conn.execute(
                "INSERT INTO trial_events"
                " (rule_id, kind, sample_id, ts, duration_ms, threshold_ms, consecutive, origin)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'preview')",
                (
                    trial_id,
                    ev.kind,
                    ev.sample_id,
                    ev.sample_ts,
                    ev.duration_ms,
                    ev.threshold_ms,
                    ev.consecutive,
                ),
            )

        # The replay snapshot was read before this write lock; samples ingested
        # in between are folded in by advancing only the tail past the cursor.
        stored = conn.execute("SELECT * FROM rules WHERE id = ?", (trial_id,)).fetchone()
        _catch_up_trial(conn, stored, now)

        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

    trial = conn.execute("SELECT * FROM rules WHERE id = ?", (trial_id,)).fetchone()
    return {
        "rule": rule_to_dict(trial),
        "preview": {
            "sample_count": result.sample_count,
            "breach_count": result.breach_count,
            "alert_count": result.alert_count,
            "recovery_count": result.recovery_count,
            "max_consecutive": result.max_consecutive,
            "cursor_sample_id": result.last_sample_id,
            "until_ts": result.last_sample_ts,
            "ends_in_alert": result.state.in_alert,
            "final_consecutive": result.state.consecutive,
        },
        "preview_events": [_event_dict(e) for e in result.events],
    }


def _event_dict(e) -> dict[str, Any]:
    return {
        "kind": e.kind,
        "sample_id": e.sample_id,
        "ts": e.sample_ts,
        "duration_ms": e.duration_ms,
        "threshold_ms": e.threshold_ms,
        "consecutive": e.consecutive,
    }


def _catch_up_trial(
    conn: sqlite3.Connection, trial: sqlite3.Row, now: float
) -> int:
    """Advance a trial over samples newer than its cursor.

    Only the tail is scanned; historical samples are never re-evaluated.
    Normally this is a no-op because ingest shadows every sample.
    """
    st = conn.execute(
        "SELECT * FROM trial_state WHERE rule_id = ?", (trial["id"],)
    ).fetchone()
    if st is None:
        raise TrialConflict("trial state is missing")
    last = st["last_sample_id"] or 0
    rows = conn.execute(
        "SELECT id, duration_ms, received_at FROM samples WHERE id > ? ORDER BY id ASC",
        (last,),
    ).fetchall()
    if not rows:
        return 0

    state = engine.EngineState(
        consecutive=st["consecutive"], in_alert=bool(st["in_alert"])
    )
    added_alerts = 0
    added_recoveries = 0
    max_consecutive = trial["shadow_max_consecutive"]
    cursor = last
    for row in rows:
        outcome = engine.advance(
            state, float(row["duration_ms"]), float(trial["threshold_ms"]),
            int(trial["consecutive"]),
        )
        state = outcome.state
        cursor = int(row["id"])
        if outcome.state.consecutive > max_consecutive:
            max_consecutive = outcome.state.consecutive
        if outcome.event == "alert":
            added_alerts += 1
        elif outcome.event == "recovery":
            added_recoveries += 1
        if outcome.event:
            conn.execute(
                "INSERT INTO trial_events"
                " (rule_id, kind, sample_id, ts, duration_ms, threshold_ms, consecutive, origin)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'catchup')",
                (
                    trial["id"],
                    outcome.event,
                    row["id"],
                    row["received_at"],
                    float(row["duration_ms"]),
                    float(trial["threshold_ms"]),
                    state.consecutive,
                ),
            )

    conn.execute(
        "UPDATE trial_state SET consecutive = ?, in_alert = ?, last_sample_id = ?,"
        " updated_at = ? WHERE rule_id = ?",
        (state.consecutive, 1 if state.in_alert else 0, cursor, now, trial["id"]),
    )
    conn.execute(
        "UPDATE rules SET shadow_sample_count = shadow_sample_count + ?,"
        " shadow_alert_count = shadow_alert_count + ?,"
        " shadow_recovery_count = shadow_recovery_count + ?,"
        " shadow_max_consecutive = ? WHERE id = ?",
        (len(rows), added_alerts, added_recoveries, max_consecutive, trial["id"]),
    )
    return len(rows)


class TrialConflict(RuntimeError):
    pass


def activate_trial(
    conn: sqlite3.Connection,
    rule_id: int,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    transaction(conn)
    try:
        trial = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
        if trial is None:
            raise TrialNotFound(rule_id)
        if trial["status"] != "trial":
            raise TrialConflict(f"rule {rule_id} is '{trial['status']}', not 'trial'")

        old_rule = active_rule(conn)
        old_state = runtime_state(conn)
        catchup = _catch_up_trial(conn, trial, now)
        trial_state = conn.execute(
            "SELECT * FROM trial_state WHERE rule_id = ?", (rule_id,)
        ).fetchone()

        last_sample = conn.execute(
            "SELECT * FROM samples ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_id = last_sample["id"] if last_sample else None
        last_duration = last_sample["duration_ms"] if last_sample else None

        old_in_alert = bool(old_state["in_alert"])
        new_in_alert = bool(trial_state["in_alert"])
        new_consecutive = int(trial_state["consecutive"])

        open_row = open_episode(conn)
        kept_episode_id = open_row["id"] if open_row else None
        transition = "unchanged"
        event_payload = None

        if old_in_alert and new_in_alert:
            transition = "continued"
            if kept_episode_id is None:
                # Defensive repair: alert flag without an episode.
                kept_episode_id = _open_episode_row(conn, old_rule, now, last_id, last_duration)
        elif old_in_alert and not new_in_alert:
            transition = "recovered"
            if kept_episode_id is not None:
                _close_episode_row(
                    conn, kept_episode_id, now, last_id, last_duration, "rule_activation"
                )
            event_id = _insert_event(
                conn,
                kind="recovery",
                rule=trial,
                episode_id=kept_episode_id,
                sample_id=last_id,
                ts=now,
                duration_ms=last_duration,
                consecutive=new_consecutive,
                cause="activation",
                details={"previous_rule_version": old_rule["version"]},
            )
            event_payload = {"id": event_id, "kind": "recovery"}
            kept_episode_id = None
        elif not old_in_alert and new_in_alert and last_id is not None:
            transition = "alerted"
            kept_episode_id = _open_episode_row(conn, trial, now, last_id, last_duration)
            event_id = _insert_event(
                conn,
                kind="alert",
                rule=trial,
                episode_id=kept_episode_id,
                sample_id=last_id,
                ts=now,
                duration_ms=last_duration,
                consecutive=new_consecutive,
                cause="activation",
                details={"previous_rule_version": old_rule["version"]},
            )
            event_payload = {"id": event_id, "kind": "alert"}
        else:
            # Neither alerting, or a rule that only alerts relative to a sample
            # that does not exist yet (empty dataset).
            transition = "unchanged"
            kept_episode_id = None

        effective_in_alert = new_in_alert and kept_episode_id is not None
        conn.execute(
            "UPDATE runtime_state SET rule_id = ?, consecutive = ?, in_alert = ?,"
            " open_episode_id = ?, last_sample_id = ?, updated_at = ? WHERE id = 1",
            (
                trial["id"],
                new_consecutive,
                1 if effective_in_alert else 0,
                kept_episode_id,
                last_id,
                now,
            ),
        )
        conn.execute(
            "UPDATE rules SET status = 'retired', retired_at = ? WHERE id = ?",
            (now, old_rule["id"]),
        )
        conn.execute(
            "UPDATE rules SET status = 'active', activated_at = ? WHERE id = ?",
            (now, trial["id"]),
        )
        conn.execute("DELETE FROM trial_state WHERE rule_id = ?", (rule_id,))
        # Withheld trial events never become issued records.
        conn.execute("DELETE FROM trial_events WHERE rule_id = ?", (rule_id,))

        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

    new_rule = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
    return {
        "activated": rule_to_dict(new_rule),
        "previous": rule_to_dict(old_rule),
        "transition": transition,
        "event": event_payload,
        "state": {
            "consecutive": new_consecutive,
            "in_alert": new_in_alert and kept_episode_id is not None,
            "open_episode_id": kept_episode_id,
        },
        "catch_up_samples": catchup,
        "last_sample_id": last_id,
    }


class TrialNotFound(LookupError):
    pass


def cancel_trial(
    conn: sqlite3.Connection, rule_id: int, *, now: float | None = None
) -> dict[str, Any]:
    now = time.time() if now is None else now
    transaction(conn)
    try:
        trial = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
        if trial is None:
            raise TrialNotFound(rule_id)
        if trial["status"] != "trial":
            raise TrialConflict(f"rule {rule_id} is '{trial['status']}', not 'trial'")
        conn.execute(
            "UPDATE rules SET status = 'cancelled', cancelled_at = ? WHERE id = ?",
            (now, rule_id),
        )
        conn.execute("DELETE FROM trial_state WHERE rule_id = ?", (rule_id,))
        conn.execute("DELETE FROM trial_events WHERE rule_id = ?", (rule_id,))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

    cancelled = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
    return {
        "cancelled": rule_to_dict(cancelled),
        "active": rule_to_dict(active_rule(conn)),
        "state": _state_dict(conn),
    }


# ---------------------------------------------------------------------------
# Read models
# ---------------------------------------------------------------------------


def _state_dict(conn: sqlite3.Connection) -> dict[str, Any]:
    st = runtime_state(conn)
    ep = open_episode(conn) if st["in_alert"] else None
    return {
        "consecutive": st["consecutive"],
        "in_alert": bool(st["in_alert"]),
        "open_episode": _episode_dict(ep) if ep else None,
        "last_sample_id": st["last_sample_id"],
        "updated_at": st["updated_at"],
    }


def _episode_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "rule_id": row["rule_id"],
        "started_ts": row["started_ts"],
        "started_sample_id": row["started_sample_id"],
        "started_duration_ms": row["started_duration_ms"],
        "ended_ts": row["ended_ts"],
        "ended_sample_id": row["ended_sample_id"],
        "ended_duration_ms": row["ended_duration_ms"],
        "end_cause": row["end_cause"],
    }


def status(conn: sqlite3.Connection, retention: int = RETENTION_SAMPLES) -> dict[str, Any]:
    rule = active_rule(conn)
    samples = conn.execute(
        "SELECT COUNT(*) AS n, MIN(id) AS first_id, MAX(id) AS last_id,"
        " MAX(received_at) AS last_ts FROM samples"
    ).fetchone()
    episodes = conn.execute("SELECT COUNT(*) AS n FROM alert_episodes").fetchone()["n"]
    events = conn.execute("SELECT COUNT(*) AS n FROM alert_events").fetchone()["n"]

    trial_row = pending_trial(conn)
    trial = None
    if trial_row is not None:
        tsrow = conn.execute(
            "SELECT * FROM trial_state WHERE rule_id = ?", (trial_row["id"],)
        ).fetchone()
        trial = {
            "rule": rule_to_dict(trial_row),
            "state": {
                "consecutive": tsrow["consecutive"] if tsrow else 0,
                "in_alert": bool(tsrow["in_alert"]) if tsrow else False,
                "last_sample_id": tsrow["last_sample_id"] if tsrow else None,
            },
        }

    return {
        "active_rule": rule_to_dict(rule),
        "state": _state_dict(conn),
        "samples": {
            "count": samples["n"],
            "first_id": samples["first_id"],
            "last_id": samples["last_id"],
            "last_received_at": samples["last_ts"],
            "retention": retention,
        },
        "episodes_total": episodes,
        "events_total": events,
        "trial": trial,
        "server_time": time.time(),
    }


def list_events(
    conn: sqlite3.Connection, limit: int = 50, offset: int = 0, kind: str | None = None
) -> dict[str, Any]:
    where = ""
    params: list[Any] = []
    if kind in ("alert", "recovery"):
        where = "WHERE kind = ?"
        params.append(kind)
    total = conn.execute(
        f"SELECT COUNT(*) AS n FROM alert_events {where}", params
    ).fetchone()["n"]
    rows = conn.execute(
        f"SELECT e.*, ep.started_ts AS episode_started_ts, ep.ended_ts AS episode_ended_ts"
        f" FROM alert_events e LEFT JOIN alert_episodes ep ON ep.id = e.episode_id"
        f" {where} ORDER BY e.id DESC LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [
            {
                "id": r["id"],
                "kind": r["kind"],
                "rule_id": r["rule_id"],
                "rule_version": r["rule_version"],
                "episode_id": r["episode_id"],
                "sample_id": r["sample_id"],
                "ts": r["ts"],
                "duration_ms": r["duration_ms"],
                "threshold_ms": r["threshold_ms"],
                "consecutive": r["consecutive"],
                "cause": r["cause"],
                "details": json.loads(r["details"]) if r["details"] else None,
                "episode_started_ts": r["episode_started_ts"],
                "episode_ended_ts": r["episode_ended_ts"],
            }
            for r in rows
        ],
    }


def list_samples(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM samples ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "id": r["id"],
            "duration_ms": r["duration_ms"],
            "received_at": r["received_at"],
            "source": r["source"],
        }
        for r in rows
    ]


def list_rules(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM rules ORDER BY version DESC LIMIT ?", (limit,)
    ).fetchall()
    return [rule_to_dict(r) for r in rows]


def trial_events(
    conn: sqlite3.Connection, rule_id: int, limit: int = 100
) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT * FROM trial_events WHERE rule_id = ? ORDER BY id DESC LIMIT ?",
        (rule_id, limit),
    ).fetchall()
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM trial_events WHERE rule_id = ?", (rule_id,)
    ).fetchone()["n"]
    return {
        "total": total,
        "items": [
            {
                "kind": r["kind"],
                "sample_id": r["sample_id"],
                "ts": r["ts"],
                "duration_ms": r["duration_ms"],
                "threshold_ms": r["threshold_ms"],
                "consecutive": r["consecutive"],
                "origin": r["origin"],
            }
            for r in rows
        ],
    }


def reset_demo(
    conn: sqlite3.Connection,
    threshold_ms: float = DEFAULT_THRESHOLD_MS,
    consecutive: int = DEFAULT_CONSECUTIVE,
    *,
    now: float | None = None,
) -> None:
    """Destructive demo reset: wipes samples, events, episodes and rules."""
    now = time.time() if now is None else now
    transaction(conn)
    try:
        for table in (
            "alert_events",
            "alert_episodes",
            "trial_events",
            "trial_state",
            "runtime_state",
            "samples",
            "rules",
        ):
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM sqlite_sequence")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    init_db(conn, threshold_ms=threshold_ms, consecutive=consecutive, now=now)
