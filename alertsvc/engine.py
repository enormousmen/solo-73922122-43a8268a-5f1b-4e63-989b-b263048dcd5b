"""Pure rule-evaluation logic.

The engine is deliberately free of database concerns so that the exact same
code path is used for:
  * the live active rule,
  * a withheld trial rule, and
  * the historical preview computed when a trial is created.

Semantics
---------
* A sample breaches the rule when ``duration_ms > threshold_ms`` (strictly
  greater; a value equal to the threshold is not a breach).
* Consecutive breaches are counted.  An alert is emitted only when the count
  reaches ``consecutive`` while not already in alert.
* A single non-breaching sample resets the counter and immediately recovers an
  open alert.
* While already in alert, further breaches do not emit more alerts.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Sequence


@dataclass(frozen=True)
class EngineState:
    consecutive: int = 0
    in_alert: bool = False
    open_episode_id: int | None = None


@dataclass(frozen=True)
class StepOutcome:
    breached: bool
    event: str | None  # "alert" | "recovery" | None
    state: EngineState


@dataclass(frozen=True)
class SimulationEvent:
    kind: str
    sample_id: int
    sample_ts: float
    duration_ms: float
    threshold_ms: float
    consecutive: int


@dataclass(frozen=True)
class SimulationResult:
    state: EngineState
    events: Sequence[SimulationEvent]
    sample_count: int
    breach_count: int
    alert_count: int
    recovery_count: int
    max_consecutive: int
    first_sample_id: int | None
    last_sample_id: int | None
    last_sample_ts: float | None
    last_duration_ms: float | None


def is_breach(duration_ms: float, threshold_ms: float) -> bool:
    return duration_ms > threshold_ms


def advance(
    state: EngineState,
    duration_ms: float,
    threshold_ms: float,
    required_consecutive: int,
) -> StepOutcome:
    breached = is_breach(duration_ms, threshold_ms)

    if breached:
        consecutive = state.consecutive + 1
    else:
        consecutive = 0

    event: str | None = None
    in_alert = state.in_alert

    if breached:
        if not in_alert and consecutive >= required_consecutive:
            in_alert = True
            event = "alert"
    else:
        if in_alert:
            in_alert = False
            event = "recovery"

    new_state = replace(state, consecutive=consecutive, in_alert=in_alert)
    return StepOutcome(breached=breached, event=event, state=new_state)


def simulate(
    samples: Iterable,
    threshold_ms: float,
    required_consecutive: int,
) -> SimulationResult:
    """Run a rule over sample rows.

    ``samples`` may be sqlite3.Row objects or any mapping/object exposing
    ``id``, ``duration_ms`` and ``received_at``.  All samples must be in
    ascending ``id`` order.
    """

    state = EngineState()
    events: list[SimulationEvent] = []
    count = 0
    breaches = 0
    max_consecutive = 0
    first_id: int | None = None
    last_id: int | None = None
    last_ts: float | None = None
    last_duration: float | None = None

    def _get(row, key):
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            return getattr(row, key)

    for row in samples:
        sample_id = int(_get(row, "id"))
        duration = float(_get(row, "duration_ms"))
        received_at = float(_get(row, "received_at"))

        if first_id is None:
            first_id = sample_id
        last_id = sample_id
        last_ts = received_at
        last_duration = duration

        outcome = advance(state, duration, threshold_ms, required_consecutive)
        state = outcome.state
        count += 1
        if outcome.breached:
            breaches += 1
        if state.consecutive > max_consecutive:
            max_consecutive = state.consecutive
        if outcome.event:
            events.append(
                SimulationEvent(
                    kind=outcome.event,
                    sample_id=sample_id,
                    sample_ts=received_at,
                    duration_ms=duration,
                    threshold_ms=threshold_ms,
                    consecutive=state.consecutive,
                )
            )

    return SimulationResult(
        state=state,
        events=events,
        sample_count=count,
        breach_count=breaches,
        alert_count=sum(1 for e in events if e.kind == "alert"),
        recovery_count=sum(1 for e in events if e.kind == "recovery"),
        max_consecutive=max_consecutive,
        first_sample_id=first_id,
        last_sample_id=last_id,
        last_sample_ts=last_ts,
        last_duration_ms=last_duration,
    )
