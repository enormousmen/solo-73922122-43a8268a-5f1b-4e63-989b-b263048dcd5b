"""Flask HTTP surface for the latency alerting service."""

from __future__ import annotations

import math
import os
import sqlite3
import time
from typing import Any

from flask import Flask, g, jsonify, render_template, request

from . import db

MAX_BODY_SAMPLES = 1000


def _parse_positive_float(raw: Any, field: str, *, allow_zero: bool = False) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ApiError(f"{field} must be a number", 400)
    if not math.isfinite(value):
        raise ApiError(f"{field} must be finite", 400)
    if value < 0 or (value == 0 and not allow_zero):
        raise ApiError(f"{field} must be {'non-negative' if allow_zero else 'positive'}", 400)
    return value


def _parse_consecutive(raw: Any) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ApiError("consecutive must be an integer", 400)
    if not 1 <= value <= 1000:
        raise ApiError("consecutive must be between 1 and 1000", 400)
    return value


class ApiError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


def create_app(overrides: dict[str, Any] | None = None) -> Flask:
    overrides = overrides or {}
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.update(
        DATABASE=overrides.get("DATABASE", os.environ.get("DATABASE", "/data/alerts.db")),
        RETENTION=overrides.get(
            "RETENTION", int(os.environ.get("RETENTION", db.RETENTION_SAMPLES))
        ),
        DEFAULT_THRESHOLD_MS=overrides.get("DEFAULT_THRESHOLD_MS", db.DEFAULT_THRESHOLD_MS),
        DEFAULT_CONSECUTIVE=overrides.get("DEFAULT_CONSECUTIVE", db.DEFAULT_CONSECUTIVE),
        JSON_SORT_KEYS=False,
    )

    def get_db() -> sqlite3.Connection:
        if "db" not in g:
            g.db = db.connect(app.config["DATABASE"])
        return g.db

    @app.teardown_appcontext
    def close_db(_exc: BaseException | None) -> None:
        conn = g.pop("db", None)
        if conn is not None:
            conn.close()

    def init() -> None:
        path = app.config["DATABASE"]
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        conn = db.connect(path)
        try:
            db.init_db(
                conn,
                threshold_ms=app.config["DEFAULT_THRESHOLD_MS"],
                consecutive=app.config["DEFAULT_CONSECUTIVE"],
            )
        finally:
            conn.close()

    app.config["INIT_DB"] = init

    @app.errorhandler(ApiError)
    def handle_api_error(err: ApiError):
        return jsonify({"error": err.message}), err.status

    @app.errorhandler(db.TrialConflict)
    def handle_conflict(err: db.TrialConflict):
        return jsonify({"error": str(err)}), 409

    @app.errorhandler(db.TrialNotFound)
    def handle_notfound(err: db.TrialNotFound):
        return jsonify({"error": f"trial rule {err} not found"}), 404

    # -- pages ------------------------------------------------------------
    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/healthz")
    def healthz():
        row = get_db().execute("SELECT 1 AS ok").fetchone()
        return jsonify({"ok": bool(row["ok"]), "time": time.time()})

    # -- ingest -----------------------------------------------------------
    @app.post("/api/samples")
    def post_samples():
        payload = request.get_json(silent=True)
        if payload is None:
            raise ApiError("JSON body required", 400)
        if isinstance(payload, dict):
            items = payload.get("samples")
            if items is None:
                items = [payload]
        elif isinstance(payload, list):
            items = payload
        else:
            raise ApiError("JSON object or array required", 400)

        if not isinstance(items, list) or not items:
            raise ApiError("at least one sample is required", 400)
        if len(items) > MAX_BODY_SAMPLES:
            raise ApiError(f"at most {MAX_BODY_SAMPLES} samples per request", 413)

        prepared: list[float] = []
        for item in items:
            if not isinstance(item, dict) or "duration_ms" not in item:
                raise ApiError("each sample needs duration_ms", 400)
            prepared.append(_parse_positive_float(item["duration_ms"], "duration_ms", allow_zero=True))

        results = db.ingest_samples(
            get_db(),
            prepared,
            now=time.time(),
            source="api",
            retention=app.config["RETENTION"],
        )
        return jsonify({"ingested": len(results), "results": results}), 201

    @app.post("/api/samples/seq")
    def post_sample_sequence():
        """Demo convenience: ingest a list of durations in one call."""
        payload = request.get_json(silent=True) or {}
        values = payload.get("values")
        if not isinstance(values, list) or not values:
            raise ApiError("values must be a non-empty array", 400)
        if len(values) > MAX_BODY_SAMPLES:
            raise ApiError(f"at most {MAX_BODY_SAMPLES} samples per request", 413)
        prepared = [_parse_positive_float(v, "value", allow_zero=True) for v in values]
        results = db.ingest_samples(
            get_db(),
            prepared,
            now=time.time(),
            source="demo",
            retention=app.config["RETENTION"],
        )
        return jsonify({"ingested": len(results), "results": results}), 201

    # -- reads ------------------------------------------------------------
    @app.get("/api/status")
    def get_status():
        return jsonify(db.status(get_db(), retention=app.config["RETENTION"]))

    @app.get("/api/events")
    def get_events():
        limit = _int_arg("limit", 50, 1, 500)
        offset = _int_arg("offset", 0, 0, 10_000_000)
        kind = request.args.get("kind")
        return jsonify(db.list_events(get_db(), limit=limit, offset=offset, kind=kind))

    @app.get("/api/samples")
    def get_samples():
        limit = _int_arg("limit", 50, 1, 500)
        return jsonify({"items": db.list_samples(get_db(), limit=limit)})

    @app.get("/api/rules")
    def get_rules():
        limit = _int_arg("limit", 50, 1, 200)
        return jsonify({"items": db.list_rules(get_db(), limit=limit)})

    # -- trials -----------------------------------------------------------
    @app.post("/api/trials")
    def create_trial():
        payload = request.get_json(silent=True) or {}
        threshold = _parse_positive_float(payload.get("threshold_ms"), "threshold_ms")
        consecutive = _parse_consecutive(payload.get("consecutive"))
        result = db.create_trial(get_db(), threshold, consecutive)
        return jsonify(result), 201

    @app.get("/api/trials/<int:rule_id>/events")
    def get_trial_events(rule_id: int):
        conn = get_db()
        rule = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
        if rule is None:
            raise db.TrialNotFound(rule_id)
        limit = _int_arg("limit", 100, 1, 500)
        return jsonify(
            {
                "rule": db.rule_to_dict(rule),
                "withheld_events": db.trial_events(conn, rule_id, limit=limit),
            }
        )

    @app.post("/api/trials/<int:rule_id>/activate")
    def activate_trial(rule_id: int):
        result = db.activate_trial(get_db(), rule_id)
        return jsonify(result)

    @app.post("/api/trials/<int:rule_id>/cancel")
    def cancel_trial(rule_id: int):
        result = db.cancel_trial(get_db(), rule_id)
        return jsonify(result)

    @app.post("/api/demo/seed")
    def demo_seed():
        """Emit a small deterministic traffic burst for the demo page."""
        payload = request.get_json(silent=True) or {}
        try:
            rounds = int(payload.get("rounds", 6))
        except (TypeError, ValueError):
            raise ApiError("rounds must be an integer", 400)
        rounds = max(1, min(rounds, 20))
        now = time.time()
        conn = get_db()
        # Three consecutive breaches (820/900/950) then a clean sample, so the
        # default 500ms x3 rule visibly alerts and recovers.
        values: list[float] = []
        for i in range(rounds):
            values.extend([120.0 + i * 5, 450.0, 820.0, 900.0, 950.0, 300.0])
        results = db.ingest_samples(
            conn, values, now=now, source="demo", retention=app.config["RETENTION"]
        )
        return jsonify(
            {
                "ingested": len(results),
                "issued": [r["event"] for r in results if r["event"]],
            }
        ), 201

    @app.post("/api/demo/reset")
    def demo_reset():
        db.reset_demo(
            get_db(),
            threshold_ms=app.config["DEFAULT_THRESHOLD_MS"],
            consecutive=app.config["DEFAULT_CONSECUTIVE"],
        )
        return jsonify({"reset": True})

    def _int_arg(name: str, default: int, low: int, high: int) -> int:
        raw = request.args.get(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            raise ApiError(f"{name} must be an integer", 400)
        if value < low or value > high:
            raise ApiError(f"{name} must be between {low} and {high}", 400)
        return value

    @app.errorhandler(405)
    def method_not_allowed(_err):
        if request.path.startswith("/api/"):
            return jsonify({"error": "method not allowed"}), 405
        return render_template("index.html"), 405

    @app.errorhandler(404)
    def not_found(_err):
        if request.path.startswith("/api/"):
            return jsonify({"error": "not found"}), 404
        return render_template("index.html"), 404

    return app
