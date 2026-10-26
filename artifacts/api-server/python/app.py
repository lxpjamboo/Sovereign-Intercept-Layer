from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("sovereign.intercept")

THRESHOLD = 15.0
DB_PATH = Path(
    os.getenv(
        "SOVEREIGN_DB_PATH",
        str(Path(__file__).resolve().parents[1] / "data" / "sovereign.sqlite3"),
    )
)

EXPECTED_FIELDS: dict[str, set[str]] = {
    "system_log": {"message", "level", "host", "timestamp"},
    "user_input": {"session_id", "action", "value", "timestamp"},
    "database_transaction": {
        "user_id",
        "operation",
        "table",
        "row_count",
        "authorized",
    },
}


class EventType(str, Enum):
    system_log = "system_log"
    user_input = "user_input"
    database_transaction = "database_transaction"
    air_gap_intercept = "air_gap_intercept"


class IngestInput(BaseModel):
    event_type: Literal["system_log", "user_input", "database_transaction"]
    source: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any]


class TelemetryEvent(BaseModel):
    id: int
    created_at: datetime
    event_type: EventType
    source: str
    payload: dict[str, Any]
    drift_score: float
    quarantined: bool
    intercept_reason: str | None


class SystemStatus(BaseModel):
    state: str
    event_count: int
    intercepted_count: int
    threshold: float
    avg_latency_ms: float
    last_event_at: datetime | None


app = FastAPI(
    title="The Sovereign Intercept Layer",
    description="Offline-first telemetry ingestion and deterministic drift interception.",
    version="1.0.0",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def initialize_database() -> None:
    with get_connection() as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                drift_score REAL NOT NULL,
                quarantined INTEGER NOT NULL DEFAULT 0,
                intercept_reason TEXT,
                processing_latency_ms REAL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_live_summary
            ON telemetry_events (event_type, quarantined, id)
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO system_state (id, state, updated_at)
            VALUES (1, 'active', ?)
            """,
            (utc_now().isoformat(),),
        )


@app.on_event("startup")
def startup() -> None:
    initialize_database()
    logger.info("sqlite_ready path=%s", DB_PATH)


def is_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.tzinfo is not None
    except ValueError:
        return False


def formatting_issues(event_type: str, payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if event_type == "system_log":
        if not isinstance(payload.get("message"), str) or not payload["message"].strip():
            issues.append("message must be a non-empty string")
        if payload.get("level") not in {"DEBUG", "INFO", "WARN", "ERROR"}:
            issues.append("level must be DEBUG, INFO, WARN, or ERROR")
        if not isinstance(payload.get("host"), str) or not payload["host"].strip():
            issues.append("host must be a non-empty string")
        if not is_iso_timestamp(payload.get("timestamp")):
            issues.append("timestamp must be ISO 8601 with a timezone")
    elif event_type == "user_input":
        if not isinstance(payload.get("session_id"), str) or not payload["session_id"].strip():
            issues.append("session_id must be a non-empty string")
        if not isinstance(payload.get("action"), str) or not payload["action"].strip():
            issues.append("action must be a non-empty string")
        if not isinstance(payload.get("timestamp"), str) or not is_iso_timestamp(
            payload.get("timestamp")
        ):
            issues.append("timestamp must be ISO 8601 with a timezone")
        if "value" not in payload:
            issues.append("value is required")
    elif event_type == "database_transaction":
        user_id = payload.get("user_id")
        if isinstance(user_id, bool) or not isinstance(user_id, (int, str)):
            issues.append("user_id must be a string or integer")
        if payload.get("operation") not in {"SELECT", "INSERT", "UPDATE", "DELETE"}:
            issues.append("operation must be SELECT, INSERT, UPDATE, or DELETE")
        if not isinstance(payload.get("table"), str) or not payload["table"].strip():
            issues.append("table must be a non-empty string")
        row_count = payload.get("row_count")
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
            issues.append("row_count must be a non-negative integer")
        if payload.get("authorized") is not True:
            issues.append("transaction is not explicitly authorized")
    return issues


def calculate_drift(event_type: str, payload: dict[str, Any]) -> tuple[float, str | None]:
    expected = EXPECTED_FIELDS[event_type]
    actual = set(payload)
    union = expected | actual
    changed_fields = expected ^ actual
    structural_variance = (len(changed_fields) / max(1, len(union))) * 100
    issues = formatting_issues(event_type, payload)
    unauthorized = event_type == "database_transaction" and payload.get("authorized") is not True

    score = 100.0 if issues or unauthorized else structural_variance
    score = round(min(100.0, score), 1)

    if unauthorized:
        reason = "Unauthorized database state change"
    elif issues:
        reason = "Unexpected payload formatting: " + "; ".join(issues)
    elif structural_variance > THRESHOLD:
        reason = (
            f"Payload structure deviates {score:.1f}% from the {event_type} baseline "
            f"({len(changed_fields)} missing or unexpected field(s))"
        )
    else:
        reason = None
    return score, reason


def row_to_event(row: sqlite3.Row) -> TelemetryEvent:
    return TelemetryEvent(
        id=row["id"],
        created_at=datetime.fromisoformat(row["created_at"]),
        event_type=row["event_type"],
        source=row["source"],
        payload=json.loads(row["payload_json"]),
        drift_score=row["drift_score"],
        quarantined=bool(row["quarantined"]),
        intercept_reason=row["intercept_reason"],
    )


def insert_event(
    connection: sqlite3.Connection,
    *,
    created_at: datetime,
    event_type: str,
    source: str,
    payload: dict[str, Any],
    drift_score: float,
    quarantined: bool,
    intercept_reason: str | None,
    latency_ms: float | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO telemetry_events (
            created_at, event_type, source, payload_json, drift_score,
            quarantined, intercept_reason, processing_latency_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            created_at.isoformat(),
            event_type,
            source,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            drift_score,
            int(quarantined),
            intercept_reason,
            latency_ms,
        ),
    )
    return int(cursor.lastrowid)


@app.get("/api/healthz", response_model=dict[str, str])
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/ingest", response_model=TelemetryEvent, status_code=201)
def ingest_event(event: IngestInput) -> TelemetryEvent:
    if event.event_type not in EXPECTED_FIELDS:
        raise HTTPException(
            status_code=422,
            detail="event_type must be system_log, user_input, or database_transaction",
        )
    if len(json.dumps(event.payload, separators=(",", ":")).encode("utf-8")) > 65536:
        raise HTTPException(status_code=413, detail="payload exceeds the 64 KiB limit")

    started = time.perf_counter()
    created_at = utc_now()
    drift_score, reason = calculate_drift(event.event_type, event.payload)
    payload_json = json.dumps(event.payload, separators=(",", ":"), sort_keys=True)

    try:
        with get_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current_state = connection.execute(
                "SELECT state FROM system_state WHERE id = 1"
            ).fetchone()["state"]
            quarantined = reason is not None or current_state == "quarantined"
            if current_state == "quarantined" and reason is None:
                reason = "System is quarantined; manual release required"
            latency_ms = (time.perf_counter() - started) * 1000
            event_id = insert_event(
                connection,
                created_at=created_at,
                event_type=event.event_type,
                source=event.source,
                payload=event.payload,
                drift_score=drift_score,
                quarantined=quarantined,
                intercept_reason=reason,
                latency_ms=latency_ms,
            )
            if reason is not None:
                connection.execute(
                    "UPDATE system_state SET state = 'quarantined', updated_at = ? WHERE id = 1",
                    (created_at.isoformat(),),
                )
                intercept_payload = {
                    "intercepted_event_id": event_id,
                    "intercepted_event_type": event.event_type,
                    "source": event.source,
                    "drift_score": drift_score,
                    "reason": reason,
                    "air_gap": True,
                }
                insert_event(
                    connection,
                    created_at=created_at,
                    event_type=EventType.air_gap_intercept.value,
                    source="SOVEREIGN-GUARD",
                    payload=intercept_payload,
                    drift_score=max(drift_score, THRESHOLD + 0.1),
                    quarantined=True,
                    intercept_reason=reason,
                )
            row = connection.execute(
                "SELECT * FROM telemetry_events WHERE id = ?", (event_id,)
            ).fetchone()
    except sqlite3.Error as error:
        logger.exception("telemetry_persistence_failed")
        raise HTTPException(status_code=503, detail="Local telemetry store is unavailable") from error

    logger.info(
        "telemetry_ingested %s",
        json.dumps(
            {
                "id": event_id,
                "created_at": created_at.isoformat(),
                "event_type": event.event_type,
                "source": event.source,
                "payload": event.payload,
                "drift_score": drift_score,
                "quarantined": quarantined,
            },
            separators=(",", ":"),
        ),
    )
    if reason is not None:
        logger.warning(
            "air_gap_intercept event_id=%s drift=%.1f%% reason=%s",
            event_id,
            drift_score,
            reason,
        )
    return row_to_event(row)


@app.get("/api/events", response_model=list[TelemetryEvent])
def get_events(limit: int = Query(default=40, ge=1, le=100)) -> list[TelemetryEvent]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT * FROM telemetry_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [row_to_event(row) for row in rows]


@app.get("/api/status", response_model=SystemStatus)
def get_system_status() -> SystemStatus:
    with get_connection() as connection:
        state = connection.execute(
            "SELECT state FROM system_state WHERE id = 1"
        ).fetchone()["state"]
        counts = connection.execute(
            """
            SELECT
              COUNT(CASE WHEN event_type != 'air_gap_intercept' THEN 1 END) AS event_count,
              COUNT(CASE WHEN event_type != 'air_gap_intercept' AND quarantined = 1 THEN 1 END) AS intercepted_count,
              AVG(CASE WHEN event_type != 'air_gap_intercept' THEN processing_latency_ms END) AS avg_latency_ms,
              MAX(created_at) AS last_event_at
            FROM telemetry_events
            """
        ).fetchone()
    return SystemStatus(
        state=state,
        event_count=counts["event_count"],
        intercepted_count=counts["intercepted_count"],
        threshold=THRESHOLD,
        avg_latency_ms=round(counts["avg_latency_ms"] or 0.0, 2),
        last_event_at=(
            datetime.fromisoformat(counts["last_event_at"])
            if counts["last_event_at"]
            else None
        ),
    )


@app.post("/api/quarantine/reset", response_model=SystemStatus)
def reset_quarantine() -> SystemStatus:
    with get_connection() as connection:
        connection.execute(
            "UPDATE system_state SET state = 'active', updated_at = ? WHERE id = 1",
            (utc_now().isoformat(),),
        )
    logger.warning("quarantine_released operator_action=manual")
    return get_system_status()
