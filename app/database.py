from __future__ import annotations

import json
import sqlite3
import re
import unicodedata
import threading
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 3
EVENT_RETENTION = 50_000


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_dump(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _normalise_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _json_load(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


class StateDatabase:
    """SQLite-backed state owned by CacheDeck.

    v0.7 stores provider-neutral game, queue, job and event records here while
    the SteamPrefill compatibility provider remains responsible for downloads.
    Depot and manifest tables are intentionally created now so the native Steam
    engine can populate them without another state migration later.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._initialised = False

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._lock:
            if self._initialised:
                return
            with self.connection() as connection:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS games (
                        key TEXT PRIMARY KEY,
                        provider TEXT NOT NULL,
                        app_id INTEGER,
                        name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        selected INTEGER NOT NULL,
                        payload TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_games_provider_app
                        ON games(provider, app_id);
                    CREATE INDEX IF NOT EXISTS idx_games_status
                        ON games(status);

                    CREATE TABLE IF NOT EXISTS queue_items (
                        queue_id TEXT PRIMARY KEY,
                        provider TEXT NOT NULL,
                        app_id INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        requested_at TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_queue_state
                        ON queue_items(state, requested_at);

                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        provider TEXT NOT NULL,
                        state TEXT NOT NULL,
                        started_at TEXT,
                        payload TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_jobs_started
                        ON jobs(started_at DESC);

                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        app_id INTEGER,
                        job_id TEXT,
                        payload TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_events_created
                        ON events(id DESC);
                    CREATE INDEX IF NOT EXISTS idx_events_job
                        ON events(job_id, id DESC);
                    CREATE INDEX IF NOT EXISTS idx_events_app
                        ON events(provider, app_id, id DESC);

                    CREATE TABLE IF NOT EXISTS depots (
                        provider TEXT NOT NULL,
                        app_id INTEGER NOT NULL,
                        depot_id INTEGER NOT NULL,
                        name TEXT,
                        last_manifest_id TEXT,
                        payload TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(provider, app_id, depot_id)
                    );

                    CREATE TABLE IF NOT EXISTS manifests (
                        provider TEXT NOT NULL,
                        app_id INTEGER NOT NULL,
                        depot_id INTEGER NOT NULL,
                        manifest_id TEXT NOT NULL,
                        branch TEXT NOT NULL DEFAULT 'public',
                        status TEXT NOT NULL DEFAULT 'observed',
                        compressed_bytes INTEGER,
                        uncompressed_bytes INTEGER,
                        observed_at TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        PRIMARY KEY(provider, app_id, depot_id, manifest_id, branch)
                    );

                    CREATE TABLE IF NOT EXISTS schedules (
                        schedule_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        expression TEXT NOT NULL,
                        timezone TEXT NOT NULL,
                        enabled INTEGER NOT NULL DEFAULT 1,
                        last_trigger_key TEXT,
                        last_run_at TEXT,
                        last_result TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_schedules_enabled
                        ON schedules(enabled, name COLLATE NOCASE);
                    """
                )
                current_row = connection.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
                current_version = int(current_row["value"]) if current_row else 0
                if current_version < 1:
                    current_version = 1
                if current_version < 2:
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS idx_queue_provider_app_state "
                        "ON queue_items(provider, app_id, state)"
                    )
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS idx_events_type_created "
                        "ON events(event_type, id DESC)"
                    )
                    current_version = 2
                if current_version < 3:
                    connection.execute(
                        "CREATE TABLE IF NOT EXISTS schedules ("
                        "schedule_id TEXT PRIMARY KEY, name TEXT NOT NULL, "
                        "expression TEXT NOT NULL, timezone TEXT NOT NULL, "
                        "enabled INTEGER NOT NULL DEFAULT 1, last_trigger_key TEXT, "
                        "last_run_at TEXT, last_result TEXT, created_at TEXT NOT NULL, "
                        "updated_at TEXT NOT NULL)"
                    )
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS idx_schedules_enabled "
                        "ON schedules(enabled, name COLLATE NOCASE)"
                    )
                    current_version = 3
                connection.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(current_version),),
                )
            self._initialised = True

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        self.initialize()
        with self._lock, self.connection() as connection:
            row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else default

    def set_meta(self, key: str, value: str | None) -> None:
        self.initialize()
        with self._lock, self.connection() as connection:
            if value is None:
                connection.execute("DELETE FROM meta WHERE key = ?", (key,))
            else:
                connection.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value),
                )

    def append_event(
        self,
        event_type: str,
        *,
        provider: str = "cachedeck",
        app_id: int | None = None,
        job_id: str | None = None,
        payload: dict[str, Any] | None = None,
        created_at: str | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> int:
        if connection is None:
            self.initialize()
        values = (
            event_type,
            provider,
            app_id,
            job_id,
            _json_dump(payload or {}),
            created_at or utc_now(),
        )
        def insert(target: sqlite3.Connection) -> int:
            cursor = target.execute(
                "INSERT INTO events(event_type, provider, app_id, job_id, payload, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                values,
            )
            event_id = int(cursor.lastrowid)
            if event_id % 250 == 0 and event_id > EVENT_RETENTION:
                target.execute(
                    "DELETE FROM events WHERE id <= ?",
                    (event_id - EVENT_RETENTION,),
                )
            return event_id

        if connection is not None:
            return insert(connection)
        with self._lock, self.connection() as owned:
            return insert(owned)

    def list_events(
        self,
        limit: int = 100,
        *,
        event_type: str | None = None,
        app_id: int | None = None,
        job_id: str | None = None,
    ) -> list[dict[str, Any]]:
        self.initialize()
        limit = max(1, min(1000, limit))
        clauses: list[str] = []
        values: list[Any] = []
        if event_type:
            clauses.append("event_type = ?")
            values.append(event_type)
        if app_id is not None:
            clauses.append("app_id = ?")
            values.append(app_id)
        if job_id:
            clauses.append("job_id = ?")
            values.append(job_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(limit)
        with self._lock, self.connection() as connection:
            rows = connection.execute(
                "SELECT id, event_type, provider, app_id, job_id, payload, created_at "
                f"FROM events{where} ORDER BY id DESC LIMIT ?",
                values,
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "event_type": row["event_type"],
                "provider": row["provider"],
                "app_id": row["app_id"],
                "job_id": row["job_id"],
                "payload": _json_load(row["payload"], {}),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def list_game_payloads(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._lock, self.connection() as connection:
            rows = connection.execute("SELECT payload FROM games ORDER BY name COLLATE NOCASE").fetchall()
        return [_json_load(row["payload"], {}) for row in rows]

    def replace_game_payloads(self, payloads: list[dict[str, Any]]) -> None:
        self.initialize()
        now = utc_now()
        with self._lock, self.connection() as connection:
            previous_rows = connection.execute(
                "SELECT key, app_id, provider, status, payload FROM games"
            ).fetchall()
            previous = {str(row["key"]): row for row in previous_rows}
            previous_by_app = {
                int(row["app_id"]): row
                for row in previous_rows
                if row["app_id"] is not None
            }
            previous_by_name = {
                _normalise_name(str(_json_load(row["payload"], {}).get("name") or "")): row
                for row in previous_rows
                if _normalise_name(str(_json_load(row["payload"], {}).get("name") or ""))
            }
            matched_previous_keys: set[str] = set()
            for payload in payloads:
                key = str(payload.get("key") or "").strip()
                name = str(payload.get("name") or "").strip()
                if not key or not name:
                    continue
                provider = str(payload.get("provider") or "steamprefill")
                app_id = payload.get("app_id")
                status = str(payload.get("status") or "selected")
                selected = 1 if payload.get("selected", True) else 0
                old = previous.get(key)
                if old is None and isinstance(app_id, int):
                    old = previous_by_app.get(app_id)
                if old is None:
                    old = previous_by_name.get(_normalise_name(name))
                if old is not None:
                    matched_previous_keys.add(str(old["key"]))
                connection.execute(
                    """
                    INSERT INTO games(key, provider, app_id, name, status, selected, payload, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        provider=excluded.provider,
                        app_id=excluded.app_id,
                        name=excluded.name,
                        status=excluded.status,
                        selected=excluded.selected,
                        payload=excluded.payload,
                        updated_at=excluded.updated_at
                    """,
                    (key, provider, app_id, name, status, selected, _json_dump(payload), now),
                )
                if old is None:
                    self.append_event(
                        "game.discovered",
                        provider=provider,
                        app_id=app_id if isinstance(app_id, int) else None,
                        payload={"key": key, "name": name, "status": status},
                        connection=connection,
                    )
                elif str(old["key"]) != key:
                    self.append_event(
                        "game.identity_resolved",
                        provider=provider,
                        app_id=app_id if isinstance(app_id, int) else None,
                        payload={
                            "from_key": str(old["key"]),
                            "to_key": key,
                            "name": name,
                        },
                        connection=connection,
                    )
                    connection.execute("DELETE FROM games WHERE key = ?", (str(old["key"]),))
                elif str(old["status"]) != status:
                    self.append_event(
                        "game.status_changed",
                        provider=provider,
                        app_id=app_id if isinstance(app_id, int) else None,
                        job_id=payload.get("last_downloaded_job_id"),
                        payload={
                            "key": key,
                            "name": name,
                            "from": str(old["status"]),
                            "to": status,
                            "progress": payload.get("progress"),
                            "message": payload.get("message"),
                        },
                        connection=connection,
                    )
                else:
                    old_payload = _json_load(old["payload"], {})
                    old_progress = old_payload.get("progress")
                    new_progress = payload.get("progress")
                    if status == "downloading" and new_progress != old_progress:
                        self.append_event(
                            "game.progress",
                            provider=provider,
                            app_id=app_id if isinstance(app_id, int) else None,
                            job_id=payload.get("last_downloaded_job_id"),
                            payload={
                                "key": key,
                                "name": name,
                                "progress": new_progress,
                                "downloaded": payload.get("downloaded"),
                                "total": payload.get("total"),
                                "speed": payload.get("speed"),
                                "eta": payload.get("eta"),
                            },
                            connection=connection,
                        )
            removed = set(previous) - matched_previous_keys
            for key in removed:
                row = previous[key]
                old_payload = _json_load(row["payload"], {})
                self.append_event(
                    "game.unselected",
                    provider=str(row["provider"]),
                    app_id=row["app_id"],
                    payload={"key": key, "name": old_payload.get("name")},
                    connection=connection,
                )
                connection.execute("DELETE FROM games WHERE key = ?", (key,))

    def upsert_game_payload(self, payload: dict[str, Any]) -> None:
        self.initialize()
        key = str(payload.get("key") or "").strip()
        name = str(payload.get("name") or "").strip()
        if not key or not name:
            return
        provider = str(payload.get("provider") or "steamprefill")
        app_id = payload.get("app_id")
        status = str(payload.get("status") or "selected")
        selected = 1 if payload.get("selected", True) else 0
        now = utc_now()
        with self._lock, self.connection() as connection:
            old = connection.execute("SELECT status, payload FROM games WHERE key = ?", (key,)).fetchone()
            connection.execute(
                """INSERT INTO games(key, provider, app_id, name, status, selected, payload, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET provider=excluded.provider, app_id=excluded.app_id,
                name=excluded.name, status=excluded.status, selected=excluded.selected,
                payload=excluded.payload, updated_at=excluded.updated_at""",
                (key, provider, app_id, name, status, selected, _json_dump(payload), now),
            )
            if old is not None and str(old["status"]) != status:
                self.append_event("game.status_changed", provider=provider,
                    app_id=app_id if isinstance(app_id, int) else None,
                    job_id=payload.get("last_downloaded_job_id"),
                    payload={"key": key, "name": name, "from": old["status"], "to": status,
                             "progress": payload.get("progress"), "message": payload.get("message")},
                    connection=connection)

    def backup_to(self, destination: Path) -> dict[str, Any]:
        self.initialize()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with (
            self._lock,
            closing(sqlite3.connect(self.path)) as source,
            closing(sqlite3.connect(destination)) as target,
        ):
            source.backup(target)
            target.commit()
        size = destination.stat().st_size
        completed_at = utc_now()
        self.set_meta("database.last_backup_at", completed_at)
        self.set_meta("database.last_backup_path", str(destination))
        self.append_event("database.backup", payload={"path": str(destination), "size_bytes": size})
        return {"path": str(destination), "size_bytes": size, "completed_at": completed_at}

    def reconcile_queue(self) -> dict[str, int]:
        self.initialize()
        recovered = interrupted = 0
        now = utc_now()
        with self._lock, self.connection() as connection:
            rows = connection.execute("SELECT queue_id, payload FROM queue_items WHERE state = 'running'").fetchall()
            for row in rows:
                payload = _json_load(row["payload"], {})
                if payload.get("job_id"):
                    continue
                payload.update({"state": "queued", "started_at": None, "message": "Recovered after CacheDeck restart."})
                connection.execute("UPDATE queue_items SET state='queued', payload=?, updated_at=? WHERE queue_id=?",
                                   (_json_dump(payload), now, row["queue_id"]))
                recovered += 1
            if recovered:
                self.append_event("queue.recovered", payload={"requeued": recovered}, connection=connection)
        return {"requeued": recovered, "interrupted": interrupted}

    def list_queue_payloads(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._lock, self.connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM queue_items ORDER BY requested_at, rowid"
            ).fetchall()
        return [_json_load(row["payload"], {}) for row in rows]

    def replace_queue_payloads(self, payloads: list[dict[str, Any]]) -> None:
        self.initialize()
        now = utc_now()
        with self._lock, self.connection() as connection:
            previous_rows = connection.execute(
                "SELECT queue_id, state, payload FROM queue_items"
            ).fetchall()
            previous = {str(row["queue_id"]): row for row in previous_rows}
            next_ids: set[str] = set()
            for payload in payloads[-100:]:
                queue_id = str(payload.get("queue_id") or "").strip()
                if not queue_id:
                    continue
                provider = str(payload.get("provider") or "steamprefill")
                app_id = int(payload.get("app_id"))
                state = str(payload.get("state") or "queued")
                requested_at = str(payload.get("requested_at") or now)
                next_ids.add(queue_id)
                old = previous.get(queue_id)
                connection.execute(
                    """
                    INSERT INTO queue_items(queue_id, provider, app_id, state, requested_at, payload, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(queue_id) DO UPDATE SET
                        provider=excluded.provider,
                        app_id=excluded.app_id,
                        state=excluded.state,
                        requested_at=excluded.requested_at,
                        payload=excluded.payload,
                        updated_at=excluded.updated_at
                    """,
                    (queue_id, provider, app_id, state, requested_at, _json_dump(payload), now),
                )
                if old is None:
                    self.append_event(
                        "queue.added",
                        provider=provider,
                        app_id=app_id,
                        payload={"queue_id": queue_id, "state": state},
                        connection=connection,
                    )
                elif str(old["state"]) != state:
                    self.append_event(
                        "queue.state_changed",
                        provider=provider,
                        app_id=app_id,
                        job_id=payload.get("job_id"),
                        payload={
                            "queue_id": queue_id,
                            "from": str(old["state"]),
                            "to": state,
                            "message": payload.get("message"),
                        },
                        connection=connection,
                    )
            for queue_id in set(previous) - next_ids:
                connection.execute("DELETE FROM queue_items WHERE queue_id = ?", (queue_id,))

    def list_job_payloads(self, limit: int = 100) -> list[dict[str, Any]]:
        self.initialize()
        with self._lock, self.connection() as connection:
            rows = connection.execute(
                "SELECT payload FROM jobs ORDER BY COALESCE(started_at, updated_at) DESC LIMIT ?",
                (max(1, min(1000, limit)),),
            ).fetchall()
        return [_json_load(row["payload"], {}) for row in rows]

    def replace_job_payloads(self, payloads: list[dict[str, Any]], limit: int = 100) -> None:
        self.initialize()
        now = utc_now()
        payloads = payloads[: max(1, min(1000, limit))]
        with self._lock, self.connection() as connection:
            previous_rows = connection.execute("SELECT job_id, state FROM jobs").fetchall()
            previous = {str(row["job_id"]): str(row["state"]) for row in previous_rows}
            next_ids: set[str] = set()
            for payload in payloads:
                job_id = str(payload.get("job_id") or "").strip()
                if not job_id:
                    continue
                provider = str(payload.get("provider") or "steamprefill")
                state = str(payload.get("state") or "unavailable")
                started_at = payload.get("started_at")
                next_ids.add(job_id)
                connection.execute(
                    """
                    INSERT INTO jobs(job_id, provider, state, started_at, payload, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        provider=excluded.provider,
                        state=excluded.state,
                        started_at=excluded.started_at,
                        payload=excluded.payload,
                        updated_at=excluded.updated_at
                    """,
                    (job_id, provider, state, started_at, _json_dump(payload), now),
                )
                if job_id not in previous:
                    self.append_event(
                        "job.created",
                        provider=provider,
                        app_id=payload.get("app_id"),
                        job_id=job_id,
                        payload={"state": state, "scope": payload.get("scope")},
                        connection=connection,
                    )
                elif previous[job_id] != state:
                    self.append_event(
                        "job.state_changed",
                        provider=provider,
                        app_id=payload.get("app_id"),
                        job_id=job_id,
                        payload={"from": previous[job_id], "to": state, "message": payload.get("message")},
                        connection=connection,
                    )
            for job_id in set(previous) - next_ids:
                connection.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))

    def list_schedules(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._lock, self.connection() as connection:
            rows = connection.execute(
                "SELECT schedule_id, name, expression, timezone, enabled, "
                "last_trigger_key, last_run_at, last_result, created_at, updated_at "
                "FROM schedules ORDER BY name COLLATE NOCASE"
            ).fetchall()
        return [
            {
                "schedule_id": str(row["schedule_id"]),
                "name": str(row["name"]),
                "expression": str(row["expression"]),
                "timezone": str(row["timezone"]),
                "enabled": bool(row["enabled"]),
                "last_trigger_key": row["last_trigger_key"],
                "last_run_at": row["last_run_at"],
                "last_result": row["last_result"],
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    def get_schedule(self, schedule_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._lock, self.connection() as connection:
            row = connection.execute(
                "SELECT schedule_id, name, expression, timezone, enabled, "
                "last_trigger_key, last_run_at, last_result, created_at, updated_at "
                "FROM schedules WHERE schedule_id = ?",
                (schedule_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "schedule_id": str(row["schedule_id"]),
            "name": str(row["name"]),
            "expression": str(row["expression"]),
            "timezone": str(row["timezone"]),
            "enabled": bool(row["enabled"]),
            "last_trigger_key": row["last_trigger_key"],
            "last_run_at": row["last_run_at"],
            "last_result": row["last_result"],
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def upsert_schedule(
        self,
        *,
        schedule_id: str,
        name: str,
        expression: str,
        timezone_name: str,
        enabled: bool,
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self._lock, self.connection() as connection:
            previous = connection.execute(
                "SELECT created_at FROM schedules WHERE schedule_id = ?",
                (schedule_id,),
            ).fetchone()
            created_at = str(previous["created_at"]) if previous else now
            connection.execute(
                "INSERT INTO schedules(schedule_id, name, expression, timezone, enabled, "
                "created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(schedule_id) DO UPDATE SET name=excluded.name, "
                "expression=excluded.expression, timezone=excluded.timezone, "
                "enabled=excluded.enabled, updated_at=excluded.updated_at",
                (schedule_id, name, expression, timezone_name, 1 if enabled else 0, created_at, now),
            )
            self.append_event(
                "schedule.updated" if previous else "schedule.created",
                payload={
                    "schedule_id": schedule_id,
                    "name": name,
                    "expression": expression,
                    "timezone": timezone_name,
                    "enabled": enabled,
                },
                connection=connection,
            )
        result = self.get_schedule(schedule_id)
        assert result is not None
        return result

    def update_schedule_runtime(
        self,
        schedule_id: str,
        *,
        last_trigger_key: str | None = None,
        last_run_at: str | None = None,
        last_result: str | None = None,
    ) -> dict[str, Any] | None:
        self.initialize()
        with self._lock, self.connection() as connection:
            current = connection.execute(
                "SELECT schedule_id FROM schedules WHERE schedule_id = ?",
                (schedule_id,),
            ).fetchone()
            if current is None:
                return None
            connection.execute(
                "UPDATE schedules SET last_trigger_key = COALESCE(?, last_trigger_key), "
                "last_run_at = COALESCE(?, last_run_at), "
                "last_result = COALESCE(?, last_result), updated_at = ? "
                "WHERE schedule_id = ?",
                (last_trigger_key, last_run_at, last_result, utc_now(), schedule_id),
            )
        return self.get_schedule(schedule_id)

    def delete_schedule(self, schedule_id: str) -> dict[str, Any] | None:
        existing = self.get_schedule(schedule_id)
        if existing is None:
            return None
        with self._lock, self.connection() as connection:
            connection.execute("DELETE FROM schedules WHERE schedule_id = ?", (schedule_id,))
            self.append_event(
                "schedule.deleted",
                payload={
                    "schedule_id": schedule_id,
                    "name": existing["name"],
                },
                connection=connection,
            )
        return existing

    def upsert_depot(
        self,
        *,
        provider: str,
        app_id: int,
        depot_id: int,
        name: str | None = None,
        last_manifest_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.initialize()
        with self._lock, self.connection() as connection:
            connection.execute(
                """
                INSERT INTO depots(provider, app_id, depot_id, name, last_manifest_id, payload, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, app_id, depot_id) DO UPDATE SET
                    name=excluded.name,
                    last_manifest_id=excluded.last_manifest_id,
                    payload=excluded.payload,
                    updated_at=excluded.updated_at
                """,
                (provider, app_id, depot_id, name, last_manifest_id, _json_dump(payload or {}), utc_now()),
            )

    def record_manifest(
        self,
        *,
        provider: str,
        app_id: int,
        depot_id: int,
        manifest_id: str,
        branch: str = "public",
        status: str = "observed",
        compressed_bytes: int | None = None,
        uncompressed_bytes: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.initialize()
        observed_at = utc_now()
        with self._lock, self.connection() as connection:
            connection.execute(
                """
                INSERT INTO manifests(
                    provider, app_id, depot_id, manifest_id, branch, status,
                    compressed_bytes, uncompressed_bytes, observed_at, payload
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, app_id, depot_id, manifest_id, branch) DO UPDATE SET
                    status=excluded.status,
                    compressed_bytes=excluded.compressed_bytes,
                    uncompressed_bytes=excluded.uncompressed_bytes,
                    observed_at=excluded.observed_at,
                    payload=excluded.payload
                """,
                (
                    provider, app_id, depot_id, manifest_id, branch, status,
                    compressed_bytes, uncompressed_bytes, observed_at, _json_dump(payload or {}),
                ),
            )
            connection.execute(
                """
                INSERT INTO depots(provider, app_id, depot_id, name, last_manifest_id, payload, updated_at)
                VALUES(?, ?, ?, NULL, ?, '{}', ?)
                ON CONFLICT(provider, app_id, depot_id) DO UPDATE SET
                    last_manifest_id=excluded.last_manifest_id,
                    updated_at=excluded.updated_at
                """,
                (provider, app_id, depot_id, manifest_id, observed_at),
            )
            self.append_event(
                "manifest.observed",
                provider=provider,
                app_id=app_id,
                payload={
                    "depot_id": depot_id,
                    "manifest_id": manifest_id,
                    "branch": branch,
                    "status": status,
                },
                connection=connection,
            )

    def counts(self) -> dict[str, int]:
        self.initialize()
        with self._lock, self.connection() as connection:
            result: dict[str, int] = {}
            for table in ("games", "queue_items", "jobs", "events", "depots", "manifests", "schedules"):
                result[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            return result

    def migration_status(self) -> dict[str, Any]:
        raw = self.get_meta("legacy_json_import_v1")
        return _json_load(raw, {"completed": False, "counts": {}})

    def migrate_legacy_json(
        self,
        *,
        library_path: Path,
        queue_path: Path,
        history_path: Path,
    ) -> dict[str, Any]:
        """Import v0.6 JSON state once without deleting the source files."""
        self.initialize()
        previous = self.migration_status()
        if previous.get("completed"):
            return previous

        counts = {"games": 0, "queue_items": 0, "jobs": 0}
        errors: list[str] = []

        try:
            raw_library = json.loads(library_path.read_text(encoding="utf-8"))
            if isinstance(raw_library, dict):
                games = raw_library.get("games") if isinstance(raw_library.get("games"), list) else []
                if games:
                    self.replace_game_payloads([item for item in games if isinstance(item, dict)])
                    counts["games"] = len(games)
                for source, target in (
                    ("last_refreshed_at", "library.last_refreshed_at"),
                    ("last_completed_job_id", "library.last_completed_job_id"),
                    ("last_activity_job_id", "library.last_activity_job_id"),
                ):
                    value = raw_library.get(source)
                    if value:
                        self.set_meta(target, str(value))
        except FileNotFoundError:
            pass
        except Exception as exc:
            errors.append(f"library.json: {exc}")

        try:
            raw_queue = json.loads(queue_path.read_text(encoding="utf-8"))
            if isinstance(raw_queue, list):
                queue = [item for item in raw_queue if isinstance(item, dict)]
                self.replace_queue_payloads(queue)
                counts["queue_items"] = len(queue)
        except FileNotFoundError:
            pass
        except Exception as exc:
            errors.append(f"game-queue.json: {exc}")

        try:
            raw_history = json.loads(history_path.read_text(encoding="utf-8"))
            if isinstance(raw_history, list):
                jobs = [item for item in raw_history if isinstance(item, dict)]
                self.replace_job_payloads(jobs)
                counts["jobs"] = len(jobs)
        except FileNotFoundError:
            pass
        except Exception as exc:
            errors.append(f"history.json: {exc}")

        result = {
            "completed": not errors,
            "ok": not errors,
            "completed_at": utc_now(),
            "counts": counts,
            "errors": errors,
            "source_files_preserved": True,
        }
        self.set_meta("legacy_json_import_v1", _json_dump(result))
        self.append_event("database.legacy_import", payload=result)
        return result
