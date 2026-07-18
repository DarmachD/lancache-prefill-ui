from __future__ import annotations

import asyncio
import contextlib
import json
import os
import pty
import select
import shlex
import signal
import struct
import subprocess
import termios
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Final, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel

from app.library import (
    GameQueueItem,
    GameRecord,
    LibraryResponse,
    LibraryStore,
    QueueStore,
    SelectedApp,
    build_library_response,
    parse_progress_snapshot,
    parse_selected_app_ids_config,
    parse_selected_apps_status,
    output_indicates_successful_prefill,
    parse_size_bytes,
    placeholder_game_name,
    is_placeholder_game_name,
    resolve_steam_metadata,
    resolve_steam_metadata_by_id,
)

APP_NAME: Final = "CacheDeck"


def read_packaged_version() -> str:
    version_file = Path(__file__).resolve().parents[1] / "VERSION"
    try:
        return version_file.read_text(encoding="utf-8").strip() or "dev"
    except OSError:
        return "dev"


APP_VERSION: Final = os.getenv("CACHEDECK_VERSION", "").strip() or read_packaged_version()

TARGET_CONTAINER: Final = os.getenv("TARGET_CONTAINER", "LANCache-Prefill")
PREFILL_DIR: Final = os.getenv("PREFILL_DIR", "/lancacheprefill/SteamPrefill")
PREFILL_USER: Final = os.getenv("PREFILL_USER", "prefill")
PREFILL_COMMAND: Final = os.getenv("PREFILL_COMMAND", "./SteamPrefill prefill")
PREFILL_STATE_DIR: Final = os.getenv("PREFILL_STATE_DIR", "/tmp/cachedeck")
CONFIG_DIR: Final = Path(os.getenv("CACHEDECK_CONFIG_DIR", "/config"))
HISTORY_LIMIT: Final = max(5, min(100, int(os.getenv("HISTORY_LIMIT", "20"))))
AUTO_RESUME_INTERRUPTED: Final = os.getenv(
    "AUTO_RESUME_INTERRUPTED", "false"
).strip().lower() in {"1", "true", "yes", "on"}

STATIC_DIR: Final = Path(__file__).resolve().parent / "static"
HISTORY_FILE: Final = CONFIG_DIR / "history.json"
LIBRARY_FILE: Final = CONFIG_DIR / "library.json"
QUEUE_FILE: Final = CONFIG_DIR / "game-queue.json"

ALLOWED_ACTIONS: Final[dict[str, str]] = {
    "status": "./SteamPrefill select-apps status --no-ansi",
    "clear-cache": "./SteamPrefill clear-cache -y",
}

SCHEDULE_KEYS: Final = (
    "GlobalSchedule",
    "GLOBAL_SCHEDULE",
    "PREFILL_SCHEDULE",
    "STEAMPREFILL_SCHEDULE",
    "SCHEDULE",
)


class ActionRequest(BaseModel):
    action: str


class BulkQueueRequest(BaseModel):
    app_ids: list[int]


class CommandResult(BaseModel):
    ok: bool
    code: int
    stdout: str
    stderr: str


PrefillState = Literal[
    "idle",
    "starting",
    "running",
    "paused",
    "completed",
    "failed",
    "stopped",
    "interrupted",
    "finished",
    "unavailable",
]


class PrefillStatus(BaseModel):
    state: PrefillState
    running: bool
    managed: bool
    source: Literal["cachedeck", "external", "none"] = "none"
    pid: int | None = None
    worker_pid: int | None = None
    paused: bool = False
    job_id: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    log_available: bool = False
    log_source: Literal["cachedeck", "container", "none"] = "none"
    message: str = ""


class PrefillStartResult(BaseModel):
    ok: bool
    message: str
    status: PrefillStatus


class PrefillLogResult(BaseModel):
    ok: bool
    source: Literal["cachedeck", "container", "none"]
    stdout: str
    stderr: str = ""


class HistoryRecord(BaseModel):
    job_id: str
    source: Literal["cachedeck", "external"]
    state: PrefillState
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    message: str = ""
    resume_of: str | None = None
    auto_resume_attempted: bool = False
    scope: Literal["full", "single"] = "full"
    app_id: int | None = None
    app_name: str | None = None


class ScheduleInfo(BaseModel):
    configured: bool
    key: str | None = None
    expression: str | None = None
    timezone: str = "UTC"
    next_run: str | None = None
    last_external_run: str | None = None
    message: str = ""


class DiagnosticCheck(BaseModel):
    name: str
    ok: bool
    detail: str


class DiagnosticsResult(BaseModel):
    ok: bool
    generated_at: str
    checks: list[DiagnosticCheck]
    summary: str


class HistoryStore:
    def __init__(self, path: Path, limit: int) -> None:
        self.path = path
        self.limit = limit
        self._lock = threading.Lock()

    def _read_unlocked(self) -> list[HistoryRecord]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return []

        if not isinstance(raw, list):
            return []

        records: list[HistoryRecord] = []
        for item in raw:
            try:
                records.append(HistoryRecord.model_validate(item))
            except Exception:
                continue
        return records[: self.limit]

    def _write_unlocked(self, records: list[HistoryRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(
                [record.model_dump(mode="json") for record in records[: self.limit]],
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        temp_path.replace(self.path)

    def list(self) -> list[HistoryRecord]:
        with self._lock:
            return self._read_unlocked()

    def latest(self) -> HistoryRecord | None:
        records = self.list()
        return records[0] if records else None

    def upsert(self, record: HistoryRecord) -> None:
        with self._lock:
            records = self._read_unlocked()
            existing = next(
                (index for index, item in enumerate(records) if item.job_id == record.job_id),
                None,
            )
            if existing is not None:
                if records[existing] == record:
                    return
                records.pop(existing)
            records.insert(0, record)
            self._write_unlocked(records)

    def update(self, job_id: str, **changes: object) -> HistoryRecord | None:
        with self._lock:
            records = self._read_unlocked()
            for index, record in enumerate(records):
                if record.job_id != job_id:
                    continue
                updated = record.model_copy(update=changes)
                if updated != record:
                    records[index] = updated
                    self._write_unlocked(records)
                return updated
        return None


history_store = HistoryStore(HISTORY_FILE, HISTORY_LIMIT)
library_store = LibraryStore(LIBRARY_FILE)
queue_store = QueueStore(QUEUE_FILE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in {None, ""} else None
    except (TypeError, ValueError):
        return None


def docker_exec_command(command: str, *, interactive: bool = False) -> list[str]:
    args = ["docker", "exec"]
    if interactive:
        args.extend(["-i", "-t"])
    if PREFILL_USER:
        args.extend(["--user", PREFILL_USER])
    if PREFILL_DIR:
        args.extend(["--workdir", PREFILL_DIR])
    args.extend([TARGET_CONTAINER, "bash", "-lc", command])
    return args


def run_process(args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


async def run_process_async(
    args: list[str], *, timeout: int
) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(run_process, args, timeout=timeout)


async def run_target_command(
    command: str, *, timeout: int = 20
) -> subprocess.CompletedProcess[str]:
    return await run_process_async(docker_exec_command(command), timeout=timeout)


async def inspect_target_details() -> dict[str, object]:
    try:
        result = await run_process_async(
            ["docker", "inspect", TARGET_CONTAINER],
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return {
            "running": False,
            "status": "timeout",
            "detail": "Docker did not answer within 10 seconds.",
            "environment": {},
        }
    except OSError as exc:
        return {
            "running": False,
            "status": "error",
            "detail": str(exc),
            "environment": {},
        }

    if result.returncode != 0:
        return {
            "running": False,
            "status": "not found",
            "detail": result.stderr.strip() or "Target container was not found.",
            "environment": {},
        }

    try:
        container = json.loads(result.stdout)[0]
    except (json.JSONDecodeError, IndexError, TypeError, KeyError):
        return {
            "running": False,
            "status": "invalid response",
            "detail": "Docker returned unreadable inspect data.",
            "environment": {},
        }

    state = container.get("State", {})
    environment: dict[str, str] = {}
    for value in container.get("Config", {}).get("Env", []) or []:
        key, separator, item_value = value.partition("=")
        if separator:
            environment[key] = item_value

    return {
        "running": bool(state.get("Running")),
        "status": str(state.get("Status") or "unknown"),
        "detail": str(state.get("Error") or ""),
        "environment": environment,
        "image": str(container.get("Config", {}).get("Image") or ""),
        "started_at": str(state.get("StartedAt") or ""),
    }


async def inspect_target() -> dict[str, object]:
    try:
        result = await run_process_async(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Running}}|{{.State.Status}}|{{.State.Error}}",
                TARGET_CONTAINER,
            ],
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return {
            "running": False,
            "status": "timeout",
            "detail": "Docker did not answer within 10 seconds.",
        }
    except OSError as exc:
        return {"running": False, "status": "error", "detail": str(exc)}

    if result.returncode != 0:
        return {
            "running": False,
            "status": "not found",
            "detail": result.stderr.strip() or "Target container was not found.",
        }

    running_text, separator, remainder = result.stdout.strip().partition("|")
    status, _, detail = remainder.partition("|") if separator else ("unknown", "", "")
    return {
        "running": running_text == "true",
        "status": status or "unknown",
        "detail": detail,
    }


def prefill_status_command() -> str:
    state_dir = shlex.quote(PREFILL_STATE_DIR)
    return f"""
state_dir={state_dir}
pid_file="$state_dir/prefill.pid"
job_file="$state_dir/prefill.job"
started_file="$state_dir/prefill.started"
finished_file="$state_dir/prefill.finished"
exit_file="$state_dir/prefill.exit"
log_file="$state_dir/prefill.log"
wrapper_file="$state_dir/prefill-wrapper.sh"

read_file() {{
    if [ -r "$1" ]; then
        IFS= read -r value < "$1" || true
        printf '%s' "$value"
    fi
}}

managed_pid="$(read_file "$pid_file")"
managed_running="false"

if [[ "$managed_pid" =~ ^[0-9]+$ ]] && kill -0 "$managed_pid" 2>/dev/null; then
    cmdline="$(tr '\\0' ' ' < "/proc/$managed_pid/cmdline" 2>/dev/null || true)"
    if [[ "$cmdline" == *"$wrapper_file"* ]]; then
        managed_running="true"
    fi
fi

worker_pid=""
worker_started=""
worker_state=""
for proc_dir in /proc/[0-9]*; do
    candidate_pid="${{proc_dir##*/}}"
    [ "$candidate_pid" = "$$" ] && continue
    [ -r "$proc_dir/cmdline" ] || continue
    cmdline="$(tr '\\0' ' ' < "$proc_dir/cmdline" 2>/dev/null || true)"
    if [[ "$cmdline" =~ SteamPrefill.*[[:space:]]prefill([[:space:]]|$) ]]; then
        worker_pid="$candidate_pid"
        worker_state="$(ps -o stat= -p "$candidate_pid" 2>/dev/null | tr -d ' ' || true)"
        elapsed="$(ps -o etimes= -p "$candidate_pid" 2>/dev/null | tr -d ' ' || true)"
        if [[ "$elapsed" =~ ^[0-9]+$ ]]; then
            start_epoch="$(( $(date +%s) - elapsed ))"
            worker_started="$(date -u -d "@$start_epoch" +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || true)"
        fi
        break
    fi
done

job_id="$(read_file "$job_file")"
started_at="$(read_file "$started_file")"
finished_at="$(read_file "$finished_file")"
exit_code="$(read_file "$exit_file")"
log_available="false"
[ -s "$log_file" ] && log_available="true"

printf '%s\\0' \
    "$managed_pid" \
    "$managed_running" \
    "$worker_pid" \
    "$job_id" \
    "$started_at" \
    "$finished_at" \
    "$exit_code" \
    "$log_available" \
    "$worker_started" \
    "$worker_state"
""".strip()


async def get_raw_prefill_status() -> PrefillStatus:
    target = await inspect_target()
    if not target["running"]:
        return PrefillStatus(
            state="unavailable",
            running=False,
            managed=False,
            message=f"{TARGET_CONTAINER} is not running: {target['status']}.",
        )

    try:
        result = await run_target_command(prefill_status_command(), timeout=15)
    except subprocess.TimeoutExpired:
        return PrefillStatus(
            state="unavailable",
            running=False,
            managed=False,
            message="Timed out while checking the prefill job.",
        )
    except OSError as exc:
        return PrefillStatus(
            state="unavailable",
            running=False,
            managed=False,
            message=f"Unable to run Docker: {exc}",
        )

    if result.returncode != 0:
        return PrefillStatus(
            state="unavailable",
            running=False,
            managed=False,
            message=result.stderr.strip() or "Unable to inspect the prefill job.",
        )

    fields = result.stdout.split("\0")
    if len(fields) < 10:
        return PrefillStatus(
            state="unavailable",
            running=False,
            managed=False,
            message="SteamPrefill returned an unreadable job state.",
        )

    managed_pid = parse_int(fields[0])
    managed_running = fields[1] == "true"
    worker_pid = parse_int(fields[2])
    job_id = fields[3] or None
    started_at = fields[4] or None
    finished_at = fields[5] or None
    exit_code = parse_int(fields[6])
    log_available = fields[7] == "true"
    worker_started = fields[8] or None
    worker_state = fields[9] or ""
    paused = worker_state.startswith(("T", "t"))

    if managed_running:
        return PrefillStatus(
            state="paused" if paused else "running",
            running=True,
            managed=True,
            source="cachedeck",
            pid=managed_pid,
            worker_pid=worker_pid,
            paused=paused,
            job_id=job_id,
            started_at=started_at,
            log_available=log_available,
            log_source="cachedeck",
            message=(
                "Prefill is paused. Resume it when you are ready."
                if paused
                else "Prefill is running independently on the server."
            ),
        )

    if worker_pid is not None:
        external_job_id = f"external-{worker_pid}-{worker_started or 'unknown'}"
        return PrefillStatus(
            state="paused" if paused else "running",
            running=True,
            managed=False,
            source="external",
            pid=worker_pid,
            worker_pid=worker_pid,
            paused=paused,
            job_id=external_job_id,
            started_at=worker_started,
            log_available=True,
            log_source="container",
            message=(
                "The scheduled/external prefill is paused. Resume it when you are ready."
                if paused
                else (
                    "A scheduler or externally started prefill is running. "
                    "If the log says 'already running, aborting schedule', only the "
                    "duplicate scheduled launch was skipped; the active prefill continues."
                )
            ),
        )

    if exit_code is not None:
        if exit_code == 0:
            state: PrefillState = "completed"
            message = "The last CacheDeck prefill completed successfully."
        elif exit_code in {130, 143}:
            state = "stopped"
            message = "The last CacheDeck prefill was stopped."
        else:
            state = "failed"
            message = f"The last CacheDeck prefill exited with code {exit_code}."
        return PrefillStatus(
            state=state,
            running=False,
            managed=True,
            source="cachedeck",
            job_id=job_id,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code,
            log_available=log_available,
            log_source="cachedeck" if log_available else "none",
            message=message,
        )

    if job_id:
        return PrefillStatus(
            state="interrupted",
            running=False,
            managed=True,
            source="cachedeck",
            job_id=job_id,
            started_at=started_at,
            finished_at=finished_at,
            log_available=log_available,
            log_source="cachedeck" if log_available else "none",
            message=(
                "The previous CacheDeck prefill stopped without recording an "
                "exit code. The target container may have restarted."
            ),
        )

    return PrefillStatus(
        state="idle",
        running=False,
        managed=False,
        source="none",
        message="No prefill job is currently running.",
    )


def record_from_status(status: PrefillStatus) -> HistoryRecord | None:
    if not status.job_id or status.source == "none":
        return None
    source: Literal["cachedeck", "external"] = (
        "cachedeck" if status.source == "cachedeck" else "external"
    )
    return HistoryRecord(
        job_id=status.job_id,
        source=source,
        state=status.state,
        started_at=status.started_at,
        finished_at=status.finished_at,
        exit_code=status.exit_code,
        message=status.message,
    )


async def get_prefill_status() -> PrefillStatus:
    status = await get_raw_prefill_status()
    latest = history_store.latest()

    if status.state == "idle" and latest and latest.state in {"running", "paused"}:
        if latest.source == "cachedeck":
            status = PrefillStatus(
                state="interrupted",
                running=False,
                managed=True,
                source="cachedeck",
                job_id=latest.job_id,
                started_at=latest.started_at,
                finished_at=utc_now(),
                log_available=False,
                log_source="none",
                message=(
                    "The last CacheDeck job disappeared after the target "
                    "container restarted or its temporary state was cleared."
                ),
            )
        else:
            history_store.update(
                latest.job_id,
                state="finished",
                finished_at=utc_now(),
                message="External prefill ended; its exit status is unavailable.",
            )

    record = record_from_status(status)
    if record:
        previous = history_store.latest()
        if previous and previous.job_id == record.job_id:
            record = record.model_copy(
                update={
                    "resume_of": previous.resume_of,
                    "auto_resume_attempted": previous.auto_resume_attempted,
                    "scope": previous.scope,
                    "app_id": previous.app_id,
                    "app_name": previous.app_name,
                }
            )
        history_store.upsert(record)
    return status


def build_prefill_wrapper(job_id: str, started_at: str, command: str) -> str:
    state_dir = shlex.quote(PREFILL_STATE_DIR)
    prefill_dir = shlex.quote(PREFILL_DIR)
    return f"""#!/usr/bin/env bash
set +e

state_dir={state_dir}
log_file="$state_dir/prefill.log"
pid_file="$state_dir/prefill.pid"
exit_file="$state_dir/prefill.exit"
finished_file="$state_dir/prefill.finished"
child_pid=""

finish_job() {{
    code="$?"
    finished_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    printf '%s\\n' "$finished_at" > "$finished_file"
    printf '%s\\n' "$code" > "$exit_file"
    rm -f "$pid_file"
    printf '\\n[%s] CacheDeck prefill finished with exit code %s.\\n' \
        "$finished_at" "$code"
}}

stop_job() {{
    if [[ "$child_pid" =~ ^[0-9]+$ ]] && kill -0 "$child_pid" 2>/dev/null; then
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
    fi
    exit 143
}}

trap finish_job EXIT
trap stop_job TERM HUP
trap 'exit 130' INT

exec >> "$log_file" 2>&1
printf '[%s] CacheDeck job {job_id} started.\\n' {shlex.quote(started_at)}
cd {prefill_dir} || exit 127

{command} &
child_pid="$!"
wait "$child_pid"
exit "$?"
"""


def build_start_command(job_id: str, started_at: str, command: str) -> str:
    state_dir = shlex.quote(PREFILL_STATE_DIR)
    wrapper_script = shlex.quote(build_prefill_wrapper(job_id, started_at, command))
    wrapper_path = shlex.quote(
        str(PurePosixPath(PREFILL_STATE_DIR) / "prefill-wrapper.sh")
    )
    return f"""
set -u
state_dir={state_dir}
pid_file="$state_dir/prefill.pid"
job_file="$state_dir/prefill.job"
started_file="$state_dir/prefill.started"
finished_file="$state_dir/prefill.finished"
exit_file="$state_dir/prefill.exit"
log_file="$state_dir/prefill.log"
lock_dir="$state_dir/start.lock"
wrapper_file={wrapper_path}

mkdir -p "$state_dir"
if ! mkdir "$lock_dir" 2>/dev/null; then
    printf 'LOCKED\\0'
    exit 75
fi
trap 'rmdir "$lock_dir" 2>/dev/null || true' EXIT

old_pid=""
if [ -r "$pid_file" ]; then
    IFS= read -r old_pid < "$pid_file" || true
fi
if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
    old_cmdline="$(tr '\\0' ' ' < "/proc/$old_pid/cmdline" 2>/dev/null || true)"
    if [[ "$old_cmdline" == *"$wrapper_file"* ]]; then
        printf 'ALREADY_RUNNING\\0%s\\0' "$old_pid"
        exit 73
    fi
fi

for proc_dir in /proc/[0-9]*; do
    candidate_pid="${{proc_dir##*/}}"
    [ "$candidate_pid" = "$$" ] && continue
    [ -r "$proc_dir/cmdline" ] || continue
    cmdline="$(tr '\\0' ' ' < "$proc_dir/cmdline" 2>/dev/null || true)"
    if [[ "$cmdline" =~ SteamPrefill.*[[:space:]]prefill([[:space:]]|$) ]]; then
        printf 'EXTERNAL_RUNNING\\0%s\\0' "$candidate_pid"
        exit 73
    fi
done

printf '%s' {wrapper_script} > "$wrapper_file"
chmod 700 "$wrapper_file"
rm -f "$exit_file" "$finished_file"
printf '%s\\n' {shlex.quote(job_id)} > "$job_file"
printf '%s\\n' {shlex.quote(started_at)} > "$started_file"
: > "$log_file"

nohup bash "$wrapper_file" >/dev/null 2>&1 </dev/null &
pid="$!"
printf '%s\\n' "$pid" > "$pid_file.tmp"
mv "$pid_file.tmp" "$pid_file"

sleep 0.15
if ! kill -0 "$pid" 2>/dev/null; then
    printf 'FAILED\\0%s\\0' "$pid"
    exit 1
fi

printf 'STARTED\\0%s\\0%s\\0' {shlex.quote(job_id)} "$pid"
""".strip()


def managed_prefill_command(app_id: int | None = None) -> str:
    """Build a machine-readable managed command without overriding user-supplied flags."""
    command = PREFILL_COMMAND.strip()
    if app_id is not None:
        command = f"{command} {app_id}"
    tokens = set(command.split())
    if "--verbose" not in tokens:
        command += " --verbose"
    if "--no-ansi" not in tokens:
        command += " --no-ansi"
    return command


async def launch_prefill_job(
    resume_of: str | None = None,
    *,
    app_id: int | None = None,
    app_name: str | None = None,
) -> PrefillStartResult:
    current = await get_raw_prefill_status()
    if current.state == "unavailable":
        raise HTTPException(status_code=503, detail=current.message)
    if current.running:
        raise HTTPException(status_code=409, detail="A SteamPrefill job is already running.")

    scope: Literal["full", "single"] = "single" if app_id is not None else "full"
    if app_id is not None and app_id < 1:
        raise HTTPException(status_code=400, detail="Invalid Steam app ID.")
    command = managed_prefill_command(app_id)

    job_id = uuid.uuid4().hex
    started_at = utc_now()
    history_store.upsert(
        HistoryRecord(
            job_id=job_id,
            source="cachedeck",
            state="starting",
            started_at=started_at,
            message=(f"Checking and updating {app_name or app_id}." if app_id is not None else "Starting detached prefill."),
            resume_of=resume_of,
            scope=scope,
            app_id=app_id,
            app_name=app_name,
        )
    )

    try:
        result = await run_target_command(build_start_command(job_id, started_at, command), timeout=20)
    except subprocess.TimeoutExpired as exc:
        history_store.update(job_id, state="failed", finished_at=utc_now(), message=str(exc))
        raise HTTPException(status_code=504, detail="Timed out while starting the prefill job.") from exc
    except OSError as exc:
        history_store.update(job_id, state="failed", finished_at=utc_now(), message=str(exc))
        raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc

    marker = result.stdout.split("\0", 1)[0]
    if result.returncode != 0 or marker != "STARTED":
        history_store.update(
            job_id,
            state="failed",
            finished_at=utc_now(),
            message=result.stderr.strip() or marker or "Detached start failed.",
        )
        if marker in {"ALREADY_RUNNING", "EXTERNAL_RUNNING"}:
            raise HTTPException(status_code=409, detail="A SteamPrefill job started before CacheDeck could launch this one.")
        if marker == "LOCKED":
            raise HTTPException(status_code=409, detail="Another start request is already being processed.")
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or "SteamPrefill did not confirm that the detached job started.",
        )

    await asyncio.sleep(0.2)
    status = await get_prefill_status()
    return PrefillStartResult(
        ok=True,
        message=(f"{app_name or app_id} was queued for a server-side check and update." if app_id is not None else "Prefill started on the server and will continue after this browser disconnects."),
        status=status,
    )


def parse_cron_field(field: str, minimum: int, maximum: int) -> set[int] | None:
    if field == "*":
        return set(range(minimum, maximum + 1))

    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            return None
        step = 1
        base = part
        if "/" in part:
            base, step_text = part.split("/", 1)
            try:
                step = int(step_text)
            except ValueError:
                return None
            if step < 1:
                return None
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            try:
                start, end = int(start_text), int(end_text)
            except ValueError:
                return None
        else:
            try:
                start = end = int(base)
            except ValueError:
                return None
        if start < minimum or end > maximum or start > end:
            return None
        values.update(range(start, end + 1, step))
    return values


def next_cron_run(expression: str, tz_name: str) -> str | None:
    fields = expression.split()
    if len(fields) != 5:
        return None

    minute = parse_cron_field(fields[0], 0, 59)
    hour = parse_cron_field(fields[1], 0, 23)
    day = parse_cron_field(fields[2], 1, 31)
    month = parse_cron_field(fields[3], 1, 12)
    weekday = parse_cron_field(fields[4], 0, 7)
    if any(value is None for value in (minute, hour, day, month, weekday)):
        return None
    assert minute is not None and hour is not None and day is not None
    assert month is not None and weekday is not None

    normalized_weekday = {0 if value == 7 else value for value in weekday}
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = timezone.utc

    day_is_wildcard = fields[2] == "*"
    weekday_is_wildcard = fields[4] == "*"
    candidate = datetime.now(tz).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 366):
        cron_weekday = (candidate.weekday() + 1) % 7
        day_matches = candidate.day in day
        weekday_matches = cron_weekday in normalized_weekday
        if day_is_wildcard and weekday_is_wildcard:
            calendar_matches = True
        elif day_is_wildcard:
            calendar_matches = weekday_matches
        elif weekday_is_wildcard:
            calendar_matches = day_matches
        else:
            # Vixie cron treats restricted day-of-month and day-of-week fields
            # as alternatives rather than requiring both to match.
            calendar_matches = day_matches or weekday_matches

        if (
            candidate.minute in minute
            and candidate.hour in hour
            and candidate.month in month
            and calendar_matches
        ):
            return candidate.isoformat(timespec="minutes")
        candidate += timedelta(minutes=1)
    return None


async def get_schedule_info() -> ScheduleInfo:
    details = await inspect_target_details()
    environment = details.get("environment", {})
    if not isinstance(environment, dict):
        environment = {}

    key = next((item for item in SCHEDULE_KEYS if environment.get(item)), None)
    expression = str(environment.get(key, "")).strip() if key else ""
    timezone_name = str(environment.get("TZ") or "UTC")
    last_external = next(
        (
            record.started_at
            for record in history_store.list()
            if record.source == "external" and record.started_at
        ),
        None,
    )

    if not expression:
        return ScheduleInfo(
            configured=False,
            timezone=timezone_name,
            last_external_run=last_external,
            message="No recognised schedule environment variable was found.",
        )

    next_run = next_cron_run(expression, timezone_name)
    return ScheduleInfo(
        configured=True,
        key=key,
        expression=expression,
        timezone=timezone_name,
        next_run=next_run,
        last_external_run=last_external,
        message=(
            "Schedule detected from the target container."
            if next_run
            else "Schedule detected, but its cron format could not be calculated."
        ),
    )


async def run_diagnostics() -> DiagnosticsResult:
    checks: list[DiagnosticCheck] = []

    socket_path = Path("/var/run/docker.sock")
    checks.append(
        DiagnosticCheck(
            name="Docker socket",
            ok=socket_path.exists(),
            detail=(
                "Mounted at /var/run/docker.sock."
                if socket_path.exists()
                else "Docker socket is not mounted."
            ),
        )
    )

    try:
        docker_version = await run_process_async(
            ["docker", "version", "--format", "{{.Server.Version}}"], timeout=10
        )
        checks.append(
            DiagnosticCheck(
                name="Docker API",
                ok=docker_version.returncode == 0,
                detail=docker_version.stdout.strip() or docker_version.stderr.strip() or "No response.",
            )
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        checks.append(DiagnosticCheck(name="Docker API", ok=False, detail=str(exc)))

    target = await inspect_target_details()
    checks.append(
        DiagnosticCheck(
            name="Target container",
            ok=bool(target["running"]),
            detail=f"{TARGET_CONTAINER}: {target['status']}",
        )
    )

    if target["running"]:
        diagnostic_command = f"""
set +e
printf 'user=%s\\n' "$(id -un 2>/dev/null || true)"
printf 'uid=%s\\n' "$(id -u 2>/dev/null || true)"
printf 'cwd=%s\\n' "$(pwd)"
if [ -x ./SteamPrefill ]; then printf 'binary=ok\\n'; else printf 'binary=missing\\n'; fi
mkdir -p {shlex.quote(PREFILL_STATE_DIR)} 2>/dev/null
probe={shlex.quote(str(PurePosixPath(PREFILL_STATE_DIR) / '.cachedeck-write-test'))}
if : > "$probe" 2>/dev/null; then rm -f "$probe"; printf 'state=writeable\\n'; else printf 'state=readonly\\n'; fi
./SteamPrefill --version 2>/dev/null | head -n 1 || true
""".strip()
        try:
            result = await run_target_command(diagnostic_command, timeout=15)
            output = result.stdout.strip()
            checks.append(
                DiagnosticCheck(
                    name="SteamPrefill executable",
                    ok="binary=ok" in output,
                    detail=output or result.stderr.strip() or "No output.",
                )
            )
            checks.append(
                DiagnosticCheck(
                    name="Target state directory",
                    ok="state=writeable" in output,
                    detail=f"{PREFILL_STATE_DIR} as {PREFILL_USER or 'container default user'}",
                )
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            checks.append(DiagnosticCheck(name="SteamPrefill executable", ok=False, detail=str(exc)))
            checks.append(DiagnosticCheck(name="Target state directory", ok=False, detail=str(exc)))

    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        probe = CONFIG_DIR / ".cachedeck-write-test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        config_ok = True
        config_detail = f"{CONFIG_DIR} is writeable."
    except OSError as exc:
        config_ok = False
        config_detail = str(exc)
    checks.append(DiagnosticCheck(name="Persistent config", ok=config_ok, detail=config_detail))

    schedule = await get_schedule_info()
    checks.append(
        DiagnosticCheck(
            name="Schedule detection",
            ok=True,
            detail=(
                f"{schedule.key}={schedule.expression}; next {schedule.next_run or 'unknown'}"
                if schedule.configured
                else f"Optional: {schedule.message}"
            ),
        )
    )

    passed = sum(check.ok for check in checks)
    summary = f"{passed}/{len(checks)} checks passed. CacheDeck {APP_VERSION}."
    return DiagnosticsResult(
        ok=all(check.ok for check in checks),
        generated_at=utc_now(),
        checks=checks,
        summary=summary,
    )


async def read_selected_app_ids_from_config() -> list[int]:
    command = r"""
for candidate in ./Config/selectedAppsToPrefill.json /Config/selectedAppsToPrefill.json; do
    if [ -r "$candidate" ]; then
        cat "$candidate"
        exit 0
    fi
done
exit 44
""".strip()
    result = await run_target_command(command, timeout=20)
    if result.returncode != 0:
        return []
    return parse_selected_app_ids_config(result.stdout)


def selected_apps_from_ids(app_ids: list[int]) -> list[GameRecord]:
    existing = {
        game.app_id: game
        for game in library_store.list_games()
        if game.app_id is not None
    }
    selected = []
    for app_id in app_ids:
        previous = existing.get(app_id)
        selected.append(
            SelectedApp(
                app_id=app_id,
                name=(previous.name if previous else placeholder_game_name(app_id)),
                download_size=(previous.download_size if previous else None),
            )
        )
    return library_store.replace_selected(selected, utc_now())


async def refresh_selected_library() -> tuple[list[GameRecord], str]:
    """Refresh the catalogue without making the Games view depend on Steam manifest availability."""
    try:
        app_ids = await read_selected_app_ids_from_config()
    except (OSError, subprocess.TimeoutExpired):
        app_ids = []

    # The selected-app JSON is the fastest and most reliable source of the library itself.
    # Avoid launching a second SteamPrefill process while a real prefill is active.
    if app_ids:
        games = selected_apps_from_ids(app_ids)
        start_metadata_refresh()
        try:
            running = (await get_raw_prefill_status()).running
        except Exception:
            running = False
        if running:
            return games, (
                f"Loaded {len(games)} selected apps directly from SteamPrefill's saved config. "
                "The manifest-heavy size scan was skipped while a prefill is running. "
                "Names, artwork and completed games are being reconstructed in the background."
            )

    status_result: subprocess.CompletedProcess[str] | None = None
    status_error = ""
    try:
        status_result = await run_target_command(
            "./SteamPrefill select-apps status --no-ansi",
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        status_error = "SteamPrefill's detailed status command timed out."
    except OSError as exc:
        status_error = f"Docker could not run SteamPrefill: {exc}"

    if status_result is not None:
        selected = parse_selected_apps_status(status_result.stdout) if status_result.returncode == 0 else []
        if selected:
            games = library_store.replace_selected(selected, utc_now())
            start_metadata_refresh()
            return games, f"Loaded {len(games)} selected Steam apps with transfer sizes."
        combined = "\n".join(part for part in (status_result.stdout, status_result.stderr) if part)
        lowered = combined.casefold()
        if "unexpected parameter" in lowered and "status" in lowered:
            status_error = "This SteamPrefill build does not support select-apps status."
        elif "unable to download manifests" in lowered or "manifestexception" in lowered:
            status_error = "SteamPrefill could not calculate app sizes because Steam manifest retrieval failed."
        elif status_result.returncode != 0:
            status_error = "SteamPrefill's detailed status command failed."
        else:
            status_error = "SteamPrefill returned no parseable status table."

    if app_ids:
        games = selected_apps_from_ids(app_ids)
        start_metadata_refresh()
        detail = (
            f"Loaded {len(games)} selected apps directly from SteamPrefill's saved config. "
            "Names and artwork are being resolved in the background. "
            "Transfer sizes will remain unknown until SteamPrefill's status command succeeds."
        )
        if status_error:
            detail += f" {status_error}"
        return games, detail

    existing = library_store.list_games()
    if existing:
        start_metadata_refresh()
        return existing, (
            "Kept the last known game library because SteamPrefill could not refresh it. "
            + (status_error or "Its selected-app config could not be read.")
        )

    raise HTTPException(
        status_code=422,
        detail=(
            "CacheDeck could not read SteamPrefill's selected games. Open Select games and save the list, "
            "then refresh. " + (status_error or "The selected-app config file was not found.")
        ),
    )


async def read_target_container_output(
    lines: int = 2500,
    *,
    since: str | None = None,
) -> str:
    args = ["docker", "logs"]
    if since:
        args.extend(["--since", since])
    args.extend(["--tail", str(lines), TARGET_CONTAINER])
    result = await run_process_async(args, timeout=20)
    return "\n".join(part for part in (result.stdout, result.stderr) if part)


async def read_current_prefill_output(lines: int = 2500) -> str:
    status = await get_prefill_status()
    if status.log_source == "cachedeck":
        log_file = shlex.quote(str(PurePosixPath(PREFILL_STATE_DIR) / "prefill.log"))
        result = await run_target_command(
            f"if [ -r {log_file} ]; then tail -n {lines} {log_file}; fi",
            timeout=20,
        )
        return "\n".join(part for part in (result.stdout, result.stderr) if part)
    if status.log_source == "container":
        return await read_target_container_output(lines, since=status.started_at)
    return ""


async def metadata_refresh_worker() -> None:
    if library_store.metadata_refreshing:
        return
    library_store.metadata_refreshing = True
    semaphore = asyncio.Semaphore(6)

    async def refresh_game(game: GameRecord) -> None:
        async with semaphore:
            if game.app_id is not None:
                needs_name = is_placeholder_game_name(game.name, game.app_id)
                if needs_name:
                    result = await asyncio.to_thread(resolve_steam_metadata_by_id, game.app_id)
                    if result is not None:
                        name, image_url, store_url = result
                        library_store.save_metadata(
                            game.key,
                            game.app_id,
                            image_url,
                            store_url,
                            name=name,
                        )
                        return
                if not game.image_url or not game.store_url:
                    library_store.save_metadata(
                        game.key,
                        game.app_id,
                        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{game.app_id}/header.jpg",
                        f"https://store.steampowered.com/app/{game.app_id}/",
                    )
                return

            result = await asyncio.to_thread(resolve_steam_metadata, game.name)
            if result is None:
                return
            app_id, image_url, store_url = result
            library_store.save_metadata(game.key, app_id, image_url, store_url)

    completed = False
    try:
        await asyncio.gather(*(refresh_game(game) for game in library_store.list_games()))
        completed = True
    finally:
        library_store.metadata_refreshing = False
    if completed:
        try:
            await sync_library_activity(deep_scan=True)
        except Exception:
            pass


def start_metadata_refresh() -> None:
    if library_store.metadata_refreshing:
        return
    asyncio.create_task(metadata_refresh_worker())


async def sync_library_activity(*, deep_scan: bool = False) -> None:
    status = await get_prefill_status()
    latest = history_store.latest()
    if status.running:
        try:
            output = await read_current_prefill_output(50000 if deep_scan else 2500)
            snapshot = parse_progress_snapshot(output)
            full_run = not latest or latest.job_id != status.job_id or latest.scope == "full"
            library_store.apply_progress(snapshot, full_run=full_run, job_id=status.job_id)
        except Exception:
            pass
        return

    if not latest:
        return

    if latest.source == "external":
        if latest.state == "finished" and latest.started_at:
            try:
                output = await read_target_container_output(5000, since=latest.started_at)
                library_store.apply_progress(parse_progress_snapshot(output), full_run=True, job_id=latest.job_id)
                if output_indicates_successful_prefill(output):
                    library_store.mark_all_downloaded(
                        latest.job_id,
                        latest.finished_at or utc_now(),
                    )
            except Exception:
                pass
        return

    if latest.state == "completed" and latest.scope == "full":
        library_store.mark_all_downloaded(latest.job_id, latest.finished_at or utc_now())

    running_item = next((item for item in queue_store.list() if item.state == "running"), None)
    if not running_item or running_item.job_id != latest.job_id:
        return

    if latest.state == "completed":
        finished = latest.finished_at or utc_now()
        try:
            output = await read_current_prefill_output()
            library_store.apply_progress(parse_progress_snapshot(output), full_run=False, job_id=latest.job_id)
        except Exception:
            pass
        queue_store.update(
            running_item.queue_id,
            state="completed",
            finished_at=finished,
            message="Steam checked the app and downloaded any available update.",
        )
        completed_game = next(
            (game for game in library_store.list_games() if game.app_id == running_item.app_id),
            None,
        )
        reported_this_run = (
            (completed_game.downloaded or completed_game.total)
            if completed_game
            else None
        )
        downloaded_this_run = parse_size_bytes(reported_this_run) > 0
        known_for_this_job = bool(
            completed_game and completed_game.last_downloaded_job_id == latest.job_id
        )
        library_store.update_by_app_id(
            running_item.app_id,
            status="downloaded",
            progress=100.0,
            queue_position=None,
            update_available=False,
            last_checked_at=finished,
            last_prefilled_at=(
                finished
                if downloaded_this_run
                else (completed_game.last_prefilled_at if completed_game else None)
            ),
            last_downloaded=(
                reported_this_run
                if downloaded_this_run
                else (
                    completed_game.last_downloaded
                    if known_for_this_job and completed_game
                    else "Not reported"
                )
            ),
            last_downloaded_job_id=latest.job_id,
            speed=None,
            eta=None,
            message="Checked and up to date at the last check.",
        )
    elif latest.state in {"failed", "stopped", "interrupted"}:
        finished = latest.finished_at or utc_now()
        queue_store.update(
            running_item.queue_id,
            state="failed",
            finished_at=finished,
            message=latest.message or "The per-game update failed.",
        )
        library_store.update_by_app_id(
            running_item.app_id,
            status="failed",
            queue_position=None,
            speed=None,
            eta=None,
            message=latest.message or "The per-game update failed.",
        )


async def game_queue_loop() -> None:
    await asyncio.sleep(5)
    while True:
        try:
            await sync_library_activity()
            raw_status = await get_raw_prefill_status()
            if not raw_status.running:
                item = queue_store.next_queued()
                if item is not None:
                    queue_store.update(
                        item.queue_id,
                        state="running",
                        started_at=utc_now(),
                        message="Steam is checking this app and will download an update if one exists.",
                    )
                    library_store.update_by_app_id(
                        item.app_id,
                        status="checking",
                        progress=0.0,
                        downloaded=None,
                        total=None,
                        speed=None,
                        eta=None,
                        queue_position=None,
                        update_available=None,
                        message="Checking Steam and applying an update if needed.",
                    )
                    try:
                        result = await launch_prefill_job(app_id=item.app_id, app_name=item.app_name)
                        queue_store.update(
                            item.queue_id,
                            job_id=result.status.job_id,
                            message="Steam is checking this app and will download an update if one exists.",
                        )
                    except Exception as exc:
                        queue_store.update(
                            item.queue_id,
                            state="failed",
                            finished_at=utc_now(),
                            message=str(exc),
                        )
                        library_store.update_by_app_id(
                            item.app_id,
                            status="failed",
                            message=str(exc),
                        )
        except Exception:
            pass
        await asyncio.sleep(4)


async def auto_recovery_loop() -> None:
    await asyncio.sleep(20)
    while True:
        try:
            if AUTO_RESUME_INTERRUPTED:
                latest = history_store.latest()
                raw_status = await get_raw_prefill_status()
                if (
                    latest
                    and latest.source == "cachedeck"
                    and latest.state in {"running", "interrupted"}
                    and not latest.auto_resume_attempted
                    and raw_status.state == "idle"
                ):
                    history_store.update(
                        latest.job_id,
                        state="interrupted",
                        finished_at=utc_now(),
                        auto_resume_attempted=True,
                        message="CacheDeck is attempting one automatic resume.",
                    )
                    with contextlib.suppress(Exception):
                        await launch_prefill_job(
                            resume_of=latest.job_id,
                            app_id=latest.app_id if latest.scope == "single" else None,
                            app_name=latest.app_name,
                        )
        except Exception:
            pass
        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(_: FastAPI):
    with contextlib.suppress(OSError):
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    recovery_task = asyncio.create_task(auto_recovery_loop())
    queue_task = asyncio.create_task(game_queue_loop())
    try:
        yield
    finally:
        recovery_task.cancel()
        queue_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await recovery_task
        with contextlib.suppress(asyncio.CancelledError):
            await queue_task


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    docs_url="/api/docs",
    redoc_url=None,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/favicon.svg", include_in_schema=False)
async def favicon() -> FileResponse:
    return FileResponse(STATIC_DIR / "favicon.svg", media_type="image/svg+xml")


@app.get("/static/{asset_path:path}", include_in_schema=False)
async def static_asset(asset_path: str) -> FileResponse:
    candidate = (STATIC_DIR / asset_path).resolve()
    if STATIC_DIR.resolve() not in candidate.parents or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset not found.")
    return FileResponse(candidate)


@app.get("/api/health")
async def health() -> dict[str, object]:
    target = await inspect_target()
    return {
        "app": APP_NAME,
        "version": APP_VERSION,
        "target": TARGET_CONTAINER,
        "prefill_dir": PREFILL_DIR,
        "prefill_user": PREFILL_USER,
        "prefill_state_dir": PREFILL_STATE_DIR,
        "config_dir": str(CONFIG_DIR),
        "auto_resume": AUTO_RESUME_INTERRUPTED,
        "running": target["running"],
        "status": target["status"],
        "detail": target["detail"],
        "time": utc_now(),
    }


@app.get("/api/logs")
async def logs(lines: int = Query(default=150, ge=10, le=2000)) -> CommandResult:
    try:
        result = await run_process_async(
            ["docker", "logs", "--tail", str(lines), TARGET_CONTAINER], timeout=20
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="Timed out while reading the target container logs.") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc
    return CommandResult(
        ok=result.returncode == 0,
        code=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


@app.post("/api/action")
async def action(request: ActionRequest) -> CommandResult:
    if request.action == "status":
        games, message = await refresh_selected_library()
        lines = [message, "", f"Selected games: {len(games)}"]
        for game in games:
            size = game.download_size or "size unavailable"
            lines.append(f"{game.name} | {size}")
        return CommandResult(ok=True, code=0, stdout="\n".join(lines), stderr="")

    command = ALLOWED_ACTIONS.get(request.action)
    if command is None:
        raise HTTPException(status_code=400, detail="Unsupported action.")
    try:
        result = await run_target_command(command, timeout=300)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="SteamPrefill did not finish within five minutes.") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc
    return CommandResult(
        ok=result.returncode == 0,
        code=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


@app.get("/api/prefill/status", response_model=PrefillStatus)
async def prefill_status() -> PrefillStatus:
    return await get_prefill_status()


@app.post("/api/prefill/start", response_model=PrefillStartResult)
async def start_prefill() -> PrefillStartResult:
    return await launch_prefill_job()


async def set_prefill_paused(paused: bool) -> PrefillStartResult:
    current = await get_prefill_status()
    operation = "pause" if paused else "resume"
    if not current.running:
        raise HTTPException(status_code=409, detail="No prefill job is running.")
    if current.worker_pid is None:
        raise HTTPException(
            status_code=409,
            detail=f"The active SteamPrefill process cannot be controlled, so CacheDeck cannot {operation} it.",
        )
    if current.paused == paused:
        return PrefillStartResult(
            ok=True,
            message=f"Prefill is already {'paused' if paused else 'running'}.",
            status=current,
        )

    signal_name = "STOP" if paused else "CONT"
    control_command = f"""
pid={current.worker_pid}
if ! kill -0 "$pid" 2>/dev/null; then exit 3; fi
cmdline="$(tr '\\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
if [[ ! "$cmdline" =~ SteamPrefill.*[[:space:]]prefill([[:space:]]|$) ]]; then exit 4; fi
kill -{signal_name} "$pid"
""".strip()
    try:
        result = await run_target_command(control_command, timeout=10)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail=f"Timed out while trying to {operation} the prefill job.") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc
    if result.returncode != 0:
        raise HTTPException(status_code=409, detail=f"The active prefill process could not be {operation}d.")

    await asyncio.sleep(0.25)
    status = await get_prefill_status()
    return PrefillStartResult(
        ok=True,
        message=(
            "Prefill paused. CacheDeck and the target scheduler remain online."
            if paused
            else "Prefill resumed. An in-flight request may retry if it timed out while paused."
        ),
        status=status,
    )


@app.post("/api/prefill/pause", response_model=PrefillStartResult)
async def pause_prefill() -> PrefillStartResult:
    return await set_prefill_paused(True)


@app.post("/api/prefill/resume", response_model=PrefillStartResult)
async def resume_prefill() -> PrefillStartResult:
    return await set_prefill_paused(False)


@app.post("/api/prefill/stop", response_model=PrefillStartResult)
async def stop_prefill() -> PrefillStartResult:
    current = await get_prefill_status()
    if not current.running:
        raise HTTPException(status_code=409, detail="No prefill job is running.")
    if not current.managed or current.pid is None:
        raise HTTPException(
            status_code=409,
            detail="This prefill was started outside CacheDeck and will not be stopped automatically.",
        )

    stop_command = f"""
state_dir={shlex.quote(PREFILL_STATE_DIR)}
wrapper_file="$state_dir/prefill-wrapper.sh"
pid={current.pid}
worker_pid={current.worker_pid or ""}
if [[ "$worker_pid" =~ ^[0-9]+$ ]]; then kill -CONT "$worker_pid" 2>/dev/null || true; fi
if ! kill -0 "$pid" 2>/dev/null; then exit 3; fi
cmdline="$(tr '\\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
if [[ "$cmdline" != *"$wrapper_file"* ]]; then exit 4; fi
kill -TERM "$pid"
""".strip()
    try:
        result = await run_target_command(stop_command, timeout=10)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="Timed out while stopping the prefill job.") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc
    if result.returncode != 0:
        raise HTTPException(status_code=409, detail="The managed prefill process is no longer available.")

    await asyncio.sleep(0.4)
    status = await get_prefill_status()
    return PrefillStartResult(ok=True, message="Stop requested for the active CacheDeck prefill.", status=status)


@app.get("/api/prefill/log", response_model=PrefillLogResult)
async def prefill_log(
    lines: int = Query(default=400, ge=10, le=5000),
    source: Literal["auto", "cachedeck", "container", "none"] = Query(default="auto"),
) -> PrefillLogResult:
    resolved_source = source
    if source == "auto":
        status = await get_prefill_status()
        if status.state == "unavailable":
            raise HTTPException(status_code=503, detail=status.message)
        resolved_source = status.log_source

    if resolved_source == "cachedeck":
        log_file = shlex.quote(str(PurePosixPath(PREFILL_STATE_DIR) / "prefill.log"))
        try:
            result = await run_target_command(
                f"if [ -r {log_file} ]; then tail -n {lines} {log_file}; fi", timeout=20
            )
        except subprocess.TimeoutExpired as exc:
            raise HTTPException(status_code=504, detail="Timed out while reading the CacheDeck prefill log.") from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc
        return PrefillLogResult(ok=result.returncode == 0, source="cachedeck", stdout=result.stdout, stderr=result.stderr)

    if resolved_source == "container":
        try:
            result = await run_process_async(
                ["docker", "logs", "--tail", str(lines), TARGET_CONTAINER], timeout=20
            )
        except subprocess.TimeoutExpired as exc:
            raise HTTPException(status_code=504, detail="Timed out while reading the target container log.") from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Unable to run Docker: {exc}") from exc
        return PrefillLogResult(ok=result.returncode == 0, source="container", stdout=result.stdout, stderr=result.stderr)

    return PrefillLogResult(ok=True, source="none", stdout="No prefill output is available yet.")


@app.get("/api/prefill/log.txt", response_class=PlainTextResponse)
async def prefill_log_download(
    lines: int = Query(default=5000, ge=10, le=20000),
) -> PlainTextResponse:
    result = await prefill_log(lines=lines, source="auto")
    return PlainTextResponse(
        content="\n".join(item for item in (result.stdout, result.stderr) if item),
        headers={"Content-Disposition": "attachment; filename=cachedeck-prefill.log"},
    )


@app.get("/api/prefill/history", response_model=list[HistoryRecord])
async def prefill_history() -> list[HistoryRecord]:
    return history_store.list()


@app.get("/api/library", response_model=LibraryResponse)
async def library(refresh: bool = Query(default=False)) -> LibraryResponse:
    message = ""
    if refresh or not library_store.list_games():
        _, message = await refresh_selected_library()
    else:
        start_metadata_refresh()
    await sync_library_activity(deep_scan=refresh or bool(message))
    return build_library_response(library_store, queue_store, message=message)


@app.post("/api/library/refresh", response_model=LibraryResponse)
async def refresh_library() -> LibraryResponse:
    _, message = await refresh_selected_library()
    await sync_library_activity(deep_scan=True)
    return build_library_response(library_store, queue_store, message=message)


@app.post("/api/library/metadata", response_model=LibraryResponse)
async def refresh_library_metadata() -> LibraryResponse:
    start_metadata_refresh()
    return build_library_response(
        library_store,
        queue_store,
        message="Steam artwork and app IDs are being resolved in the background.",
    )


def enqueue_library_games(app_ids: list[int]) -> tuple[int, list[str]]:
    unique_ids = list(dict.fromkeys(app_ids))
    if not unique_ids:
        raise HTTPException(status_code=400, detail="Choose at least one Steam app.")
    if len(unique_ids) > 1000:
        raise HTTPException(status_code=400, detail="A maximum of 1,000 games can be queued at once.")

    games_by_id = {
        game.app_id: game
        for game in library_store.list_games()
        if game.app_id is not None
    }
    missing = [app_id for app_id in unique_ids if app_id not in games_by_id]
    if missing:
        raise HTTPException(
            status_code=404,
            detail=f"{len(missing)} Steam app(s) are not in the selected library.",
        )

    queued_names: list[str] = []
    active_ids = {item.app_id for item in queue_store.active()}
    for app_id in unique_ids:
        game = games_by_id[app_id]
        queue_item = queue_store.enqueue(
            GameQueueItem(
                queue_id=uuid.uuid4().hex,
                app_id=app_id,
                app_name=game.name,
                requested_at=utc_now(),
            )
        )
        library_store.update_by_app_id(
            app_id,
            status="queued" if queue_item.state == "queued" else "checking",
            progress=0.0,
            update_available=None,
            message=(
                "Queued for a Steam update check. Any available update will be downloaded automatically."
                if queue_item.state == "queued"
                else "Steam is already checking this app."
            ),
        )
        if app_id not in active_ids:
            queued_names.append(game.name)
    return len(queued_names), queued_names


@app.post("/api/library/games/{app_id}/update", response_model=LibraryResponse)
async def queue_game_update(app_id: int) -> LibraryResponse:
    count, names = enqueue_library_games([app_id])
    return build_library_response(
        library_store,
        queue_store,
        message=f"{names[0]} was added to the update queue." if count else "That game is already queued or running.",
    )


@app.post("/api/library/queue", response_model=LibraryResponse)
async def queue_library_games(request: BulkQueueRequest) -> LibraryResponse:
    count, _ = enqueue_library_games(request.app_ids)
    return build_library_response(
        library_store,
        queue_store,
        message=(
            f"Added {count} game{'s' if count != 1 else ''} to the update queue."
            if count
            else "Every selected game is already queued or running."
        ),
    )


@app.post("/api/library/games/{app_id}/forget", response_model=LibraryResponse)
async def forget_game_status(app_id: int) -> LibraryResponse:
    active_item = next(
        (item for item in queue_store.active() if item.app_id == app_id),
        None,
    )
    if active_item is not None:
        raise HTTPException(
            status_code=409,
            detail="Remove this game from the queue or wait for its current check to finish first.",
        )
    game = library_store.forget_status(app_id)
    if game is None:
        raise HTTPException(status_code=404, detail="That Steam app is not in the selected library.")
    return build_library_response(
        library_store,
        queue_store,
        message=f"Forgot CacheDeck's status for {game.name}. LANCache data was not deleted.",
    )


@app.delete("/api/library/queue/{queue_id}", response_model=LibraryResponse)
async def cancel_game_update(queue_id: str) -> LibraryResponse:
    item = queue_store.cancel(queue_id)
    if item is None:
        raise HTTPException(status_code=409, detail="Only queued updates can be removed.")
    library_store.update_by_app_id(
        item.app_id,
        status="selected",
        queue_position=None,
        message="Selected for prefill.",
    )
    return build_library_response(
        library_store,
        queue_store,
        message=f"Removed {item.app_name} from the queue.",
    )


@app.get("/api/schedule", response_model=ScheduleInfo)
async def schedule() -> ScheduleInfo:
    return await get_schedule_info()


@app.get("/api/diagnostics", response_model=DiagnosticsResult)
async def diagnostics() -> DiagnosticsResult:
    return await run_diagnostics()


async def stream_subprocess_to_websocket(
    websocket: WebSocket, args: list[str]
) -> None:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        await websocket.send_text(f"Unable to start log stream: {exc}\n")
        return

    assert process.stdout is not None

    async def pump_output() -> None:
        while True:
            chunk = await process.stdout.read(8192)
            if not chunk:
                return
            await websocket.send_bytes(chunk)

    async def wait_for_disconnect() -> None:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                return

    pump_task = asyncio.create_task(pump_output())
    disconnect_task = asyncio.create_task(wait_for_disconnect())
    try:
        done, pending = await asyncio.wait(
            {pump_task, disconnect_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            with contextlib.suppress(WebSocketDisconnect, RuntimeError):
                task.result()
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=2)
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()


@app.websocket("/ws/prefill-log")
async def prefill_log_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    requested = websocket.query_params.get("source", "auto")
    if requested not in {"auto", "cachedeck", "container", "none"}:
        requested = "auto"

    resolved = requested
    if requested == "auto":
        status = await get_prefill_status()
        resolved = status.log_source

    if resolved == "none":
        await websocket.send_text("No prefill output is available yet.\n")
        await websocket.close(code=1000)
        return

    if resolved == "cachedeck":
        log_path = str(PurePosixPath(PREFILL_STATE_DIR) / "prefill.log")
        command = (
            f"mkdir -p {shlex.quote(PREFILL_STATE_DIR)}; "
            f"touch {shlex.quote(log_path)}; "
            f"exec tail -n 400 -f {shlex.quote(log_path)}"
        )
        args = docker_exec_command(command)
    else:
        args = ["docker", "logs", "--tail", "400", "--follow", TARGET_CONTAINER]

    await stream_subprocess_to_websocket(websocket, args)


@app.websocket("/ws/terminal")
async def terminal(websocket: WebSocket) -> None:
    await websocket.accept()
    target = await inspect_target()
    if not target["running"]:
        await websocket.send_text(
            "\r\n\x1b[31mCacheDeck could not connect to "
            f"{TARGET_CONTAINER}: {target['status']}.\x1b[0m\r\n"
        )
        await websocket.close(code=1011)
        return

    master_fd, slave_fd = pty.openpty()
    import fcntl

    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
    environment = os.environ.copy()
    environment["TERM"] = "xterm-256color"

    try:
        process = subprocess.Popen(
            docker_exec_command("exec bash", interactive=True),
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            env=environment,
            close_fds=True,
            start_new_session=True,
        )
    except OSError as exc:
        os.close(master_fd)
        os.close(slave_fd)
        await websocket.send_text(f"\r\n\x1b[31mUnable to start Docker terminal: {exc}\x1b[0m\r\n")
        await websocket.close(code=1011)
        return

    os.close(slave_fd)

    async def pump_output() -> None:
        loop = asyncio.get_running_loop()
        while process.poll() is None:
            ready, _, _ = await loop.run_in_executor(None, lambda: select.select([master_fd], [], [], 0.2))
            if not ready:
                continue
            try:
                data = os.read(master_fd, 8192)
            except OSError:
                break
            if not data:
                break
            try:
                await websocket.send_bytes(data)
            except RuntimeError:
                break

    output_task = asyncio.create_task(pump_output())
    try:
        while True:
            message = await websocket.receive()
            text = message.get("text")
            if text is not None:
                if text.startswith("__RESIZE__:"):
                    try:
                        _, columns, rows = text.split(":", 2)
                        fcntl.ioctl(
                            master_fd,
                            termios.TIOCSWINSZ,
                            struct.pack("HHHH", int(rows), int(columns), 0, 0),
                        )
                    except (ValueError, OSError):
                        pass
                else:
                    os.write(master_fd, text.encode("utf-8"))
                continue
            data = message.get("bytes")
            if data is not None:
                os.write(master_fd, data)
    except WebSocketDisconnect:
        pass
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.to_thread(process.wait), timeout=3)
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        output_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await output_task
        with contextlib.suppress(OSError):
            os.close(master_fd)
