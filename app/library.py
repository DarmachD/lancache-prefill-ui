from __future__ import annotations

import json
import re
import threading
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field

GameStatus = Literal[
    "selected",
    "queued",
    "checking",
    "downloading",
    "downloaded",
    "update_available",
    "failed",
]

QueueState = Literal["queued", "running", "completed", "failed", "cancelled"]


class SelectedApp(BaseModel):
    provider: str = "steamprefill"
    app_id: int | None = None
    name: str
    download_size: str | None = None


class ProgressSnapshot(BaseModel):
    app_name: str | None = None
    progress: float | None = None
    downloaded: str | None = None
    total: str | None = None
    speed: str | None = None
    eta: str | None = None
    update_available_for: list[str] = Field(default_factory=list)
    up_to_date_for: list[str] = Field(default_factory=list)
    completed_for: list[str] = Field(default_factory=list)
    failed_for: list[str] = Field(default_factory=list)
    downloaded_for: dict[str, str] = Field(default_factory=dict)
    total_for: dict[str, str] = Field(default_factory=dict)


class GameRecord(BaseModel):
    key: str
    provider: str = "steamprefill"
    app_id: int | None = None
    name: str
    download_size: str | None = None
    image_url: str | None = None
    store_url: str | None = None
    selected: bool = True
    status: GameStatus = "selected"
    progress: float | None = None
    downloaded: str | None = None
    total: str | None = None
    speed: str | None = None
    eta: str | None = None
    queue_position: int | None = None
    update_available: bool | None = None
    last_checked_at: str | None = None
    last_prefilled_at: str | None = None
    last_downloaded: str | None = None
    last_downloaded_job_id: str | None = None
    current_manifest_id: str | None = None
    target_manifest_id: str | None = None
    message: str = "Selected for prefill, but LANCache presence has not been verified."
    verification_source: str | None = None
    verified_at: str | None = None
    metadata_attempts: int = 0
    metadata_retry_at: str | None = None
    metadata_error: str | None = None
    metadata_resolved_at: str | None = None


class GameQueueItem(BaseModel):
    queue_id: str
    provider: str = "steamprefill"
    app_id: int
    app_name: str
    requested_at: str
    state: QueueState = "queued"
    started_at: str | None = None
    finished_at: str | None = None
    job_id: str | None = None
    message: str = "Queued for a check and update."


class LibrarySummary(BaseModel):
    total: int = 0
    downloaded: int = 0
    queued: int = 0
    downloading: int = 0
    update_available: int = 0
    failed: int = 0
    unresolved: int = 0
    known_size_count: int = 0
    total_size_bytes: int = 0
    queue_remaining_bytes: int = 0
    latest_run_downloaded_bytes: int = 0


class LibraryResponse(BaseModel):
    generated_at: str
    last_refreshed_at: str | None = None
    metadata_refreshing: bool = False
    message: str = ""
    summary: LibrarySummary
    games: list[GameRecord]
    queue: list[GameQueueItem]


def steam_artwork_url(app_id: int) -> str:
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"


def steam_store_url(app_id: int) -> str:
    return f"https://store.steampowered.com/app/{app_id}/"


def placeholder_game_name(app_id: int) -> str:
    return f"Steam app {app_id}"


def is_placeholder_game_name(name: str, app_id: int | None = None) -> bool:
    value = (name or "").strip().casefold()
    if app_id is not None and value == placeholder_game_name(app_id).casefold():
        return True
    return bool(re.fullmatch(r"steam app \d+", value))


class LibraryStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.metadata_refreshing = False

    def _empty(self) -> dict[str, object]:
        return {
            "last_refreshed_at": None,
            "last_completed_job_id": None,
            "last_activity_job_id": None,
            "games": [],
        }

    def _read_unlocked(self) -> dict[str, object]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return self._empty()
        if not isinstance(raw, dict):
            return self._empty()
        raw.setdefault("last_refreshed_at", None)
        raw.setdefault("last_completed_job_id", None)
        raw.setdefault("last_activity_job_id", None)
        raw.setdefault("games", [])
        return raw

    def _write_unlocked(self, raw: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        temp.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
        temp.replace(self.path)

    def list_games(self) -> list[GameRecord]:
        with self._lock:
            raw = self._read_unlocked()
            result: list[GameRecord] = []
            for item in raw.get("games", []):
                try:
                    result.append(GameRecord.model_validate(item))
                except Exception:
                    continue
            return result

    def last_refreshed_at(self) -> str | None:
        with self._lock:
            return self._read_unlocked().get("last_refreshed_at") or None

    def last_activity_job_id(self) -> str | None:
        with self._lock:
            return self._read_unlocked().get("last_activity_job_id") or None

    def replace_selected(self, selected: list[SelectedApp], refreshed_at: str) -> list[GameRecord]:
        with self._lock:
            raw = self._read_unlocked()
            existing: dict[str, GameRecord] = {}
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                existing[game.key] = game

            next_games: list[GameRecord] = []
            seen: set[str] = set()
            for app in selected:
                key = game_key(app.app_id, app.name)
                if key in seen:
                    continue
                seen.add(key)
                previous = existing.get(key)
                if previous is None and app.app_id is not None:
                    previous = next(
                        (g for g in existing.values() if g.app_id == app.app_id),
                        None,
                    )
                if previous is None:
                    previous = next(
                        (g for g in existing.values() if normalise_name(g.name) == normalise_name(app.name)),
                        None,
                    )
                if previous:
                    resolved_app_id = app.app_id or previous.app_id
                    resolved_name = app.name or previous.name
                    if (
                        is_placeholder_game_name(resolved_name, resolved_app_id)
                        and not is_placeholder_game_name(previous.name, previous.app_id)
                    ):
                        resolved_name = previous.name
                    update = {
                        "key": key,
                        "provider": app.provider or previous.provider,
                        "app_id": resolved_app_id,
                        "name": resolved_name,
                        "download_size": app.download_size or previous.download_size,
                        "image_url": previous.image_url or (steam_artwork_url(resolved_app_id) if resolved_app_id else None),
                        "store_url": previous.store_url or (steam_store_url(resolved_app_id) if resolved_app_id else None),
                        "selected": True,
                    }
                    # A catalogue refresh must never erase live or completed state. Queue
                    # reconciliation and activity parsing own those transitions.
                    game = previous.model_copy(update=update)
                else:
                    game = GameRecord(
                        key=key,
                        provider=app.provider,
                        app_id=app.app_id,
                        name=app.name,
                        download_size=app.download_size,
                        image_url=steam_artwork_url(app.app_id) if app.app_id else None,
                        store_url=steam_store_url(app.app_id) if app.app_id else None,
                    )
                next_games.append(game)

            raw["last_refreshed_at"] = refreshed_at
            raw["games"] = [g.model_dump(mode="json") for g in next_games]
            self._write_unlocked(raw)
            return next_games

    def update_game(self, key: str, **changes: object) -> GameRecord | None:
        with self._lock:
            raw = self._read_unlocked()
            games: list[GameRecord] = []
            updated: GameRecord | None = None
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                if game.key == key:
                    game = game.model_copy(update=changes)
                    updated = game
                games.append(game)
            if changes.get("last_downloaded_job_id"):
                raw["last_activity_job_id"] = changes["last_downloaded_job_id"]
            raw["games"] = [g.model_dump(mode="json") for g in games]
            self._write_unlocked(raw)
            return updated

    def update_by_app_id(self, app_id: int, **changes: object) -> GameRecord | None:
        with self._lock:
            raw = self._read_unlocked()
            games: list[GameRecord] = []
            updated: GameRecord | None = None
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                if game.app_id == app_id:
                    game = game.model_copy(update=changes)
                    updated = game
                games.append(game)
            if changes.get("last_downloaded_job_id"):
                raw["last_activity_job_id"] = changes["last_downloaded_job_id"]
            raw["games"] = [g.model_dump(mode="json") for g in games]
            self._write_unlocked(raw)
            return updated

    def update_by_name(self, name: str, **changes: object) -> GameRecord | None:
        target = normalise_name(name)
        with self._lock:
            raw = self._read_unlocked()
            games: list[GameRecord] = []
            updated: GameRecord | None = None
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                if normalise_name(game.name) == target:
                    game = game.model_copy(update=changes)
                    updated = game
                games.append(game)
            if changes.get("last_downloaded_job_id"):
                raw["last_activity_job_id"] = changes["last_downloaded_job_id"]
            raw["games"] = [g.model_dump(mode="json") for g in games]
            self._write_unlocked(raw)
            return updated

    def mark_all_downloaded(self, job_id: str, when: str) -> bool:
        with self._lock:
            raw = self._read_unlocked()
            if raw.get("last_completed_job_id") == job_id:
                return False
            games: list[GameRecord] = []
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                downloaded_in_this_job = game.last_downloaded_job_id == job_id
                game = game.model_copy(
                    update={
                        "status": "downloaded",
                        "progress": 100.0,
                        "update_available": False,
                        "last_checked_at": when,
                        "last_prefilled_at": when if downloaded_in_this_job else game.last_prefilled_at,
                        "queue_position": None,
                        "speed": None,
                        "eta": None,
                        "message": "Checked and up to date at the last successful prefill.",
                        "verification_source": "full_run",
                        "verified_at": when,
                    }
                )
                games.append(game)
            raw["last_completed_job_id"] = job_id
            raw["last_activity_job_id"] = job_id
            raw["games"] = [g.model_dump(mode="json") for g in games]
            self._write_unlocked(raw)
            return True

    def apply_progress(
        self,
        snapshot: ProgressSnapshot,
        *,
        full_run: bool = False,
        job_id: str | None = None,
    ) -> None:
        with self._lock:
            raw = self._read_unlocked()
            games: list[GameRecord] = []
            current = normalise_name(snapshot.app_name or "")
            update_names = {normalise_name(v) for v in snapshot.update_available_for}
            up_to_date_names = {normalise_name(v) for v in snapshot.up_to_date_for}
            completed_names = {normalise_name(v) for v in snapshot.completed_for}
            failed_names = {normalise_name(v) for v in snapshot.failed_for}
            downloaded_for = {normalise_name(name): value for name, value in snapshot.downloaded_for.items()}
            total_for = {normalise_name(name): value for name, value in snapshot.total_for.items()}
            now = utc_now()
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                name_key = normalise_name(game.name)
                changes: dict[str, object] = {}
                if name_key in update_names:
                    changes.update(
                        status="update_available",
                        update_available=True,
                        last_checked_at=now,
                        message="An update was detected.",
                    )
                if name_key in up_to_date_names:
                    changes.update(
                        status="downloaded",
                        progress=100.0,
                        downloaded=None,
                        total=None,
                        last_downloaded="0 B",
                        last_downloaded_job_id=job_id or game.last_downloaded_job_id,
                        update_available=False,
                        last_checked_at=now,
                        speed=None,
                        eta=None,
                        queue_position=None,
                        message="Already up to date at the last check.",
                        verification_source="steam_check",
                        verified_at=now,
                    )
                if name_key in completed_names:
                    completed_download = downloaded_for.get(name_key) or total_for.get(name_key) or game.downloaded
                    changes.update(
                        status="downloaded",
                        progress=100.0,
                        downloaded=total_for.get(name_key) or game.total or game.downloaded,
                        total=total_for.get(name_key) or game.total,
                        last_downloaded=completed_download or game.last_downloaded,
                        last_downloaded_job_id=job_id or game.last_downloaded_job_id,
                        update_available=False,
                        last_checked_at=now,
                        last_prefilled_at=now,
                        speed=None,
                        eta=None,
                        queue_position=None,
                        message="Downloaded and up to date.",
                        verification_source="observed_download",
                        verified_at=now,
                    )
                if name_key in failed_names:
                    changes.update(status="failed", speed=None, eta=None, message="The last prefill attempt failed.")
                is_finished = name_key in completed_names or name_key in up_to_date_names
                if current and name_key == current and not is_finished:
                    changes.update(
                        status="downloading",
                        progress=(
                            max(0.0, min(100.0, snapshot.progress))
                            if snapshot.progress is not None else None
                        ),
                        downloaded=snapshot.downloaded,
                        total=snapshot.total,
                        speed=snapshot.speed,
                        eta=snapshot.eta,
                        update_available=True,
                        message="Downloading into LANCache now.",
                    )
                elif full_run and not changes and game.status not in {"downloaded", "update_available", "failed"}:
                    changes.update(status="queued", message="Queued in the active full prefill run.")
                if changes:
                    game = game.model_copy(update=changes)
                games.append(game)
            if job_id and (completed_names or up_to_date_names):
                raw["last_activity_job_id"] = job_id
            raw["games"] = [g.model_dump(mode="json") for g in games]
            self._write_unlocked(raw)

    def mark_provider_verified(self, app_ids: set[int], when: str) -> int:
        """Import explicit app-level success evidence from the active provider."""
        if not app_ids:
            return 0
        with self._lock:
            raw = self._read_unlocked()
            games: list[GameRecord] = []
            changed = 0
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                if game.app_id in app_ids and game.status not in {"downloading", "checking", "queued"}:
                    game = game.model_copy(update={
                        "status": "downloaded",
                        "progress": 100.0,
                        "update_available": False,
                        "last_checked_at": game.last_checked_at or when,
                        "message": "SteamPrefill reports this app as previously downloaded.",
                        "verification_source": "provider_history",
                        "verified_at": when,
                    })
                    changed += 1
                games.append(game)
            if changed:
                raw["games"] = [game.model_dump(mode="json") for game in games]
                self._write_unlocked(raw)
            return changed

    def mark_manually_downloaded(self, app_id: int, when: str) -> GameRecord | None:
        return self.update_by_app_id(
            app_id,
            status="downloaded",
            progress=100.0,
            downloaded=None,
            total=None,
            speed=None,
            eta=None,
            queue_position=None,
            update_available=False,
            verified_at=when,
            verification_source="manual",
            message="Manually marked as present in LANCache. CacheDeck has not independently verified every cached object.",
        )

    def forget_status(self, app_id: int) -> GameRecord | None:
        return self.update_by_app_id(
            app_id,
            status="selected",
            progress=None,
            downloaded=None,
            total=None,
            speed=None,
            eta=None,
            queue_position=None,
            update_available=None,
            last_checked_at=None,
            last_prefilled_at=None,
            last_downloaded=None,
            last_downloaded_job_id=None,
            verification_source=None,
            verified_at=None,
            message="CacheDeck status forgotten. LANCache presence is unverified; no cache data was deleted.",
        )

    def save_metadata(
        self,
        key: str,
        app_id: int,
        image_url: str | None,
        store_url: str | None,
        name: str | None = None,
    ) -> None:
        with self._lock:
            raw = self._read_unlocked()
            games: list[GameRecord] = []
            for item in raw.get("games", []):
                try:
                    game = GameRecord.model_validate(item)
                except Exception:
                    continue
                if game.key == key:
                    new_key = game_key(app_id, game.name)
                    game = game.model_copy(
                        update={
                            "key": new_key,
                            "app_id": app_id,
                            "name": name or game.name,
                            "image_url": image_url,
                            "store_url": store_url,
                            "metadata_attempts": 0,
                            "metadata_retry_at": None,
                            "metadata_error": None,
                            "metadata_resolved_at": utc_now(),
                        }
                    )
                games.append(game)
            raw["games"] = [g.model_dump(mode="json") for g in games]
            self._write_unlocked(raw)


class QueueStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def _read_unlocked(self) -> list[GameQueueItem]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return []
        if not isinstance(raw, list):
            return []
        result: list[GameQueueItem] = []
        for item in raw:
            try:
                result.append(GameQueueItem.model_validate(item))
            except Exception:
                continue
        return result

    def _write_unlocked(self, items: list[GameQueueItem]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        temp.write_text(
            json.dumps([item.model_dump(mode="json") for item in items[-100:]], indent=2) + "\n",
            encoding="utf-8",
        )
        temp.replace(self.path)

    def list(self) -> list[GameQueueItem]:
        with self._lock:
            return self._read_unlocked()

    def active(self) -> list[GameQueueItem]:
        return [item for item in self.list() if item.state in {"queued", "running"}]

    def enqueue(self, item: GameQueueItem) -> GameQueueItem:
        with self._lock:
            items = self._read_unlocked()
            existing = next(
                (x for x in items if x.app_id == item.app_id and x.state in {"queued", "running"}),
                None,
            )
            if existing:
                return existing
            items.append(item)
            self._write_unlocked(items)
            return item

    def next_queued(self) -> GameQueueItem | None:
        return next((item for item in self.list() if item.state == "queued"), None)

    def update(self, queue_id: str, **changes: object) -> GameQueueItem | None:
        with self._lock:
            items = self._read_unlocked()
            updated: GameQueueItem | None = None
            for index, item in enumerate(items):
                if item.queue_id != queue_id:
                    continue
                item = item.model_copy(update=changes)
                items[index] = item
                updated = item
                break
            self._write_unlocked(items)
            return updated

    def cancel(self, queue_id: str) -> GameQueueItem | None:
        item = next((x for x in self.list() if x.queue_id == queue_id), None)
        if not item or item.state != "queued":
            return None
        return self.update(
            queue_id,
            state="cancelled",
            finished_at=utc_now(),
            message="Removed from the queue.",
        )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalise_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = value.casefold().replace("™", "").replace("®", "")
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def game_key(app_id: int | None, name: str) -> str:
    if app_id is not None:
        return f"steam:{app_id}"
    compact = re.sub(r"[^a-z0-9]+", "-", normalise_name(name)).strip("-")
    return f"name:{compact or 'unknown'}"


ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
SIZE_RE = re.compile(r"(?P<size>\d+(?:\.\d+)?\s*(?:[KMGTPE]i?B|bytes?))", re.I)
APP_ID_RE = re.compile(r"(?<!\d)(?P<id>\d{3,10})(?!\d)")
SIZE_UNITS = {"B": 0, "KB": 1, "KIB": 1, "MB": 2, "MIB": 2, "GB": 3, "GIB": 3, "TB": 4, "TIB": 4, "PB": 5, "PIB": 5, "EB": 6, "EIB": 6}


def parse_size_bytes(value: str | None) -> int:
    if not value:
        return 0
    match = SIZE_RE.search(value)
    if not match:
        return 0
    number_match = re.search(r"\d+(?:\.\d+)?", match.group("size"))
    unit_match = re.search(r"(?:[KMGTPE]i?B|bytes?)", match.group("size"), re.I)
    if not number_match or not unit_match:
        return 0
    unit = unit_match.group(0).upper()
    if unit.startswith("BYTE"):
        unit = "B"
    return int(float(number_match.group(0)) * (1024 ** SIZE_UNITS.get(unit, 0)))


def clean_terminal_output(value: str) -> str:
    value = ANSI_RE.sub("", value or "")
    return value.replace("\r\n", "\n").replace("\r", "\n")


def parse_selected_apps_status(output: str) -> list[SelectedApp]:
    text = clean_terminal_output(output)
    lines = [line.rstrip() for line in text.splitlines()]
    rows: list[list[str]] = []
    for line in lines:
        if not line.strip():
            continue
        if any(char in line for char in ("│", "|")):
            parts = [part.strip() for part in re.split(r"[│|]", line)]
            parts = [part for part in parts if part]
            if len(parts) >= 2 and not all(set(part) <= set("-═─━+┼┬┴ ") for part in parts):
                rows.append(parts)

    result: list[SelectedApp] = []
    header: list[str] | None = None
    for row in rows:
        joined = " ".join(row).casefold()
        if "app" in joined and ("size" in joined or "download" in joined):
            header = [part.casefold() for part in row]
            continue
        if header:
            name_index = next((i for i, value in enumerate(header) if "app" in value and "id" not in value), 0)
            size_index = next((i for i, value in enumerate(header) if "size" in value), None)
            id_index = next((i for i, value in enumerate(header) if "id" in value), None)
            if name_index >= len(row):
                continue
            name = row[name_index].strip()
            if not name or name.casefold() in {"total", "selected"}:
                continue
            app_id: int | None = None
            if id_index is not None and id_index < len(row):
                match = APP_ID_RE.search(row[id_index])
                if match:
                    app_id = int(match.group("id"))
            if app_id is None:
                for part in row:
                    if part == name:
                        continue
                    match = APP_ID_RE.fullmatch(part.strip())
                    if match:
                        app_id = int(match.group("id"))
                        break
            size = row[size_index].strip() if size_index is not None and size_index < len(row) else None
            if size and not SIZE_RE.search(size):
                size = None
            result.append(SelectedApp(app_id=app_id, name=name, download_size=size))

    if result:
        return dedupe_selected(result)

    # Fallback for plain, whitespace-aligned tables. SteamPrefill output normally
    # ends each app row with a compressed download size.
    for line in lines:
        stripped = line.strip(" |│")
        if not stripped or stripped.startswith(("+", "-", "═", "─", "━")):
            continue
        size_match = None
        for match in SIZE_RE.finditer(stripped):
            size_match = match
        if not size_match:
            continue
        before = stripped[: size_match.start()].strip(" -:│|")
        if not before or before.casefold().startswith(("total", "download size", "selected")):
            continue
        app_id: int | None = None
        id_match = APP_ID_RE.match(before)
        if id_match:
            app_id = int(id_match.group("id"))
            before = before[id_match.end():].strip(" -:│|")
        if not before:
            continue
        result.append(
            SelectedApp(app_id=app_id, name=before, download_size=size_match.group("size"))
        )
    return dedupe_selected(result)


def parse_selected_app_ids_config(output: str) -> list[int]:
    """Parse SteamPrefill's Config/selectedAppsToPrefill.json file."""
    text = clean_terminal_output(output).strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        # Last-resort compatibility for logs wrapped around the JSON array.
        match = re.search(r"\[[\s\d,\"]*\]", text)
        if not match:
            return []
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            return []

    if isinstance(payload, dict):
        for key in ("selectedApps", "selectedAppIds", "apps", "appIds"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
    if not isinstance(payload, list):
        return []

    result: list[int] = []
    seen: set[int] = set()
    for value in payload:
        try:
            app_id = int(value)
        except (TypeError, ValueError):
            continue
        if app_id <= 0 or app_id in seen:
            continue
        seen.add(app_id)
        result.append(app_id)
    return result


def parse_successfully_downloaded_app_ids(output: str) -> set[int]:
    """Extract only explicit app-level success records from SteamPrefill state.

    Current SteamPrefill primarily stores depot -> manifest history in
    ``successfullyDownloadedDepots.json``. Some builds/wrappers include app IDs
    and an explicit successful/completed flag. CacheDeck accepts those records,
    but deliberately refuses to guess an app from a bare depot ID.
    """
    text = clean_terminal_output(output).strip()
    if not text:
        return set()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return set()

    app_keys = {"appid", "app_id", "applicationid", "application_id"}
    success_keys = {"success", "successful", "completed", "complete", "downloaded", "uptodate", "up_to_date"}
    success_states = {"success", "successful", "completed", "complete", "downloaded", "up_to_date", "uptodate"}
    result: set[int] = set()

    def walk(value: object) -> None:
        if isinstance(value, list):
            for item in value:
                walk(item)
            return
        if not isinstance(value, dict):
            return
        lowered = {str(key).casefold(): item for key, item in value.items()}
        raw_app_id = next((lowered[key] for key in app_keys if key in lowered), None)
        explicit_success = any(lowered.get(key) is True for key in success_keys)
        state = str(lowered.get("status") or lowered.get("state") or "").casefold().replace(" ", "_")
        if state in success_states:
            explicit_success = True
        if raw_app_id is not None and explicit_success:
            try:
                app_id = int(raw_app_id)
            except (TypeError, ValueError):
                app_id = 0
            if app_id > 0:
                result.add(app_id)
        for child in value.values():
            walk(child)

    walk(payload)
    return result


def dedupe_selected(items: list[SelectedApp]) -> list[SelectedApp]:
    seen: set[str] = set()
    result: list[SelectedApp] = []
    for item in items:
        key = game_key(item.app_id, item.name)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


START_RE = re.compile(
    r"\bStarting(?:\s+(?:download(?:ing)?|prefill(?:ing)?))?\s+(?P<name>.+?)\s*$",
    re.I,
)
FINISHED_DOWNLOAD_RE = re.compile(
    r"Finished\s+downloading\s+(?P<size>\d+(?:\.\d+)?\s*(?:[KMGTPE]i?B|bytes?))",
    re.I,
)


PROGRESS_RE = re.compile(
    r"(?P<percent>\d{1,3})%"
    r"(?:\s+(?P<eta>\d{1,3}:\d{2}:\d{2}))?"
    r"(?:\s+(?P<downloaded>\d+(?:\.\d+)?)\s*/\s*(?P<total>\d+(?:\.\d+)?)\s*(?P<unit>[KMGTPE]i?B))?"
    r"(?:\s+(?P<speed>\d+(?:\.\d+)?\s*(?:[KMGTPE]?(?:bit|B)/s)))?",
    re.I,
)


def parse_progress_snapshot(output: str, initial_app_name: str | None = None) -> ProgressSnapshot:
    """Parse SteamPrefill output, optionally continuing the previous active app.

    SteamPrefill redraws progress and sometimes rotates/truncates Docker log tails.
    Carrying the active app across incremental reads lets a later ``Finished
    downloading`` line still be attributed correctly even when its ``Starting``
    line is no longer in the current chunk.
    """
    text = clean_terminal_output(output)
    current: str | None = initial_app_name
    progress: float | None = None
    downloaded: str | None = None
    total: str | None = None
    speed: str | None = None
    eta: str | None = None
    updates: list[str] = []
    up_to_date: list[str] = []
    completed: list[str] = []
    failed: list[str] = []
    downloaded_for: dict[str, str] = {}
    total_for: dict[str, str] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        start_match = START_RE.search(line)
        if start_match:
            candidate = start_match.group("name").strip(" :-")
            ignored_starts = (
                "login", "steam session", "container", "cachedeck", "lancache",
                "manifest", "metadata", "selected app", "application",
            )
            if candidate and not candidate.casefold().startswith(ignored_starts):
                current = candidate
                progress = None
                downloaded = total = speed = eta = None
        progress_match = PROGRESS_RE.search(line)
        if progress_match and ("download" in line.casefold() or current):
            progress = float(progress_match.group("percent"))
            eta = progress_match.group("eta") or eta
            unit = progress_match.group("unit")
            if progress_match.group("downloaded") and unit:
                downloaded = f"{progress_match.group('downloaded')} {unit}"
            if progress_match.group("total") and unit:
                total = f"{progress_match.group('total')} {unit}"
            speed = progress_match.group("speed") or speed
            if current and downloaded:
                downloaded_for[current] = downloaded
            if current and total:
                total_for[current] = total
        finished_match = FINISHED_DOWNLOAD_RE.search(line)
        if finished_match and current:
            finished_size = finished_match.group("size")
            progress = 100.0
            downloaded = finished_size
            total = total or finished_size
            downloaded_for[current] = finished_size
            total_for[current] = total
            completed.append(current)
        lowered = line.casefold()
        for pattern, bucket in (
            (r"(?P<name>.+?)\s+(?:has an update|update available)", updates),
            (r"(?P<name>.+?)\s+(?:is already up to date|already up to date)", up_to_date),
            (r"(?P<name>.+?)\s+(?:completed successfully)", completed),
            (r"(?P<name>.+?)\s+(?:failed|download failed)", failed),
        ):
            match = re.search(pattern, line, re.I)
            if match:
                name = match.group("name").strip(" :-[]")
                if name:
                    bucket.append(name)
        if current and any(
            phrase in lowered
            for phrase in ("download complete", "successfully prefilled", "completed download")
        ):
            completed.append(current)
            if downloaded:
                downloaded_for[current] = downloaded
            if total:
                total_for[current] = total

    return ProgressSnapshot(
        app_name=current,
        progress=progress,
        downloaded=downloaded,
        total=total,
        speed=speed,
        eta=eta,
        update_available_for=updates,
        up_to_date_for=up_to_date,
        completed_for=completed,
        failed_for=failed,
        downloaded_for=downloaded_for,
        total_for=total_for,
    )


SUCCESSFUL_PREFILL_RE = re.compile(
    r"\bPrefilled\s+\d+\s+apps?\s+in\b|\bUpdated\s*[│|]\s*Up To Date\b",
    re.I,
)
FATAL_PREFILL_RE = re.compile(
    r"(?:^|\n)(?:.*\b(?:Unhandled exception|fatal error|prefill failed)\b.*)(?:$|\n)",
    re.I,
)


def output_indicates_successful_prefill(output: str) -> bool:
    """Return true only when the current run contains SteamPrefill's success summary."""
    text = clean_terminal_output(output)
    if not SUCCESSFUL_PREFILL_RE.search(text):
        return False
    return not FATAL_PREFILL_RE.search(text)


def resolve_steam_metadata_by_id(
    app_id: int,
    timeout: int = 8,
) -> tuple[str, str | None, str | None] | None:
    params = urlencode({"appids": str(app_id), "l": "english", "cc": "GB"})
    request = Request(
        f"https://store.steampowered.com/api/appdetails/?{params}",
        headers={"User-Agent": "CacheDeck (+https://github.com/DarmachD/CacheDeck)"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    entry = payload.get(str(app_id)) if isinstance(payload, dict) else None
    if not isinstance(entry, dict) or not entry.get("success"):
        return None
    data = entry.get("data")
    if not isinstance(data, dict):
        return None
    name = str(data.get("name") or "").strip()
    if not name:
        return None
    image_url = str(data.get("header_image") or "").strip() or steam_artwork_url(app_id)
    return name, image_url, steam_store_url(app_id)


def resolve_steam_metadata(name: str, timeout: int = 8) -> tuple[int, str | None, str | None] | None:
    params = urlencode({"term": name, "l": "english", "cc": "GB"})
    request = Request(
        f"https://store.steampowered.com/api/storesearch/?{params}",
        headers={"User-Agent": "CacheDeck (+https://github.com/DarmachD/CacheDeck)"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return None
    target = normalise_name(name)
    exact = next(
        (item for item in items if normalise_name(str(item.get("name") or "")) == target),
        None,
    )
    chosen = exact or items[0]
    try:
        app_id = int(chosen["id"])
    except (KeyError, TypeError, ValueError):
        return None
    image_url = str(chosen.get("tiny_image") or "").strip() or steam_artwork_url(app_id)
    return app_id, image_url, steam_store_url(app_id)


def build_library_response(
    store: LibraryStore,
    queue_store: QueueStore,
    *,
    message: str = "",
) -> LibraryResponse:
    games = store.list_games()
    active_queue = queue_store.active()
    queue_positions = {
        item.app_id: index + 1
        for index, item in enumerate(item for item in active_queue if item.state == "queued")
    }
    running_ids = {item.app_id for item in active_queue if item.state == "running"}
    rendered: list[GameRecord] = []
    for game in games:
        changes: dict[str, object] = {}
        if game.app_id in running_ids and game.status != "downloading":
            changes.update(status="checking", message="Checking Steam and applying an update if needed.")
        elif game.app_id in queue_positions:
            changes.update(
                status="queued",
                queue_position=queue_positions[game.app_id],
                message=f"Queued at position {queue_positions[game.app_id]}.",
            )
        elif game.status in {"queued", "checking"}:
            changes.update(status="selected", queue_position=None, message="Selected for prefill.")
        if changes:
            game = game.model_copy(update=changes)
        rendered.append(game)

    order = {
        "downloading": 0,
        "checking": 1,
        "queued": 2,
        "update_available": 3,
        "failed": 4,
        "selected": 5,
        "downloaded": 6,
    }
    rendered.sort(key=lambda item: (order.get(item.status, 99), item.name.casefold()))
    game_by_app_id = {game.app_id: game for game in rendered if game.app_id is not None}
    queue_remaining_bytes = 0
    for item in active_queue:
        game = game_by_app_id.get(item.app_id)
        if game is None:
            continue
        estimated = parse_size_bytes(game.download_size)
        if item.state == "running":
            estimated = max(0, estimated - parse_size_bytes(game.downloaded))
        queue_remaining_bytes += estimated

    summary = LibrarySummary(
        total=len(rendered),
        downloaded=sum(game.status == "downloaded" for game in rendered),
        queued=sum(game.status == "queued" for game in rendered),
        downloading=sum(game.status in {"downloading", "checking"} for game in rendered),
        update_available=sum(game.status == "update_available" for game in rendered),
        failed=sum(game.status == "failed" for game in rendered),
        unresolved=sum(
            game.app_id is None or is_placeholder_game_name(game.name, game.app_id)
            for game in rendered
        ),
        known_size_count=sum(parse_size_bytes(game.download_size) > 0 for game in rendered),
        total_size_bytes=sum(parse_size_bytes(game.download_size) for game in rendered),
        queue_remaining_bytes=queue_remaining_bytes,
        latest_run_downloaded_bytes=sum(
            parse_size_bytes(game.last_downloaded)
            for game in rendered
            if game.last_downloaded_job_id == store.last_activity_job_id()
        ),
    )
    return LibraryResponse(
        generated_at=utc_now(),
        last_refreshed_at=store.last_refreshed_at(),
        metadata_refreshing=store.metadata_refreshing,
        message=message,
        summary=summary,
        games=rendered,
        queue=queue_store.list(),
    )
