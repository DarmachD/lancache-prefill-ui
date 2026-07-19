from __future__ import annotations

from pathlib import Path

from app.database import StateDatabase
from app.library import (
    GameQueueItem,
    GameRecord,
    LibraryStore,
    ProgressSnapshot,
    QueueStore,
    game_key,
    normalise_name,
    steam_artwork_url,
    steam_store_url,
    utc_now,
)


class SQLiteLibraryStore(LibraryStore):
    """LibraryStore-compatible adapter backed by CacheDeck's SQLite database."""

    def __init__(self, database: StateDatabase) -> None:
        super().__init__(Path("cachedeck.db"))
        self.database = database

    def _read_unlocked(self) -> dict[str, object]:
        return {
            "last_refreshed_at": self.database.get_meta("library.last_refreshed_at"),
            "last_completed_job_id": self.database.get_meta("library.last_completed_job_id"),
            "last_activity_job_id": self.database.get_meta("library.last_activity_job_id"),
            "games": self.database.list_game_payloads(),
        }

    def _write_unlocked(self, raw: dict[str, object]) -> None:
        games = raw.get("games") if isinstance(raw.get("games"), list) else []
        self.database.replace_game_payloads(
            [item for item in games if isinstance(item, dict)]
        )
        for source, target in (
            ("last_refreshed_at", "library.last_refreshed_at"),
            ("last_completed_job_id", "library.last_completed_job_id"),
            ("last_activity_job_id", "library.last_activity_job_id"),
        ):
            value = raw.get(source)
            self.database.set_meta(target, str(value) if value else None)

    def update_game(self, key: str, **changes: object):
        with self._lock:
            game = next((item for item in self.list_games() if item.key == key), None)
            if game is None:
                return None
            updated = game.model_copy(update=changes)
            self.database.upsert_game_payload(updated.model_dump(mode="json"))
            if changes.get("last_downloaded_job_id"):
                self.database.set_meta("library.last_activity_job_id", str(changes["last_downloaded_job_id"]))
            return updated

    def update_by_app_id(self, app_id: int, **changes: object):
        game = next((item for item in self.list_games() if item.app_id == app_id), None)
        return self.update_game(game.key, **changes) if game else None

    def update_by_name(self, name: str, **changes: object):
        target = normalise_name(name)
        game = next((item for item in self.list_games() if normalise_name(item.name) == target), None)
        return self.update_game(game.key, **changes) if game else None

    def save_metadata(
        self,
        key: str,
        app_id: int,
        image_url: str | None,
        store_url: str | None,
        name: str | None = None,
    ) -> None:
        game = next((item for item in self.list_games() if item.key == key), None)
        if game is None:
            return
        resolved_name = name or game.name
        resolved_key = game_key(app_id, resolved_name)
        if resolved_key != game.key:
            # Identity resolution needs the database's atomic old-key cleanup.
            super().save_metadata(key, app_id, image_url, store_url, name=resolved_name)
            return
        updated = game.model_copy(update={
            "app_id": app_id,
            "name": resolved_name,
            "image_url": image_url or steam_artwork_url(app_id),
            "store_url": store_url or steam_store_url(app_id),
            "metadata_attempts": 0,
            "metadata_retry_at": None,
            "metadata_error": None,
            "metadata_resolved_at": utc_now(),
        })
        self.database.upsert_game_payload(updated.model_dump(mode="json"))

    def mark_provider_verified(self, app_ids: set[int], when: str) -> int:
        changed = 0
        for game in self.list_games():
            if game.app_id not in app_ids or game.status in {"downloading", "checking", "queued"}:
                continue
            if game.status == "downloaded" and game.progress == 100.0:
                continue
            self.update_game(
                game.key,
                status="downloaded",
                progress=100.0,
                update_available=False,
                last_checked_at=game.last_checked_at or when,
                message="SteamPrefill reports this app as previously downloaded.",
            )
            changed += 1
        return changed

    def apply_progress(
        self,
        snapshot: ProgressSnapshot,
        *,
        full_run: bool = False,
        job_id: str | None = None,
    ) -> None:
        current = normalise_name(snapshot.app_name or "")
        updates = {normalise_name(value) for value in snapshot.update_available_for}
        up_to_date = {normalise_name(value) for value in snapshot.up_to_date_for}
        completed = {normalise_name(value) for value in snapshot.completed_for}
        failed = {normalise_name(value) for value in snapshot.failed_for}
        downloaded_for = {normalise_name(name): value for name, value in snapshot.downloaded_for.items()}
        total_for = {normalise_name(name): value for name, value in snapshot.total_for.items()}
        now = utc_now()
        touched_activity = False

        for game in self.list_games():
            name_key = normalise_name(game.name)
            changes: dict[str, object] = {}
            if name_key in updates:
                changes.update(status="update_available", update_available=True,
                               last_checked_at=now, message="An update was detected.")
            if name_key in up_to_date:
                changes.update(status="downloaded", progress=100.0, downloaded=None,
                               total=None, last_downloaded="0 B",
                               last_downloaded_job_id=job_id or game.last_downloaded_job_id,
                               update_available=False, last_checked_at=now, speed=None,
                               eta=None, queue_position=None,
                               message="Already up to date at the last check.")
                touched_activity = True
            if name_key in completed:
                completed_download = downloaded_for.get(name_key) or total_for.get(name_key) or game.downloaded
                changes.update(status="downloaded", progress=100.0,
                               downloaded=total_for.get(name_key) or game.total or game.downloaded,
                               total=total_for.get(name_key) or game.total,
                               last_downloaded=completed_download or game.last_downloaded,
                               last_downloaded_job_id=job_id or game.last_downloaded_job_id,
                               update_available=False, last_checked_at=now,
                               last_prefilled_at=now, speed=None, eta=None,
                               queue_position=None, message="Downloaded and up to date.")
                touched_activity = True
            if name_key in failed:
                changes.update(status="failed", speed=None, eta=None,
                               message="The last prefill attempt failed.")
            finished = name_key in completed or name_key in up_to_date
            if current and name_key == current and not finished:
                changes.update(
                    status="downloading",
                    progress=(max(0.0, min(100.0, snapshot.progress))
                              if snapshot.progress is not None else None),
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
                self.update_game(game.key, **changes)

        if job_id and touched_activity:
            self.database.set_meta("library.last_activity_job_id", job_id)


class SQLiteQueueStore(QueueStore):
    """QueueStore-compatible adapter backed by CacheDeck's SQLite database."""

    def __init__(self, database: StateDatabase) -> None:
        super().__init__(Path("cachedeck.db"))
        self.database = database

    def _read_unlocked(self):
        result = []
        for item in self.database.list_queue_payloads():
            try:
                result.append(GameQueueItem.model_validate(item))
            except Exception:
                continue
        return result

    def _write_unlocked(self, items) -> None:
        self.database.replace_queue_payloads(
            [item.model_dump(mode="json") for item in items[-100:]]
        )
