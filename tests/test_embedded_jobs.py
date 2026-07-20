import asyncio
import os
import tempfile
import unittest
from pathlib import Path

import app.main as main
from app.database import StateDatabase
from app.library import GameQueueItem, SelectedApp
from app.providers import create_provider
from app.state import SQLiteLibraryStore, SQLiteQueueStore


class EmbeddedQuickJobIntegrationTests(unittest.TestCase):
    def test_quick_targeted_check_completes_and_verifies_game(self):
        old_values = {
            "PREFILL_DIR": main.PREFILL_DIR,
            "PREFILL_STATE_DIR": main.PREFILL_STATE_DIR,
            "provider": main.provider,
            "state_database": main.state_database,
            "library_store": main.library_store,
            "queue_store": main.queue_store,
            "history_store": main.history_store,
        }
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                engine_dir = root / "engine"
                state_dir = engine_dir / "state"
                engine_dir.mkdir()
                binary = engine_dir / "SteamPrefill"
                binary.write_text(
                    """#!/usr/bin/env bash
set -e
if [[ "${1:-}" == "prefill" ]]; then
  echo '[8:02:56 AM] Starting Biped'
  echo '[8:03:00 AM] Finished downloading 1.48 GiB in 01.7426 - 7.27 Gbit/s'
  echo '[8:03:00 AM] Prefill complete!'
  echo 'Prefilled 1 apps totaling 1.48 GiB in 44.8227'
  echo 'Updated | Up To Date'
  exit 0
fi
exit 0
""",
                    encoding="utf-8",
                )
                binary.chmod(0o755)

                database = StateDatabase(root / "cachedeck.db")
                main.PREFILL_DIR = str(engine_dir)
                main.PREFILL_STATE_DIR = str(state_dir)
                main.provider = create_provider(
                    "embedded-steam",
                    working_directory=str(engine_dir),
                    container_user="",
                    command=f"{binary} prefill",
                    embedded_binary=str(binary),
                )
                main.state_database = database
                main.library_store = SQLiteLibraryStore(database)
                main.queue_store = SQLiteQueueStore(database)
                main.history_store = main.HistoryStore(root / "history.json", 20, database)
                main.library_store.replace_selected(
                    [SelectedApp(app_id=1071870, name="Biped", download_size="1.48 GiB")],
                    "2026-07-20T08:02:50+00:00",
                )
                item = main.queue_store.enqueue(
                    GameQueueItem(
                        queue_id="queue-biped",
                        provider="embedded-steam",
                        app_id=1071870,
                        app_name="Biped",
                        requested_at="2026-07-20T08:03:12+00:00",
                        state="running",
                        started_at="2026-07-20T08:03:12+00:00",
                    )
                )

                result = asyncio.run(
                    main.launch_prefill_job(app_id=1071870, app_name="Biped")
                )
                self.assertTrue(result.ok)
                self.assertEqual(result.status.state, "completed")
                self.assertIsNotNone(result.status.job_id)
                # Reproduce the v0.8.0 queue state produced by the old
                # short-process race. The successful wrapper state should repair
                # this automatically after upgrade.
                main.queue_store.update(
                    item.queue_id,
                    state="failed",
                    finished_at="2026-07-20T08:03:12+00:00",
                    job_id=None,
                    message="500: SteamPrefill did not confirm that the detached job started.",
                )

                asyncio.run(main.sync_library_activity())
                game = main.library_store.list_games()[0]
                self.assertEqual(game.status, "downloaded")
                self.assertEqual(game.progress, 100.0)
                self.assertEqual(game.verification_source, "observed_download")
                self.assertEqual(game.last_downloaded, "1.48 GiB")
                queue_item = main.queue_store.list()[0]
                self.assertEqual(queue_item.state, "completed")
        finally:
            for name, value in old_values.items():
                setattr(main, name, value)

    def test_transient_steam_metadata_failure_retries_then_completes(self):
        old_values = {
            "PREFILL_DIR": main.PREFILL_DIR,
            "PREFILL_STATE_DIR": main.PREFILL_STATE_DIR,
            "STEAM_METADATA_MAX_ATTEMPTS": main.STEAM_METADATA_MAX_ATTEMPTS,
            "STEAM_METADATA_RETRY_BASE_SECONDS": main.STEAM_METADATA_RETRY_BASE_SECONDS,
            "provider": main.provider,
            "state_database": main.state_database,
            "library_store": main.library_store,
            "queue_store": main.queue_store,
            "history_store": main.history_store,
        }
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                engine_dir = root / "engine"
                state_dir = engine_dir / "state"
                engine_dir.mkdir()
                attempts_file = engine_dir / "attempts"
                binary = engine_dir / "SteamPrefill"
                binary.write_text(
                    f"""#!/usr/bin/env bash
set -e
count=0
if [ -r {attempts_file!s} ]; then count=$(cat {attempts_file!s}); fi
count=$((count + 1))
printf '%s' "$count" > {attempts_file!s}
if [ "$count" -lt 3 ]; then
  echo 'Unable to load latest App metadata! An unexpected error occurred!'
  echo 'This could possibly be due to transient errors with the Steam network.'
  echo 'System.Threading.Tasks.TaskCanceledException: A task was canceled.'
  echo 'at SteamPrefill.Handlers.AppInfoHandler.AppInfoRequestAsync(List<uint> appIdsToLoad)'
  exit 1
fi
echo '[8:02:56 AM] Starting Biped'
echo '[8:03:00 AM] Finished downloading 1.48 GiB in 01.7426 - 7.27 Gbit/s'
echo '[8:03:00 AM] Prefill complete!'
echo 'Prefilled 1 apps totaling 1.48 GiB in 44.8227'
echo 'Updated | Up To Date'
exit 0
""",
                    encoding="utf-8",
                )
                binary.chmod(0o755)

                database = StateDatabase(root / "cachedeck.db")
                main.PREFILL_DIR = str(engine_dir)
                main.PREFILL_STATE_DIR = str(state_dir)
                main.STEAM_METADATA_MAX_ATTEMPTS = 3
                main.STEAM_METADATA_RETRY_BASE_SECONDS = 0
                main.provider = create_provider(
                    "embedded-steam",
                    working_directory=str(engine_dir),
                    container_user="",
                    command=f"{binary} prefill",
                    embedded_binary=str(binary),
                )
                main.state_database = database
                main.library_store = SQLiteLibraryStore(database)
                main.queue_store = SQLiteQueueStore(database)
                main.history_store = main.HistoryStore(root / "history.json", 20, database)
                main.library_store.replace_selected(
                    [SelectedApp(app_id=1071870, name="Biped", download_size="1.48 GiB")],
                    "2026-07-20T08:02:50+00:00",
                )
                item = main.queue_store.enqueue(
                    GameQueueItem(
                        queue_id="queue-biped-retry",
                        provider="embedded-steam",
                        app_id=1071870,
                        app_name="Biped",
                        requested_at="2026-07-20T08:03:12+00:00",
                        state="running",
                        started_at="2026-07-20T08:03:12+00:00",
                    )
                )

                result = asyncio.run(
                    main.launch_prefill_job(app_id=1071870, app_name="Biped")
                )
                main.queue_store.update(item.queue_id, job_id=result.status.job_id)
                for _ in range(30):
                    status = asyncio.run(main.get_prefill_status())
                    if not status.running:
                        break
                    asyncio.run(asyncio.sleep(0.05))
                asyncio.run(main.sync_library_activity())

                self.assertEqual(attempts_file.read_text(encoding="utf-8"), "3")
                game = main.library_store.list_games()[0]
                self.assertEqual(game.status, "downloaded")
                self.assertEqual(game.verification_source, "observed_download")
                self.assertEqual(main.queue_store.list()[0].state, "completed")
        finally:
            for name, value in old_values.items():
                setattr(main, name, value)

    def test_exhausted_transient_failure_preserves_verified_game_state(self):
        old_values = {
            "PREFILL_DIR": main.PREFILL_DIR,
            "PREFILL_STATE_DIR": main.PREFILL_STATE_DIR,
            "STEAM_METADATA_MAX_ATTEMPTS": main.STEAM_METADATA_MAX_ATTEMPTS,
            "STEAM_METADATA_RETRY_BASE_SECONDS": main.STEAM_METADATA_RETRY_BASE_SECONDS,
            "provider": main.provider,
            "state_database": main.state_database,
            "library_store": main.library_store,
            "queue_store": main.queue_store,
            "history_store": main.history_store,
        }
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                engine_dir = root / "engine"
                state_dir = engine_dir / "state"
                engine_dir.mkdir()
                binary = engine_dir / "SteamPrefill"
                binary.write_text(
                    """#!/usr/bin/env bash
echo 'Unable to load latest App metadata! An unexpected error occurred!'
echo 'This could possibly be due to transient errors with the Steam network.'
echo 'System.Threading.Tasks.TaskCanceledException: A task was canceled.'
echo 'at SteamPrefill.Handlers.AppInfoHandler.AppInfoRequestAsync(List<uint> appIdsToLoad)'
exit 1
""",
                    encoding="utf-8",
                )
                binary.chmod(0o755)

                database = StateDatabase(root / "cachedeck.db")
                main.PREFILL_DIR = str(engine_dir)
                main.PREFILL_STATE_DIR = str(state_dir)
                main.STEAM_METADATA_MAX_ATTEMPTS = 3
                main.STEAM_METADATA_RETRY_BASE_SECONDS = 0
                main.provider = create_provider(
                    "embedded-steam",
                    working_directory=str(engine_dir),
                    container_user="",
                    command=f"{binary} prefill",
                    embedded_binary=str(binary),
                )
                main.state_database = database
                main.library_store = SQLiteLibraryStore(database)
                main.queue_store = SQLiteQueueStore(database)
                main.history_store = main.HistoryStore(root / "history.json", 20, database)
                main.library_store.replace_selected(
                    [SelectedApp(app_id=1071870, name="Biped", download_size="1.48 GiB")],
                    "2026-07-20T08:02:50+00:00",
                )
                main.library_store.update_by_app_id(
                    1071870,
                    status="downloaded",
                    progress=100.0,
                    verification_source="manual",
                    verified_at="2026-07-20T08:00:00+00:00",
                )
                item = main.queue_store.enqueue(
                    GameQueueItem(
                        queue_id="queue-biped-fail",
                        provider="embedded-steam",
                        app_id=1071870,
                        app_name="Biped",
                        requested_at="2026-07-20T08:03:12+00:00",
                        state="running",
                        started_at="2026-07-20T08:03:12+00:00",
                    )
                )

                result = asyncio.run(
                    main.launch_prefill_job(app_id=1071870, app_name="Biped")
                )
                main.queue_store.update(item.queue_id, job_id=result.status.job_id)
                for _ in range(30):
                    status = asyncio.run(main.get_prefill_status())
                    if not status.running:
                        break
                    asyncio.run(asyncio.sleep(0.05))
                asyncio.run(main.sync_library_activity())

                game = main.library_store.list_games()[0]
                self.assertEqual(game.status, "downloaded")
                self.assertEqual(game.verification_source, "manual")
                self.assertIn("timed out", game.message.casefold())
                queue_item = main.queue_store.list()[0]
                self.assertEqual(queue_item.state, "failed")
                self.assertIn("automatic retries", queue_item.message)
        finally:
            for name, value in old_values.items():
                setattr(main, name, value)


if __name__ == "__main__":
    unittest.main()
