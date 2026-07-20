import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app.main import cron_matches, managed_prefill_command, validate_schedule_values


class MainHelperTests(unittest.TestCase):
    def test_managed_prefill_adds_machine_readable_flags(self):
        command = managed_prefill_command(730)
        self.assertIn("prefill 730", command)
        self.assertIn("--verbose", command)
        self.assertIn("--no-ansi", command)

    def test_managed_prefill_does_not_duplicate_flags(self):
        command = managed_prefill_command()
        self.assertEqual(command.split().count("--verbose"), 1)
        self.assertEqual(command.split().count("--no-ansi"), 1)


class ScheduleHelperTests(unittest.TestCase):
    def test_cron_matching_uses_schedule_timezone_datetime(self):
        candidate = datetime(2026, 7, 20, 2, 0, tzinfo=ZoneInfo("Europe/London"))
        self.assertTrue(cron_matches("0 2 * * *", candidate))
        self.assertFalse(cron_matches("30 2 * * *", candidate))

    def test_schedule_validation_normalises_and_rejects_bad_values(self):
        expression, timezone_name = validate_schedule_values(" 0   2 * * * ", "Europe/London")
        self.assertEqual(expression, "0 2 * * *")
        self.assertEqual(timezone_name, "Europe/London")
        with self.assertRaises(HTTPException):
            validate_schedule_values("not cron", "Europe/London")
        with self.assertRaises(HTTPException):
            validate_schedule_values("0 2 * * *", "Not/AZone")


class QueueHelperTests(unittest.TestCase):
    def test_bulk_queue_deduplicates_active_games(self):
        import tempfile
        from pathlib import Path

        import app.main as main
        from app.library import LibraryStore, QueueStore, SelectedApp

        with tempfile.TemporaryDirectory() as directory:
            old_library = main.library_store
            old_queue = main.queue_store
            try:
                main.library_store = LibraryStore(Path(directory) / "library.json")
                main.queue_store = QueueStore(Path(directory) / "queue.json")
                main.library_store.replace_selected(
                    [
                        SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB"),
                        SelectedApp(app_id=570, name="Dota 2", download_size="20.0 GiB"),
                    ],
                    "2026-07-18T00:00:00+00:00",
                )
                count, names = main.enqueue_library_games([730, 570, 730])
                self.assertEqual(count, 2)
                self.assertEqual(set(names), {"Counter-Strike 2", "Dota 2"})
                second_count, _ = main.enqueue_library_games([730, 570])
                self.assertEqual(second_count, 0)
                self.assertEqual(len(main.queue_store.active()), 2)
            finally:
                main.library_store = old_library
                main.queue_store = old_queue


class WebSocketOriginTests(unittest.TestCase):
    def test_allows_same_host_and_rejects_cross_site_origin(self):
        from types import SimpleNamespace

        from app.main import websocket_origin_allowed

        same_host = SimpleNamespace(
            headers={"origin": "http://cachedeck.local:8088", "host": "cachedeck.local:8088"}
        )
        cross_site = SimpleNamespace(
            headers={"origin": "https://evil.example", "host": "cachedeck.local:8088"}
        )
        self.assertTrue(websocket_origin_allowed(same_host))
        self.assertFalse(websocket_origin_allowed(cross_site))


class DetachedJobHandshakeTests(unittest.TestCase):
    def test_a_fast_successful_job_is_confirmed_even_after_it_exits(self):
        import os
        import subprocess
        import tempfile
        import time
        from pathlib import Path

        import app.main as main
        from app.providers import create_provider

        old_dir = main.PREFILL_DIR
        old_state_dir = main.PREFILL_STATE_DIR
        old_provider = main.provider
        try:
            with tempfile.TemporaryDirectory() as directory:
                state_dir = os.path.join(directory, "state")
                main.PREFILL_DIR = directory
                main.PREFILL_STATE_DIR = state_dir
                main.provider = create_provider(
                    "embedded-steam",
                    working_directory=directory,
                    container_user="",
                    command="/bin/true prefill",
                    embedded_binary="/bin/true",
                )
                command = main.build_start_command(
                    "quick-success",
                    "2026-07-20T08:03:12+00:00",
                    "true",
                )
                result = subprocess.run(
                    ["bash", "-lc", command],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.split("\0", 1)[0], "STARTED")

                exit_file = os.path.join(state_dir, "prefill.exit")
                for _ in range(100):
                    if os.path.exists(exit_file):
                        break
                    time.sleep(0.01)
                self.assertTrue(os.path.exists(exit_file))
                self.assertEqual(Path(exit_file).read_text(encoding="utf-8").strip(), "0")
                self.assertFalse(os.path.exists(os.path.join(state_dir, "prefill.pid")))
        finally:
            main.PREFILL_DIR = old_dir
            main.PREFILL_STATE_DIR = old_state_dir
            main.provider = old_provider


class InteractiveTerminalActivityTests(unittest.TestCase):
    def test_successful_selector_download_updates_the_game_once(self):
        import tempfile
        from pathlib import Path

        import app.main as main
        from app.database import StateDatabase
        from app.library import LibraryStore, SelectedApp

        old_library = main.library_store
        old_database = main.state_database
        try:
            with tempfile.TemporaryDirectory() as directory:
                main.library_store = LibraryStore(Path(directory) / "library.json")
                main.state_database = StateDatabase(Path(directory) / "cachedeck.db")
                main.library_store.replace_selected(
                    [SelectedApp(app_id=1071870, name="Biped", download_size="1.48 GiB")],
                    "2026-07-20T08:02:50+00:00",
                )
                tracker = main.TerminalActivityTracker("terminal-test")
                output = """
[8:02:55 AM] Selected 1 apps to prefill!
[8:02:56 AM] Starting Biped
[8:03:00 AM] Finished downloading 1.48 GiB in 01.7426 - 7.27 Gbit/s
[8:03:00 AM] Prefill complete!
"""
                self.assertTrue(tracker.observe(output))
                game = main.library_store.list_games()[0]
                self.assertEqual(game.status, "downloaded")
                self.assertEqual(game.progress, 100.0)
                self.assertEqual(game.last_downloaded, "1.48 GiB")
                self.assertEqual(game.verification_source, "observed_download")

                self.assertFalse(tracker.observe(output))
                events = main.state_database.list_events(20, event_type="provider.game_completed")
                self.assertEqual(len(events), 1)
        finally:
            main.library_store = old_library
            main.state_database = old_database


class SelectedStatusMergeTests(unittest.TestCase):
    def test_status_rows_without_ids_keep_config_ids_and_provider(self):
        import tempfile
        from pathlib import Path

        import app.main as main
        from app.library import LibraryStore, SelectedApp

        old_library = main.library_store
        try:
            with tempfile.TemporaryDirectory() as directory:
                main.library_store = LibraryStore(Path(directory) / "library.json")
                main.library_store.replace_selected(
                    [SelectedApp(app_id=1071870, name="Biped")],
                    "2026-07-20T08:00:00+00:00",
                )
                merged = main.merge_status_apps_with_config_ids(
                    [SelectedApp(app_id=None, name="Biped", download_size="1.48 GiB")],
                    [1071870],
                )
                self.assertEqual(merged[0].app_id, 1071870)
                self.assertEqual(merged[0].provider, main.provider.provider_id)
        finally:
            main.library_store = old_library


if __name__ == "__main__":
    unittest.main()
