import tempfile
import unittest
from pathlib import Path

from app.library import (
    GameQueueItem,
    LibraryStore,
    QueueStore,
    SelectedApp,
    output_indicates_successful_prefill,
    parse_progress_snapshot,
    parse_selected_apps_status,
)


class LibraryParserTests(unittest.TestCase):
    def test_parses_box_table(self):
        output = """
│ App ID  │ App                                    │ Download Size │
│ 1466860 │ Age of Empires IV: Anniversary Edition │ 55.0 GiB      │
│ 730     │ Counter-Strike 2                       │ 34.4 GiB      │
"""
        apps = parse_selected_apps_status(output)
        self.assertEqual([app.app_id for app in apps], [1466860, 730])
        self.assertEqual(apps[0].name, "Age of Empires IV: Anniversary Edition")
        self.assertEqual(apps[0].download_size, "55.0 GiB")

    def test_parses_plain_status_rows(self):
        output = """
Age of Empires IV: Anniversary Edition    55.0 GiB
Counter-Strike 2                           34.4 GiB
"""
        apps = parse_selected_apps_status(output)
        self.assertEqual(len(apps), 2)
        self.assertEqual(apps[1].name, "Counter-Strike 2")

    def test_recognises_successful_prefill_summary(self):
        output = """
Prefilled 288 apps in 01:23:45
Updated | Up To Date
      12 |        276
"""
        self.assertTrue(output_indicates_successful_prefill(output))
        self.assertFalse(output_indicates_successful_prefill("Steam login failed"))

    def test_parses_live_progress(self):
        output = """
[1:13:11 AM] Starting Age of Empires IV: Anniversary Edition
Downloading.. 35% 10:46:33 19.7 / 55.0 GiB 7.8 Mbit/s
"""
        progress = parse_progress_snapshot(output)
        self.assertEqual(progress.app_name, "Age of Empires IV: Anniversary Edition")
        self.assertEqual(progress.progress, 35.0)
        self.assertEqual(progress.downloaded, "19.7 GiB")
        self.assertEqual(progress.total, "55.0 GiB")
        self.assertEqual(progress.speed, "7.8 Mbit/s")


class StoreTests(unittest.TestCase):
    def test_library_state_survives_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            store = LibraryStore(Path(directory) / "library.json")
            store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB")],
                "2026-07-18T00:00:00+00:00",
            )
            store.update_by_app_id(730, status="downloaded", progress=100.0)
            games = store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="35.0 GiB")],
                "2026-07-18T01:00:00+00:00",
            )
            self.assertEqual(games[0].status, "downloaded")
            self.assertEqual(games[0].download_size, "35.0 GiB")

    def test_known_app_id_gets_artwork_without_lookup(self):
        with tempfile.TemporaryDirectory() as directory:
            store = LibraryStore(Path(directory) / "library.json")
            games = store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB")],
                "2026-07-18T00:00:00+00:00",
            )
            self.assertIn("/730/header.jpg", games[0].image_url or "")
            self.assertEqual(games[0].store_url, "https://store.steampowered.com/app/730/")

    def test_queue_deduplicates_active_game(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = QueueStore(Path(directory) / "queue.json")
            first = queue.enqueue(
                GameQueueItem(
                    queue_id="a",
                    app_id=730,
                    app_name="Counter-Strike 2",
                    requested_at="2026-07-18T00:00:00+00:00",
                )
            )
            second = queue.enqueue(
                GameQueueItem(
                    queue_id="b",
                    app_id=730,
                    app_name="Counter-Strike 2",
                    requested_at="2026-07-18T00:01:00+00:00",
                )
            )
            self.assertEqual(first.queue_id, second.queue_id)
            self.assertEqual(len(queue.list()), 1)


class LibraryQolTests(unittest.TestCase):
    def test_completion_records_latest_download_size(self):
        from app.library import build_library_response

        with tempfile.TemporaryDirectory() as directory:
            store = LibraryStore(Path(directory) / "library.json")
            queue = QueueStore(Path(directory) / "queue.json")
            store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB")],
                "2026-07-18T00:00:00+00:00",
            )
            snapshot = parse_progress_snapshot(
                """
[1:13:11 AM] Starting Counter-Strike 2
Downloading.. 100% 00:00:00 4.2 / 34.4 GiB 800 Mbit/s
Download complete
"""
            )
            store.apply_progress(snapshot, job_id="job-1")
            game = store.list_games()[0]
            self.assertEqual(game.status, "downloaded")
            self.assertEqual(game.last_downloaded, "4.2 GiB")
            response = build_library_response(store, queue)
            self.assertGreater(response.summary.latest_run_downloaded_bytes, 4 * 1024**3)


    def test_up_to_date_records_zero_download_without_changing_last_download_time(self):
        from app.library import build_library_response

        with tempfile.TemporaryDirectory() as directory:
            store = LibraryStore(Path(directory) / "library.json")
            queue = QueueStore(Path(directory) / "queue.json")
            store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB")],
                "2026-07-18T00:00:00+00:00",
            )
            store.update_by_app_id(
                730,
                status="downloaded",
                last_prefilled_at="2026-07-17T12:00:00+00:00",
                last_downloaded="2.0 GiB",
                last_downloaded_job_id="old-job",
            )
            snapshot = parse_progress_snapshot("Counter-Strike 2 is already up to date")
            store.apply_progress(snapshot, job_id="job-2")
            game = store.list_games()[0]
            self.assertEqual(game.last_downloaded, "0 B")
            self.assertEqual(game.last_prefilled_at, "2026-07-17T12:00:00+00:00")
            response = build_library_response(store, queue)
            self.assertEqual(response.summary.latest_run_downloaded_bytes, 0)

    def test_summary_reports_transfer_and_queue_estimates(self):
        from app.library import build_library_response

        with tempfile.TemporaryDirectory() as directory:
            store = LibraryStore(Path(directory) / "library.json")
            queue = QueueStore(Path(directory) / "queue.json")
            store.replace_selected(
                [
                    SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB"),
                    SelectedApp(app_id=570, name="Dota 2", download_size="20.0 GiB"),
                ],
                "2026-07-18T00:00:00+00:00",
            )
            queue.enqueue(
                GameQueueItem(
                    queue_id="a",
                    app_id=730,
                    app_name="Counter-Strike 2",
                    requested_at="2026-07-18T00:00:00+00:00",
                )
            )
            response = build_library_response(store, queue)
            self.assertEqual(response.summary.known_size_count, 2)
            self.assertGreater(response.summary.total_size_bytes, 54 * 1024**3)
            self.assertGreater(response.summary.queue_remaining_bytes, 34 * 1024**3)

    def test_forget_status_does_not_remove_game(self):
        with tempfile.TemporaryDirectory() as directory:
            store = LibraryStore(Path(directory) / "library.json")
            store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB")],
                "2026-07-18T00:00:00+00:00",
            )
            store.update_by_app_id(
                730,
                status="downloaded",
                progress=100.0,
                last_downloaded="2.0 GiB",
                last_downloaded_job_id="job-1",
            )
            forgotten = store.forget_status(730)
            self.assertIsNotNone(forgotten)
            self.assertEqual(forgotten.status, "selected")
            self.assertIsNone(forgotten.last_downloaded)
            self.assertEqual(len(store.list_games()), 1)


if __name__ == "__main__":
    unittest.main()
