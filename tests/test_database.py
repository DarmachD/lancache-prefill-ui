import json
import tempfile
import unittest
from pathlib import Path

from app.database import SCHEMA_VERSION, StateDatabase
from app.library import GameQueueItem, SelectedApp
from app.state import SQLiteLibraryStore, SQLiteQueueStore


class DatabaseMigrationTests(unittest.TestCase):
    def test_imports_legacy_json_without_deleting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            library_path = root / "library.json"
            queue_path = root / "game-queue.json"
            history_path = root / "history.json"
            library_path.write_text(
                json.dumps(
                    {
                        "last_refreshed_at": "2026-07-18T00:00:00+00:00",
                        "last_completed_job_id": "job-1",
                        "last_activity_job_id": "job-1",
                        "games": [
                            {
                                "key": "steam:730",
                                "app_id": 730,
                                "name": "Counter-Strike 2",
                                "status": "downloaded",
                                "selected": True,
                                "progress": 100.0,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            queue_path.write_text(
                json.dumps(
                    [
                        {
                            "queue_id": "q1",
                            "app_id": 730,
                            "app_name": "Counter-Strike 2",
                            "requested_at": "2026-07-18T00:00:00+00:00",
                            "state": "completed",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            history_path.write_text(
                json.dumps(
                    [
                        {
                            "job_id": "job-1",
                            "source": "cachedeck",
                            "state": "completed",
                            "started_at": "2026-07-18T00:00:00+00:00",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            database = StateDatabase(root / "cachedeck.db")
            result = database.migrate_legacy_json(
                library_path=library_path,
                queue_path=queue_path,
                history_path=history_path,
            )

            self.assertTrue(result["completed"])
            self.assertEqual(result["counts"]["games"], 1)
            self.assertEqual(database.counts()["games"], 1)
            self.assertEqual(database.counts()["jobs"], 1)
            self.assertEqual(database.get_meta("schema_version"), str(SCHEMA_VERSION))
            self.assertTrue(library_path.exists())
            self.assertTrue(queue_path.exists())
            self.assertTrue(history_path.exists())

            second = database.migrate_legacy_json(
                library_path=library_path,
                queue_path=queue_path,
                history_path=history_path,
            )
            self.assertEqual(second["completed_at"], result["completed_at"])


class DatabaseStoreTests(unittest.TestCase):
    def test_library_state_and_events_are_persistent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = StateDatabase(Path(directory) / "cachedeck.db")
            store = SQLiteLibraryStore(database)
            store.replace_selected(
                [SelectedApp(app_id=730, name="Counter-Strike 2", download_size="34.4 GiB")],
                "2026-07-18T00:00:00+00:00",
            )
            store.update_by_app_id(730, status="downloaded", progress=100.0)

            reopened = SQLiteLibraryStore(StateDatabase(Path(directory) / "cachedeck.db"))
            game = reopened.list_games()[0]
            self.assertEqual(game.status, "downloaded")
            self.assertEqual(game.provider, "steamprefill")
            event_types = {event["event_type"] for event in database.list_events()}
            self.assertIn("game.discovered", event_types)
            self.assertIn("game.status_changed", event_types)

    def test_resolving_app_identity_does_not_emit_unselected_event(self):
        with tempfile.TemporaryDirectory() as directory:
            database = StateDatabase(Path(directory) / "cachedeck.db")
            store = SQLiteLibraryStore(database)
            store.replace_selected(
                [SelectedApp(app_id=None, name="Counter-Strike 2")],
                "2026-07-18T00:00:00+00:00",
            )
            game = store.list_games()[0]
            store.save_metadata(
                game.key,
                730,
                "https://example.invalid/header.jpg",
                "https://store.steampowered.com/app/730/",
                "Counter-Strike 2",
            )
            event_types = [event["event_type"] for event in database.list_events()]
            self.assertIn("game.identity_resolved", event_types)
            self.assertNotIn("game.unselected", event_types)

    def test_queue_is_persistent_and_deduplicated(self):
        with tempfile.TemporaryDirectory() as directory:
            database = StateDatabase(Path(directory) / "cachedeck.db")
            queue = SQLiteQueueStore(database)
            first = queue.enqueue(
                GameQueueItem(
                    queue_id="q1",
                    app_id=730,
                    app_name="Counter-Strike 2",
                    requested_at="2026-07-18T00:00:00+00:00",
                )
            )
            second = queue.enqueue(
                GameQueueItem(
                    queue_id="q2",
                    app_id=730,
                    app_name="Counter-Strike 2",
                    requested_at="2026-07-18T00:01:00+00:00",
                )
            )
            self.assertEqual(first.queue_id, second.queue_id)
            reopened = SQLiteQueueStore(StateDatabase(Path(directory) / "cachedeck.db"))
            self.assertEqual(len(reopened.active()), 1)

    def test_depot_and_manifest_foundations_are_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            database = StateDatabase(Path(directory) / "cachedeck.db")
            database.upsert_depot(
                provider="native-steam",
                app_id=730,
                depot_id=731,
                name="Counter-Strike 2 content",
            )
            database.record_manifest(
                provider="native-steam",
                app_id=730,
                depot_id=731,
                manifest_id="1234567890",
                compressed_bytes=1024,
            )
            counts = database.counts()
            self.assertEqual(counts["depots"], 1)
            self.assertEqual(counts["manifests"], 1)
            self.assertIn(
                "manifest.observed",
                {event["event_type"] for event in database.list_events()},
            )


class DatabaseHardeningTests(unittest.TestCase):
    def test_failed_legacy_import_can_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            library = root / "library.json"
            queue = root / "game-queue.json"
            history = root / "history.json"
            library.write_text("{broken", encoding="utf-8")
            database = StateDatabase(root / "cachedeck.db")
            first = database.migrate_legacy_json(
                library_path=library, queue_path=queue, history_path=history
            )
            self.assertFalse(first["completed"])
            library.write_text(json.dumps({"games": []}), encoding="utf-8")
            second = database.migrate_legacy_json(
                library_path=library, queue_path=queue, history_path=history
            )
            self.assertTrue(second["completed"])

    def test_reconciles_stranded_running_queue_item(self):
        with tempfile.TemporaryDirectory() as directory:
            database = StateDatabase(Path(directory) / "cachedeck.db")
            queue = SQLiteQueueStore(database)
            queue.enqueue(GameQueueItem(
                queue_id="q1", app_id=730, app_name="Counter-Strike 2",
                requested_at="2026-07-18T00:00:00+00:00", state="running"
            ))
            result = database.reconcile_queue()
            self.assertEqual(result["requeued"], 1)
            self.assertEqual(queue.list()[0].state, "queued")

    def test_database_backup_and_event_filtering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = StateDatabase(root / "cachedeck.db")
            database.append_event("game.progress", app_id=730)
            database.append_event("queue.added", app_id=440)
            backup = database.backup_to(root / "backups" / "cachedeck.db")
            self.assertTrue(Path(backup["path"]).is_file())
            events = database.list_events(event_type="game.progress", app_id=730)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_type"], "game.progress")


if __name__ == "__main__":
    unittest.main()
