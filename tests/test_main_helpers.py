import unittest

from app.main import managed_prefill_command


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


if __name__ == "__main__":
    unittest.main()
