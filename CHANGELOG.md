# Changelog

## 0.8.2

- Recovered `selectedAppsToPrefill.json` after SteamPrefill's interactive selector saves the selection but crashes during its optional post-selection app-metadata scan.
- Added a clear terminal warning for transient Steam app-metadata timeouts without falsely marking games as missing or failed.
- Added automatic retry of managed full and targeted prefills when SteamPrefill reports transient `TaskCanceledException`/AppInfo metadata failures.
- Preserved the last verified game state when all automatic Steam metadata retries are exhausted; the queue reports a retryable Steam outage instead of changing a known-downloaded game to **Failed**.
- Added structured `provider.steam_metadata_deferred` activity events for exhausted transient failures.
- Made wrapper completion markers atomic by removing the live PID before publishing the exit marker.
- Added bounded retry settings through `CACHEDECK_STEAM_METADATA_ATTEMPTS` and `CACHEDECK_STEAM_METADATA_RETRY_SECONDS`.
- Added integration tests for retry-then-success, exhausted transient failure state preservation and selector metadata-timeout detection.

## 0.8.1

- Fixed fast cached or already-current targeted checks being falsely reported as `SteamPrefill did not confirm that the detached job started`.
- Replaced the detached-process lifetime probe with an explicit wrapper-ready handshake and made the wrapper own its PID file, removing the short-job PID race.
- Added automatic recovery for v0.8.0 queue items that were marked failed even though the embedded wrapper recorded a successful exit.
- Added structured in-memory parsing of interactive **Select games** terminal output so observed downloads immediately update the Games tab.
- Kept raw terminal transcripts out of persistent storage to avoid retaining Steam login or Steam Guard input.
- Throttled interactive progress persistence to reduce unnecessary SQLite writes while retaining immediate completion events.
- Improved detached-start failure messages with the actual readiness/exit reason.
- Preserved Steam app IDs and the active provider when a status table omits its App ID column.
- Closed SQLite backup connections explicitly and made ResourceWarnings fail the CI test suite.
- Added end-to-end tests for a near-instant embedded targeted check, false-failure recovery and Biped-style interactive completion output.

## 0.8.0

- Added the `embedded-steam` provider and made it the default.
- Bundled the official SteamPrefill 3.6.0 Linux x64 core inside the CacheDeck image.
- Removed the requirement for a separate SteamPrefill container in normal operation.
- Moved Steam execution, detached jobs, live logs and engine state into CacheDeck's persistent `/config/steam-engine` directory.
- Retained the external `steamprefill` provider as a compatibility option.
- Added automatic selected-app seeding from CacheDeck SQLite and an optional one-click import from an old target container.
- Added upgrade aliases for the known v0.7 working-directory, user, command and job-state defaults so existing Unraid variables do not accidentally send embedded data to temporary legacy paths.
- Added embedded-engine readiness, core version and execution-mode reporting to the API and Dashboard.
- Added LANCache Steam DNS resolution to Diagnostics.
- Removed the Docker-socket requirement for the embedded provider; it remains optional for legacy mode/import.
- Updated Compose and Unraid templates for the all-in-one architecture.
- Added bundled-core verification and embedded-provider assertions to GitHub Actions.
- Fixed the in-image provider test failure caused by Docker defaults pinning the embedded binary to `/config/steam-engine` even when the engine directory was relocated.
- Made the embedded binary, working directory, state directory and command follow a custom engine directory while preserving genuinely custom overrides.
- Added compatibility handling when switching an existing v0.8 installation back to the legacy external-container provider.
- Hardened the entrypoint with packaged-binary validation and custom binary-path support.
- Changed engine readiness reporting to verify the executable and writable working directory instead of assuming every local provider is ready.
- Kept capability reporting honest: the v0.8 beta still uses SteamPrefill as a transitional core and does not yet claim direct structured SteamKit progress or authoritative cache-object indexing.

## 0.7.4

- Added persistent CacheDeck-managed schedules with create, edit, enable/disable and remove controls.
- Added multiple named cron schedules with per-schedule timezone, next run, last result and run-now action.
- Added a server-side schedule worker that records started, skipped and failed schedule events.
- Kept target-container schedule detection visible with a duplicate-schedule warning.
- Added explicit **Select all**, **Select visible** and **Deselect all** controls in the Games tab.
- Added schedule schema migration, CRUD persistence and cron validation tests.
- Extended the Docker HTTP smoke test to exercise schedule creation and deletion.

## 0.7.3

- Renamed the misleading **Not downloaded** state to **Unverified**.
- Added manual per-game **Mark as downloaded** records with a visible verification source.
- Added **Scan historical logs** to recover completions still visible in retained SteamPrefill logs.
- Added **Verify unverified** to queue targeted Steam checks for unresolved library state.
- Added verification-source labels for observed downloads, Steam checks, full runs, provider history and manual records.
- Rebuilt the Activity tab rows with separate event-type, timestamp and detail columns plus responsive mobile spacing.
- Changed Activity filtering to update after typing rather than requiring focus loss.

## 0.7.2

- Fixed the GitHub Actions HTTP smoke test to wait for Docker health before querying the API.
- Removed the stale hard-coded schema-v1 assertion; the smoke test now verifies that `/api/health` and `/api/engine` agree on the current schema version.

- Stopped restarting Steam metadata resolution on every Games-view poll.
- Added exponential metadata retry backoff and a force-refresh path.
- Added a persistent incremental activity-log cursor with active-game continuity.
- Preserved live, queued and completed game state across selected-library refreshes.
- Added immediate structured completion events when SteamPrefill output identifies a finished game.
- Added cautious import of explicit app-level success records from SteamPrefill provider state.
- Changed unavailable per-game progress from misleading 0% to **Progress unknown**.
- Prevented silent background polling from flashing the loading message.
- Persisted Games-view search, filter, sort and updates-only preferences.

## 0.7.1

- Reduced SQLite write amplification for single-game state updates.
- Added startup recovery for queue items stranded before a job ID was assigned.
- Added retryable legacy JSON migration and ordered schema migration groundwork.
- Added same-origin protection for state-changing HTTP requests.
- Added filterable structured Activity view, database backups and engine repair controls.
- Prevented duplicate metadata refresh workers.

## 0.7.0

- Added CacheDeck-owned SQLite state at `/config/cachedeck.db`
- Added automatic one-time migration of v0.6 library, queue and history JSON without deleting the source files
- Added provider-neutral tables for games, queues, jobs, events, depots and manifests
- Added a provider interface and moved SteamPrefill command/capability knowledge into the SteamPrefill compatibility provider
- Added structured game, queue and job state events with `/api/engine/events` access
- Added `/api/engine` capability, schema, migration and record-count reporting
- Added a Dashboard Engine card showing the active provider and native-engine foundations
- Added provider fields and future manifest fields to persistent game/job records
- Added SQLite integrity and provider checks to Diagnostics
- Added same-origin WebSocket protection plus `CACHEDECK_ALLOWED_ORIGINS` for deliberate reverse-proxy origins
- Added GitHub Actions unit, container-build and HTTP smoke tests before image publication
- Added database migration, persistence, event and provider unit tests
- Kept SteamPrefill as the active download provider; native Steam downloads are intentionally deferred to v0.8

## 0.6.2

- Added a direct fallback to SteamPrefill's `Config/selectedAppsToPrefill.json` when `select-apps status` fails or times out
- Prevented a single Steam manifest failure from leaving the Games view completely empty
- Added background Steam app-ID metadata resolution so fallback entries gain proper names and artwork
- Skipped the manifest-heavy status scan while a real prefill is active, avoiding a competing SteamPrefill process
- Added a deep current-run log reconciliation after metadata resolution so already completed games appear without restarting the prefill
- Preserved the last known game library when a later refresh fails
- Added recognition of SteamPrefill's real `Finished downloading ...` log line so completed games update during an active run
- Kept transfer sizes honest: they remain unknown when SteamPrefill cannot calculate them
- Made the Dashboard status action use the same compatibility-safe library refresh path
- Added parser and persistence tests for selected-app config fallback and real completion logs

## 0.6.1

- Added selected-library compressed transfer totals and estimated remaining queue size
- Added per-game latest-run downloaded amounts when SteamPrefill reports them
- Added multi-select with bulk **Check & update**, select-visible and clear-selection controls
- Added confirmation before queuing large batches or starting large full-library checks
- Added **Only show updates** and **Retry failed** shortcuts
- Added last-checked and last-downloaded sorting
- Added persistent comfortable/compact list density
- Added a sticky game-library control bar for long libraries
- Added per-game overflow actions with Steam Store links and **Forget CacheDeck status**
- Clarified throughout the UI that compressed transfer size is not exact LANCache disk usage
- Added queue and summary tests for size estimates and latest-run tracking
- Fixed Games-view event handlers being registered inside the notification toggle path

## 0.6.0

- Added Dashboard and Games view tabs
- Added a Steam game list with artwork, names, compressed sizes and status badges
- Added live per-game progress bars using SteamPrefill output
- Added selected, queued, checking, downloading, downloaded, update-available and failed states
- Added a persistent per-game Check & update queue
- Added one-click per-game targeted prefills and a full-library Check all & update action
- Added search, status filters and name/size/status sorting
- Added cached Steam app-ID, artwork and store-link metadata under `/config`
- Corrected the SteamPrefill status action to use `select-apps status --no-ansi`
- Added unit tests for status parsing, live progress and persistent queue/library state
- Managed checks now add `--verbose --no-ansi` so CacheDeck can track up-to-date games reliably
- Added recognition of successful scheduler/external run summaries
- Uses Steam app IDs for artwork and store links immediately when SteamPrefill provides them

## 0.5.1

- Added Pause/Resume control for active SteamPrefill jobs, including scheduler-started jobs
- Clarified that `already running, aborting schedule` means only the duplicate scheduled launch was skipped
- Renamed the UI's auto-resume status to Restart recovery to distinguish it from manual Pause/Resume
- Ensured a paused managed job can still be stopped cleanly
- Disabled browser caching for the main dashboard so updates do not leave an old UI behind

## 0.5.0

- Replaced repeated log polling with a reconnectable live WebSocket stream
- Made the interactive console lazy-loaded so it opens only when needed
- Added elapsed time and a current-activity summary to the prefill dashboard
- Added persistent run history in CacheDeck's `/config` directory
- Added optional one-shot automatic resume for interrupted managed jobs
- Added schedule detection, timezone display and next-run calculation
- Added one-click diagnostics with copyable support output
- Added copy, download, clear-view and full-screen controls for prefill logs
- Added optional browser notifications when a running job finishes
- Moved cache deletion into a collapsed danger zone
- Bundled xterm browser assets instead of loading them from a public CDN
- Added a single `VERSION` source and automated Docker build-version injection
- Expanded the Unraid template with persistent appdata and recovery settings

## 0.4.0

- Launches prefills as detached server-side jobs instead of terminal commands
- Keeps downloads running when the browser or laptop disconnects
- Restores active job state and output when CacheDeck is reopened
- Adds live prefill status, timestamps, exit code and a reconnectable log
- Detects SteamPrefill jobs started by the target container scheduler
- Prevents duplicate prefill launches
- Adds a safe stop control for jobs launched by CacheDeck
- Keeps the interactive terminal for game selection and manual commands

## 0.3.0

- Renamed the project to CacheDeck
- Updated all repository, package and maintainer metadata
- Added current Unraid Community Applications repository metadata
- Moved the app template to `templates/cachedeck.xml`
- Added the required root-level `ca_profile.xml`
- Added a new CacheDeck icon and browser favicon
- Updated the dashboard branding and responsive layout
- Added target-container details and automatic health refresh
- Added recent Docker log viewing
- Added safer error handling and destructive-action confirmation
- Updated the GitHub Actions workflow for GHCR publishing
- Added pull-request build validation
- Updated Python dependencies

## 0.2.0

- Added the initial browser terminal and SteamPrefill action controls
- Added early Docker and Unraid packaging
