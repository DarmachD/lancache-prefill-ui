# Changelog

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
