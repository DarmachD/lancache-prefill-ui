# Changelog

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
