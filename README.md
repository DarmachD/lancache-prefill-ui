# CacheDeck

CacheDeck is a browser control plane for LANCache game prefilling. The current
provider is an existing
[SteamPrefill](https://github.com/tpill90/steam-lancache-prefill) container, but
CacheDeck now owns its game catalogue, queue, run history and structured event
state instead of treating terminal output as its database.

Version 0.7.2 is the hardened compatibility foundation for CacheDeck's native Steam prefill engine. It
keeps SteamPrefill as the working compatibility provider, so existing downloads,
schedules and authentication continue to work while the native provider is
built incrementally.

## Features

- Detached prefills continue after the browser or laptop disconnects
- Reconnectable live output using a WebSocket stream
- Detection of jobs started by SteamPrefill's scheduler or another client
- Pause and resume controls for active CacheDeck or scheduler-started prefills
- Games tab with Steam artwork, selected/downloaded/queued state and honest known/unknown progress
- Persistent per-game update queue with one-click and bulk **Check & update**
- Search, filters, sorting and comfortable/compact library density
- Compressed transfer totals, estimated queue size and latest-run amounts
- CacheDeck-owned SQLite state database under `/config`
- Provider-neutral game, queue, job, depot and manifest schema
- Structured engine events for game, queue and job state changes
- Automatic one-time import from v0.6 JSON state without deleting the originals
- Provider capability reporting so unsupported features are not faked in the UI
- Persistent run history and optional one-shot restart recovery
- Schedule, timezone and next-run visibility
- Environment, provider and SQLite diagnostics
- Lazy-loaded interactive SteamPrefill console
- Copy, download, clear-view and full-screen logs
- Same-origin WebSocket protection, with optional explicit reverse-proxy origins
- Bundled terminal assets, Docker/GHCR packaging and an Unraid template

## Requirements

- A working SteamPrefill Docker container for the v0.7 compatibility provider
- Access to `/var/run/docker.sock`
- The SteamPrefill path and user used by the target container
- A persistent `/config` mapping
- A trusted LAN, or an authenticated reverse proxy; CacheDeck has no built-in
  user authentication

## Docker Compose

```yaml
services:
  cachedeck:
    image: ghcr.io/darmachd/cachedeck:latest
    container_name: CacheDeck
    restart: unless-stopped
    ports:
      - "8088:8080"
    environment:
      CACHEDECK_PROVIDER: steamprefill
      TARGET_CONTAINER: LANCache-Prefill
      PREFILL_DIR: /lancacheprefill/SteamPrefill
      PREFILL_USER: prefill
      PREFILL_STATE_DIR: /tmp/cachedeck
      CACHEDECK_CONFIG_DIR: /config
      AUTO_RESUME_INTERRUPTED: "false"
      # Optional when a reverse proxy gives the browser a different origin:
      # CACHEDECK_ALLOWED_ORIGINS: https://cachedeck.example.com
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./cachedeck-config:/config
```

Open `http://YOUR-SERVER-IP:8088`.

## v0.7 engine foundations

The active engine is visible on the Dashboard. In v0.7 it reports:

- **Provider:** SteamPrefill compatibility provider
- **State owner:** CacheDeck SQLite
- **Structured progress:** unavailable from the provider, so legacy output is
  still parsed where necessary
- **Depot/manifest tracking:** schema ready but not populated until the native
  Steam provider arrives
- **Per-game purge:** unavailable because LANCache stores shared HTTP objects

The provider interface separates CacheDeck's UI and persistent state from the
program that performs the download. The next native provider can therefore
write exact app, depot, manifest, queue and progress events without replacing
the web application or migrating the library again.

The engine API is available at:

```text
/api/engine
/api/engine/events?limit=100
```

## Persistent jobs and state

The **Start prefill** button launches the active provider as a detached process
inside the target container. Closing the browser, disconnecting the computer or
restarting only CacheDeck does not stop that job.

Two locations are used deliberately:

- `PREFILL_STATE_DIR` is inside the SteamPrefill target container and stores the
  active PID, exit code and live log. Its default is `/tmp/cachedeck`.
- `CACHEDECK_CONFIG_DIR` is inside CacheDeck and should be mapped to persistent
  appdata. CacheDeck's main state file is `/config/cachedeck.db`.

SQLite may also create `cachedeck.db-wal` and `cachedeck.db-shm`; keep all three
inside the same persistent directory.

On the first v0.7 start, CacheDeck imports these v0.6 files when present:

```text
/config/library.json
/config/game-queue.json
/config/history.json
```

The source JSON files are preserved as a rollback/reference copy. CacheDeck then
uses SQLite for new state. The migration status and imported record counts are
shown in the Engine card.

The **Pause prefill** button temporarily freezes the active SteamPrefill process.
Resume continues the process, although a network request that timed out during a
long pause may retry. A target-container restart still terminates the process;
`AUTO_RESUME_INTERRUPTED=true` enables one restart attempt.

When the target log says `SteamPrefill already running, aborting schedule`, the
active prefill was not stopped. The scheduler skipped only its duplicate launch.

## Games view

CacheDeck first reads SteamPrefill's saved `selectedAppsToPrefill.json`, which
allows selected games to appear even when Steam's manifest service is failing.
It then attempts the heavier `select-apps status --no-ansi` scan for compressed
transfer sizes. Names and artwork are resolved in the background with exponential
retry backoff and persisted in CacheDeck's database. Normal Games-view polling no
longer restarts that metadata work.

CacheDeck tracks SteamPrefill output incrementally and persists the log cursor and
active game name in SQLite. This allows a completion line to remain attributable
after log rotation or after the original `Starting ...` line has fallen out of the
visible tail. Where SteamPrefill does not report a trustworthy percentage, the UI
shows **Progress unknown** instead of a misleading 0%.

A per-game **Check & update** action adds the Steam app to CacheDeck's queue. When
the provider is free, CacheDeck runs a targeted prefill. SteamPrefill does not
expose a separate dry-run update check, so checking and applying an available
update remain one operation while it is the active provider.

**Downloaded** means CacheDeck observed a successful prefill/check for the game.
It does not prove every underlying HTTP object still exists in LANCache after
cache eviction or manual clearing. Displayed sizes are compressed transfer
estimates rather than exact physical cache usage.

LANCache stores shared CDN objects, not isolated installed-game folders. A safe
per-game uninstall is therefore not available. **Forget CacheDeck status** only
resets CacheDeck's record and does not delete cache data.

## Schedule detection

The SteamPrefill compatibility provider recognises:

- `GlobalSchedule`
- `GLOBAL_SCHEDULE`
- `PREFILL_SCHEDULE`
- `STEAMPREFILL_SCHEDULE`
- `SCHEDULE`

Standard five-field cron expressions are displayed with the next expected run.
The target container's `TZ` value is used when present.

## Diagnostics

The diagnostics panel checks:

- Docker socket and Docker API access
- Target-container availability
- SteamPrefill executable and writable job-state directory
- Persistent `/config` access
- CacheDeck SQLite `quick_check`, schema and record count
- Active provider and compatibility mode
- Detected schedule information

The output omits the target container's full environment so secrets are not
copied into support posts.

## WebSocket origin protection

WebSockets accept same-origin browser connections by default. Reverse proxies
that deliberately use another public origin can set a comma-separated allowlist:

```text
CACHEDECK_ALLOWED_ORIGINS=https://cachedeck.example.com,https://other.example.com
```

Do not set `*` unless the application is already protected by a trusted
authentication layer.

## Unraid

The Community Applications template is:

```text
templates/cachedeck.xml
```

Before public submission:

1. Push the repository to GitHub.
2. Tag the release, for example `v0.7.2`.
3. Confirm the test and Docker-build jobs succeed.
4. Make the `ghcr.io/darmachd/cachedeck` package public.
5. Test a clean install and an upgrade from v0.6.2.
6. Run Validate and Scan in the Community Apps submission portal.
7. Submit the repository after all checks pass.

## Development

Create a Python 3.13 virtual environment and install the requirements:

```bash
python -m venv .venv
pip install -r requirements.txt
```

Run locally:

```bash
uvicorn app.main:app --reload
```

Run all tests:

```bash
python -m unittest discover -s tests -v
```

Build and smoke-test the same container used by GitHub Actions:

```bash
docker build -t cachedeck:test .
docker run --rm -v "$PWD/tests:/app/tests:ro" cachedeck:test python -m unittest discover -s tests -v
```

The complete PTY and targeted-prefill workflow still requires Docker socket
access and a real SteamPrefill target, so perform the final integration test on
Unraid.

## Security

Mounting the Docker socket provides powerful control over the Docker host.
CacheDeck has no built-in authentication in this release. Keep it on trusted
networks or behind an authenticated reverse proxy.

## Licence

MIT. Copyright © 2026 Danny.

Bundled third-party browser assets are documented in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
