# CacheDeck

CacheDeck is a browser control plane for keeping Steam content warm in a
LANCache. It provides a visual game library, persistent queue, detached jobs,
schedules, structured activity, diagnostics and an interactive console.

## v0.8 embedded-engine beta

CacheDeck 0.8 defaults to the `embedded-steam` provider. The official
SteamPrefill download core is bundled inside the CacheDeck image and runs as a
child process in the same container. A separate SteamPrefill Docker container
is therefore no longer required for normal operation.

This is a transitional engine rather than a direct SteamKit rewrite:

- CacheDeck owns process execution, schedules, logs, queue and SQLite state.
- SteamPrefill 3.6.1 remains the component which logs in to Steam, resolves
  depots/manifests and sends downloads through LANCache.
- The provider boundary remains in place so a future structured Steam worker can
  replace that core without another UI or database migration.
- Structured per-chunk progress, authoritative cache residency and safe per-game
  purging are not yet available.

The legacy `steamprefill` provider remains available for installations that want
CacheDeck to control an existing external SteamPrefill container.

## Features

- Embedded Steam prefill engine; no separate prefill container required
- Detached downloads which continue after the browser disconnects
- Pause, resume and stop controls
- Visual Steam library with artwork, queue and verification state
- One-click and bulk **Check & update** actions
- Persistent SQLite catalogue, queue, history, schedules and activity
- Multiple editable timezone-aware cron schedules
- Reconnectable live logs and an on-demand interactive terminal
- Interactive selector downloads are folded back into structured Games status without persisting the raw terminal transcript
- Historical log scanning and manual verification overrides
- Database backup, repair and installation diagnostics
- Same-origin protection for WebSockets and state-changing HTTP requests
- Optional legacy-provider and selected-game import support
- Docker, GHCR and Unraid Community Applications packaging

## Requirements

For the default embedded provider:

- A working LANCache installation
- DNS inside the CacheDeck container which resolves
  `lancache.steamcontent.com` to your LANCache address
- A persistent `/config` mapping
- Network access to Steam
- A trusted LAN or authenticated reverse proxy; CacheDeck has no built-in user
  authentication

The Docker socket is **not required** by `embedded-steam`. Mount it only when
using the legacy provider or the one-click selected-game import from an old
SteamPrefill container.

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
      CACHEDECK_PROVIDER: embedded-steam
      CACHEDECK_STEAM_ENGINE_DIR: /config/steam-engine
      PREFILL_STATE_DIR: /config/steam-engine/state
      CACHEDECK_CONFIG_DIR: /config
      AUTO_RESUME_INTERRUPTED: "false"
      TZ: Europe/London
    volumes:
      - ./cachedeck-config:/config
```

Open `http://YOUR-SERVER-IP:8088`.

When custom DNS is required, configure the container to use the same DNS path as
other LANCache clients. CacheDeck Diagnostics reports the IPv4 address returned
for `lancache.steamcontent.com` and warns when it does not look like a private
LAN address.

## First run

1. Open CacheDeck.
2. Select **Select games**.
3. Complete Steam login/Steam Guard in the interactive console.
4. Choose the games to prefill and save the selection.
5. At **Run prefill now?**, choose **Yes** to hand the run to CacheDeck's managed background job, or **No** to save the selection without starting it.
6. You can also start it later or create a schedule.

Steam session/configuration data is stored under:

```text
/config/steam-engine/Config
```

The embedded binary and job state are stored under:

```text
/config/steam-engine/SteamPrefill
/config/steam-engine/state
```

Protect the complete `/config` directory as application credentials and session
data may be present within it.

## Upgrading from v0.7

The image defaults to the new embedded provider. Existing CacheDeck database,
game metadata, queue, run history and schedules remain in `/config/cachedeck.db`.

CacheDeck attempts to seed the embedded engine's selected-app file from its own
SQLite library when no embedded selection exists. On the Engine card,
**Import old SteamPrefill state** can copy both
`selectedAppsToPrefill.json` and `successfullyDownloadedDepots.json` from the
former `LANCache-Prefill` container when the Docker socket is temporarily
mounted. This preserves the old selected-game list and SteamPrefill
depot/manifest history so an upgrade does not treat every selected game as a
first-time prefill.

Steam credentials and session data are not copied from another container, so
complete Steam login once inside CacheDeck.

Recommended Unraid values:

```text
CACHEDECK_PROVIDER=embedded-steam
PREFILL_DIR=/config/steam-engine
PREFILL_STATE_DIR=/config/steam-engine/state
PREFILL_USER=
```

CacheDeck recognises the exact v0.7 defaults and redirects them to the embedded
paths automatically, but updating the template keeps the configuration clear.
Custom legacy paths remain untouched.

After importing the old SteamPrefill state, remove the Docker socket mapping
unless it is still needed for another reason. Disable the old SteamPrefill
container and its `GLOBAL_SCHEDULE` only after confirming the embedded engine
can log in and run a test prefill.

## Legacy external-container provider

To retain the v0.7 architecture:

```yaml
environment:
  CACHEDECK_PROVIDER: steamprefill
  TARGET_CONTAINER: LANCache-Prefill
  PREFILL_DIR: /lancacheprefill/SteamPrefill
  PREFILL_USER: prefill
  PREFILL_STATE_DIR: /tmp/cachedeck
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
  - ./cachedeck-config:/config
```

This mode requires the Docker socket and the external target container.

## Games and cache-state accuracy

**Downloaded** means CacheDeck observed or recorded a successful prefill/check.
**Unverified** means CacheDeck cannot prove the historical state; it does not
mean the game is definitely absent from LANCache.

SteamPrefill tracks previously prefetched versions and normally skips games that
are already current. CacheDeck adds `--verbose --no-ansi` to managed jobs so it
can recover as much per-game state as the transitional core reports.

LANCache stores shared HTTP objects rather than isolated game installations.
Objects can be evicted and transfer size does not equal exact disk usage.
Consequently CacheDeck does not offer a deceptive per-game uninstall button.

## Schedules

Schedules are created, edited, enabled, disabled, run immediately and removed
from the Dashboard. They use five-field cron expressions and an IANA timezone,
and are persisted in `/config/cachedeck.db`.

Example nightly schedule:

```text
0 2 * * *
Europe/London
```

A scheduled occurrence is recorded as skipped when a prefill is already active.
The browser does not need to remain open.

## Persistent state and backups

Primary state:

```text
/config/cachedeck.db
/config/cachedeck.db-wal
/config/cachedeck.db-shm
```

Database backups created from the Engine card are placed in:

```text
/config/backups
```

Steam engine files, credentials and logs live beneath `/config/steam-engine`.
Include the full `/config` directory in backups.

## Diagnostics

Diagnostics checks:

- Embedded binary and writable engine directories
- LANCache Steam DNS resolution
- Persistent config access
- SQLite integrity and schema
- Provider mode and capability reporting
- Schedule detection
- Docker socket/API only when the selected provider requires them

## Security

Keep CacheDeck LAN-only or place it behind an authenticated reverse proxy.
Same-origin browser requests work by default. Deliberate alternate proxy origins
can be allowed with:

```text
CACHEDECK_ALLOWED_ORIGINS=https://cachedeck.example.com
```

Do not use a wildcard unless access control is enforced before requests reach
CacheDeck.

## Development

Create a Python 3.13 environment and run the tests:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Run the API locally:

```bash
uvicorn app.main:app --reload
```

Build the complete embedded-engine image:

```bash
docker build -t cachedeck:test .
docker run --rm -p 8088:8080 -v "$PWD/test-config:/config" cachedeck:test
```

GitHub Actions builds the image, runs the unit tests inside it, verifies the
bundled SteamPrefill binary and performs HTTP API/schedule smoke tests before
publishing.

## Third-party component

The embedded beta packages the official SteamPrefill 3.6.1 Linux x64 release.
SteamPrefill is MIT licensed. See `THIRD_PARTY_NOTICES.md` for attribution and
licence text.
