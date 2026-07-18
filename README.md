# CacheDeck

CacheDeck is a browser control panel for an existing
[SteamPrefill](https://github.com/tpill90/steam-lancache-prefill) container.
It provides persistent server-side prefill jobs, reconnectable live output,
schedule visibility, run history, diagnostics and an on-demand interactive
console without replacing SteamPrefill's own authentication or game selector.

## Features

- Detached prefills continue after the browser or laptop disconnects
- Reconnectable live output using a WebSocket stream rather than repeated log polling
- Detection of jobs started by SteamPrefill's scheduler or another client
- Pause and resume controls for active CacheDeck or scheduler-started prefills
- Persistent run history stored in CacheDeck's `/config` directory
- Optional one-shot automatic resume after a managed job is interrupted
- Configured cron schedule, timezone and next expected run display
- One-click environment diagnostics with copyable support output
- Lazy-loaded interactive SteamPrefill console for selecting games
- Copy, download, clear-view and full-screen prefill logs
- Optional browser completion notifications
- Bundled terminal assets, so the UI does not depend on a public CDN
- Docker, GHCR and Unraid Community Applications packaging

## Requirements

- A working SteamPrefill Docker container
- Access to `/var/run/docker.sock`
- The SteamPrefill path and user used by the target container
- A trusted LAN; CacheDeck has no built-in authentication and should not be
  exposed directly to the internet

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
      TARGET_CONTAINER: LANCache-Prefill
      PREFILL_DIR: /lancacheprefill/SteamPrefill
      PREFILL_USER: prefill
      PREFILL_STATE_DIR: /tmp/cachedeck
      CACHEDECK_CONFIG_DIR: /config
      AUTO_RESUME_INTERRUPTED: "false"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./cachedeck-config:/config
```

Open `http://YOUR-SERVER-IP:8088`.

## Persistent jobs and state

The **Start prefill** button launches SteamPrefill as a detached process inside
the existing target container. Closing the browser, disconnecting the computer,
or restarting only CacheDeck does not stop that job.

Two locations are used deliberately:

- `PREFILL_STATE_DIR` is inside the SteamPrefill target container and stores the
  active PID, exit code and live log. Its default is `/tmp/cachedeck`.
- `CACHEDECK_CONFIG_DIR` is inside CacheDeck and should be mapped to persistent
  appdata. It stores run history that survives CacheDeck and target-container
  recreation.

The **Pause prefill** button temporarily freezes the active SteamPrefill process
without stopping CacheDeck or the target container. Resume continues the same
process, although a network request that timed out during a long pause may retry.
This works for CacheDeck-managed and scheduler-started prefills.

A restart of the SteamPrefill target container still terminates the actual
SteamPrefill process. Set `AUTO_RESUME_INTERRUPTED=true` to let CacheDeck make
one automatic restart attempt after it detects that a managed job disappeared.
The UI calls this **Restart recovery** because it is separate from manual
Pause/Resume. It is disabled by default to avoid unexpected bandwidth use.

When a target-container log says `SteamPrefill already running, aborting schedule`,
the existing prefill has not been stopped. The scheduler detected the active job
and skipped only its duplicate launch.

## Schedule detection

CacheDeck looks for the following target-container environment variables:

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
- Configured user and working directory
- SteamPrefill executable availability
- Write access to the target job-state directory
- Write access to CacheDeck's persistent config directory
- Detected schedule information

The resulting text deliberately omits the target container's full environment
so secrets are not copied into support posts.

## Unraid

The Community Applications template is:

```text
templates/cachedeck.xml
```

Before public submission:

1. Push the repository to GitHub.
2. Tag the release, for example `v0.5.1`, if you want matching semver image tags.
3. Confirm the GitHub Actions build succeeds.
4. Make the `ghcr.io/darmachd/cachedeck` package public.
5. Test a clean install from the Unraid template.
6. Run Validate and Scan in the Community Apps submission portal.
7. Submit the repository after all checks pass.

## Versioning

`VERSION` is the single release-version source for ordinary builds. A Git tag
such as `v0.5.1` overrides it during the tagged GitHub Actions build and also
creates semver container tags.

## Development

Create a Python 3.13 virtual environment and install the requirements:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload
```

The application needs the Docker socket and a Linux PTY for its terminal, so the
complete terminal workflow is best tested inside Docker or directly on Unraid.

## Security

Mounting the Docker socket provides powerful control over the Docker host.
CacheDeck has no built-in authentication in this release. Keep it accessible
only from trusted networks.

## Licence

MIT. Copyright © 2026 Danny.

Bundled third-party browser assets are documented in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
