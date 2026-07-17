# LANCache Prefill UI

A browser control panel for an existing SteamPrefill container, packaged so it can be published through GitHub Container Registry and submitted to Unraid Community Applications.

## Current release

- Browser-based SteamPrefill game picker
- One-click Select Games and Start Prefill controls
- Live interactive console
- Docker health check
- Unraid XML template
- GitHub Actions build and publish workflow
- Editable environment variables in the Unraid template

The current version deliberately wraps SteamPrefill's maintained interactive picker instead of recreating Steam's account API. This avoids the dead API endpoint that broke Autofiller.Web.
