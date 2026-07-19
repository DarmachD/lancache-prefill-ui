from __future__ import annotations

import shlex

from app.providers.base import PrefillProvider, ProviderCapabilities


class SteamPrefillProvider(PrefillProvider):
    provider_id = "steamprefill"
    display_name = "SteamPrefill compatibility provider"
    compatibility_mode = True
    capabilities = ProviderCapabilities(
        selected_library=True,
        per_app_jobs=True,
        structured_progress=False,
        manifest_tracking=False,
        depot_tracking=False,
        native_pause_resume=False,
        cache_object_index=False,
        per_game_purge=False,
    )

    def __init__(self, *, working_directory: str, container_user: str, command: str) -> None:
        self._working_directory = working_directory
        self._container_user = container_user
        self._command = command.strip() or "./SteamPrefill prefill"

    @property
    def working_directory(self) -> str:
        return self._working_directory

    @property
    def container_user(self) -> str:
        return self._container_user

    @property
    def select_games_command(self) -> str:
        return "./SteamPrefill select-apps"

    @property
    def status_command(self) -> str:
        return "./SteamPrefill select-apps status --no-ansi"

    @property
    def clear_cache_command(self) -> str:
        return "./SteamPrefill clear-cache -y"

    @property
    def schedule_keys(self) -> tuple[str, ...]:
        return (
            "GlobalSchedule",
            "GLOBAL_SCHEDULE",
            "PREFILL_SCHEDULE",
            "STEAMPREFILL_SCHEDULE",
            "SCHEDULE",
        )

    @property
    def selected_app_config_candidates(self) -> tuple[str, ...]:
        return (
            "./Config/selectedAppsToPrefill.json",
            "/Config/selectedAppsToPrefill.json",
        )

    @property
    def downloaded_state_candidates(self) -> tuple[str, ...]:
        return (
            "./Config/successfullyDownloadedDepots.json",
            "/Config/successfullyDownloadedDepots.json",
        )

    def managed_prefill_command(self, app_id: int | None = None) -> str:
        command = self._command
        parts = shlex.split(command)
        if app_id is not None:
            try:
                prefill_index = parts.index("prefill")
            except ValueError:
                parts.extend(["prefill", str(app_id)])
            else:
                parts.insert(prefill_index + 1, str(app_id))
        if "--verbose" not in parts:
            parts.append("--verbose")
        if "--no-ansi" not in parts:
            parts.append("--no-ansi")
        return shlex.join(parts)

    def process_match_shell(self, variable: str = "$cmdline") -> str:
        # Keep the shell expression unquoted because callers use it inside [[ ... ]].
        return f'[[ {variable} =~ SteamPrefill.*[[:space:]]prefill([[:space:]]|$) ]]'
