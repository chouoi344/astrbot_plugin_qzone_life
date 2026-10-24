"""Simple experience choices with inherited identity and built-in policies."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def normalize_config(config: Mapping[str, Any] | None) -> dict[str, bool | int | str]:
    """Return only current user choices without mutating legacy configuration.

    AstrBot may insert the new slider's default before plugin initialization.
    An existing legacy maximum therefore takes precedence during migration;
    the caller removes legacy keys before saving this normalized dictionary.
    """
    config = config or {}
    raw_limit = config.get("daily_max", config.get("daily_post_limit", 2))
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError, OverflowError):
        limit = 2
    provider_id = config.get("llm_provider_id", "")
    if not isinstance(provider_id, str):
        provider_id = ""
    failure_policy = config.get("failure_policy")
    if failure_policy not in ("直接跳过", "稍后重试"):
        # Preserve the one meaningful legacy choice; retry timings stay built in.
        legacy_retry_count = config.get("auto_retry_count")
        failure_policy = "直接跳过" if legacy_retry_count in (0, "0") else "稍后重试"
    return {
        "auto_publish": config.get("auto_publish")
        if isinstance(config.get("auto_publish"), bool)
        else True,
        "daily_post_limit": max(0, min(5, limit)),
        "auto_reply": config.get("auto_reply")
        if isinstance(config.get("auto_reply"), bool)
        else True,
        "llm_provider_id": provider_id.strip(),
        "failure_policy": failure_policy,
    }


@dataclass
class Settings:
    auto_publish: bool = True
    auto_reply: bool = True
    admin_commands_only: bool = True
    daily_min: int = 1
    daily_max: int = 2
    min_gap_minutes: int = 180
    auto_retry_count: int = 2
    auto_retry_first_minutes: int = 10
    auto_retry_second_minutes: int = 30
    poll_seconds: int = 60
    watch_posts: int = 20
    max_replies_per_poll: int = 5
    llm_provider_id: str = ""
    vision_provider_id: str = ""
    persona_id: str = ""
    timezone: str = "Asia/Shanghai"
    selfie_timeout: int = 600
    llm_timeout: int = 90
    request_timeout: int = 45
    cookie_ttl: int = 1800
    max_images: int = 9

    @classmethod
    def from_config(cls, config: Mapping[str, Any] | None, host_config=None):
        visible = normalize_config(config)
        timezone = str((host_config or {}).get("timezone") or "Asia/Shanghai").strip()
        try:
            ZoneInfo(timezone)
        except (ValueError, ZoneInfoNotFoundError):
            timezone = "Asia/Shanghai"
        # Runtime timings stay built in; identity and vision follow the host.
        return cls(
            auto_publish=visible["auto_publish"],
            auto_reply=visible["auto_reply"],
            daily_min=1 if visible["daily_post_limit"] else 0,
            daily_max=visible["daily_post_limit"],
            llm_provider_id=visible["llm_provider_id"],
            auto_retry_count=0 if visible["failure_policy"] == "直接跳过" else 2,
            timezone=timezone,
        )


def admin_ids(context) -> set[str]:
    """Only numeric QQ identities from the host's existing administrator list."""
    raw = context.get_config().get("admins_id", [])
    if not isinstance(raw, (list, tuple, set)):
        return set()
    return {str(value).strip() for value in raw if str(value).strip().isdigit()}
