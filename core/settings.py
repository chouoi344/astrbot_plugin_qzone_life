"""Validated plugin settings; no credentials are kept here."""

from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo


@dataclass
class Settings:
    auto_publish: bool = True
    auto_reply: bool = True
    admin_commands_only: bool = True
    daily_min: int = 1
    daily_max: int = 2
    min_gap_minutes: int = 180
    active_start: int = 8
    active_end: int = 23
    poll_seconds: int = 180
    watch_posts: int = 20
    max_replies_per_poll: int = 5
    llm_provider_id: str = ""
    vision_provider_id: str = ""
    persona_id: str = ""
    timezone: str = "Asia/Shanghai"
    selfie_timeout: int = 300
    llm_timeout: int = 90
    request_timeout: int = 45
    cookie_ttl: int = 1800
    max_images: int = 9

    @classmethod
    def from_config(cls, config: dict[str, Any], host_config=None):
        defaults = cls()
        values = {}
        for name, default in vars(defaults).items():
            raw = config.get(name, default)
            if isinstance(default, bool):
                values[name] = raw if isinstance(raw, bool) else default
            elif isinstance(default, int):
                try:
                    values[name] = int(raw)
                except (ValueError, TypeError):
                    values[name] = default
            else:
                values[name] = str(raw or "").strip()
        if not config.get("timezone"):
            values["timezone"] = str((host_config or {}).get("timezone") or defaults.timezone)
        ZoneInfo(values["timezone"])
        for key, lo, hi in (
            ("daily_min", 1, 2),
            ("daily_max", 1, 2),
            ("min_gap_minutes", 30, 1440),
            ("active_start", 0, 23),
            ("active_end", 1, 24),
            ("poll_seconds", 60, 3600),
            ("watch_posts", 1, 50),
            ("max_replies_per_poll", 1, 20),
            ("selfie_timeout", 60, 1200),
            ("llm_timeout", 15, 300),
            ("request_timeout", 10, 120),
            ("cookie_ttl", 60, 86400),
            ("max_images", 1, 9),
        ):
            values[key] = max(lo, min(hi, values[key]))
        values["daily_min"] = min(values["daily_min"], values["daily_max"])
        if values["active_start"] >= values["active_end"]:
            raise ValueError("自主发布开始小时必须早于结束小时")
        return cls(**values)


def admin_ids(context) -> set[str]:
    """Only numeric QQ identities from the host's existing administrator list."""
    raw = context.get_config().get("admins_id", [])
    if not isinstance(raw, (list, tuple, set)):
        return set()
    return {str(value).strip() for value in raw if str(value).strip().isdigit()}
