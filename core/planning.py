"""Deterministic, restart-safe opportunity selection from the actual schedule."""

import hashlib
import json
import random
import re
from datetime import datetime, timedelta


def schedule_hash(life: dict) -> str:
    raw = json.dumps(
        {key: life.get(key) for key in ("outfit", "schedule", "meta")},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def parse_schedule(text: str) -> list[dict]:
    activities = []
    clock = r"(?<!\d)(\d{1,2})(?:[:：](\d{2})|点(?:(\d{1,2})分?|半)?)(?!\d)"

    def hours(match, prefix):
        hour = int(match[1])
        minute = int(match[2] or match[3] or (30 if "半" in match[0] else 0))
        if any(x in prefix for x in ("下午", "晚上", "傍晚")) and hour < 12:
            hour += 12
        if "凌晨" in prefix and hour == 12:
            hour = 0
        return hour, minute

    for line in re.split(r"[\n；;]", str(text)):
        match = re.search(clock, line)
        if not match:
            continue
        prefix = line[: match.start()]
        hour, minute = hours(match, prefix)
        if hour > 23 or minute > 59:
            continue
        rest = line[match.end() :]
        ending = re.match(r"\s*[-—~～至到]\s*((?:上午|中午|下午|晚上|傍晚|凌晨)?)(.*)", rest)
        end_time = None
        if ending:
            end_match = re.match(clock, ending[2])
            if end_match:
                eh, em = hours(end_match, ending[1] or prefix)
                if eh < 24 and em < 60:
                    end_time = f"{eh:02}:{em:02}"
                    rest = ending[2][end_match.end() :]
        activity = re.sub(r"^[\s\-—~～至:：]+", "", rest).strip()
        if activity:
            item = {"time": f"{hour:02}:{minute:02}", "activity": activity[:600]}
            if end_time:
                item["end_time"] = end_time
            activities.append(item)
    return activities


def select_slots(
    activities, now: datetime, fingerprint: str, target: int, settings, *, seed: str = ""
) -> list[dict]:
    """Select distinct activities within today's remaining window; never backfill stale events."""
    if target <= 0:
        return []
    rng = random.Random(f"{seed}:{now.date()}:{fingerprint}")
    candidates = []
    starts = []
    for item in activities:
        try:
            h, m = (int(x) for x in item["time"].split(":"))
            starts.append(now.replace(hour=h, minute=m, second=0, microsecond=0))
        except (KeyError, TypeError, ValueError):
            continue
    for item in activities:
        try:
            h, m = (int(x) for x in item["time"].split(":"))
            at = now.replace(hour=h, minute=m, second=0, microsecond=0)
        except (KeyError, TypeError, ValueError):
            continue
        activity = str(item.get("activity", "")).strip()
        if not activity or any(x in activity for x in ("睡觉", "入睡", "睡眠")):
            continue
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        deadline = midnight + timedelta(hours=settings.active_end)
        following = [start for start in starts if start > at]
        if following:
            deadline = min(deadline, min(following))
        delay_max = 20
        if item.get("end_time"):
            try:
                eh, em = (int(x) for x in item["end_time"].split(":"))
                end = at.replace(hour=eh, minute=em)
                if end <= at:
                    continue
                deadline = min(deadline, end)
            except (TypeError, ValueError):
                continue
        delay_max = min(delay_max, int((deadline - at).total_seconds() // 60) - 1)
        if delay_max < 1:
            continue
        at += timedelta(minutes=rng.randint(min(5, delay_max), delay_max))
        if not settings.active_start <= at.hour < settings.active_end or at < now:
            continue
        candidates.append((at, activity, min(deadline, at + timedelta(hours=1))))
    rng.shuffle(candidates)
    chosen = []
    for at, activity, deadline in candidates:
        if all(
            abs((at - old[0]).total_seconds()) >= settings.min_gap_minutes * 60 for old in chosen
        ):
            chosen.append((at, activity, deadline))
            if len(chosen) >= target:
                break
    return [
        {
            "plan_id": f"{now.date()}:{fingerprint}:{at.hour:02}{at.minute:02}",
            "day": str(now.date()),
            "schedule_hash": fingerprint,
            "slot_at": at.timestamp(),
            "expires_at": deadline.timestamp(),
            "activity": activity,
            "status": "pending",
        }
        for at, activity, deadline in sorted(chosen)
    ]


def daily_target(day: str, bot_id: str, minimum: int, maximum: int) -> int:
    return random.Random(f"qzone:{bot_id}:{day}").randint(minimum, maximum)
