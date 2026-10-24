"""Deterministic opportunities inside real schedule intervals, including midnight."""

import hashlib
import json
import random
import re
from datetime import datetime, timedelta


def schedule_hash(life: dict) -> str:
    contents = {key: life.get(key) for key in ("outfit", "schedule", "meta")}
    if isinstance(contents["meta"], dict):
        contents["meta"] = {
            key: value for key, value in contents["meta"].items()
            if key != "qzone_generation_in_progress"
        }
    raw = json.dumps(
        contents,
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def parse_schedule(text: str) -> list[dict]:
    activities = []
    clock = r"(?<!\d)(\d{1,2})(?:[:：](\d{1,2})|点(?:(\d{1,2})分?|半)?)(?!\d)"

    def hours(match, prefix):
        hour = int(match[1])
        minute = int(match[2] or match[3] or (30 if "半" in match[0] else 0))
        if hour == 12 and any(x in prefix for x in ("凌晨", "晚上", "傍晚", "午夜", "深夜", "夜里")):
            hour = 0
        elif any(x in prefix for x in ("下午", "晚上", "傍晚")) and hour < 12:
            hour += 12
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
        ending = re.match(
            r"\s*[-—–~～至到→]\s*(次日|翌日|第二天|明日)?\s*"
            r"(上午|中午|下午|晚上|傍晚|凌晨|午夜|深夜|夜里)?\s*(.*)",
            rest,
        )
        end_time = None
        if ending:
            next_day, period, tail = ending.groups()
            end_match = re.match(clock, tail)
            invalid_range = False
            if end_match:
                # "晚上11点到次日8点" ends at 08:00, not 20:00.
                end_prefix = period or ("" if next_day else prefix)
                if (
                    not period and not next_day
                    and any(x in prefix for x in ("晚上", "傍晚"))
                    and int(end_match[1]) < int(match[1])
                ):
                    end_prefix = "凌晨"
                eh, em = hours(end_match, end_prefix)
                valid_clock = (eh < 24 and em < 60) or (eh == 24 and em == 0)
                # The daily planner models intervals shorter than one day.
                # Do not collapse an explicit multi-day interval into today.
                invalid_range = not valid_clock or bool(next_day and eh * 60 + em >= hour * 60 + minute)
                if not invalid_range:
                    end_time = f"{eh:02}:{em:02}"
                    rest = tail[end_match.end() :]
            elif next_day or period or re.match(r"[\d?？]", tail):
                invalid_range = True
            if invalid_range:
                # Preserve the start as a quiet boundary. Dropping the row
                # would extend the previous activity through an unknown period.
                activities.append({"time": f"{hour:02}:{minute:02}", "activity": ""})
                continue
        activity = re.sub(r"^[\s\-—–~～至:：|\]）)]+", "", rest).strip()
        # A timed empty row is still an explicit boundary, not missing data to
        # be filled by the preceding activity.
        item = {"time": f"{hour:02}:{minute:02}", "activity": activity[:600]}
        if end_time:
            item["end_time"] = end_time
        activities.append(item)
    return activities


_SLEEP = re.compile(
    r"睡觉|入睡|睡眠|午睡|午休|就寝|补觉|小憩|打盹|熟睡|睡着|关灯休息|\b(?:sleep|nap|bedtime)\b",
    re.I,
)
_BLANK = re.compile(
    r"^(?:无安排|未安排|暂无安排|没有安排|无日程|暂无日程|没有日程|空白|留白|待定|未定)(?:$|[，。,:：\s（(]|时段|时间|活动|日程)"
)


def _shareable(activity):
    label = activity.strip(" \t\r\n*`_#[]【】()（）「」『』<>")
    return bool(label and not _SLEEP.search(label) and not _BLANK.search(label))


def _minute(value, *, ending=False):
    try:
        hour, minute = (int(part) for part in str(value).split(":"))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour * 60 + minute
        if ending and hour == 24 and minute == 0:
            return 1440
    except (ValueError, TypeError):
        pass
    return None


def normalize_intervals(activities, now: datetime, *, valid_until=None):
    """Clip explicit/current overnight intervals to today and the source refresh.

    A single start may end at the next start on the same calendar day. The last
    isolated point has no proven end and never creates an opportunity.
    """
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = midnight + timedelta(days=1)
    limit = tomorrow
    if valid_until is not None:
        try:
            limit = min(limit, datetime.fromtimestamp(float(valid_until), now.tzinfo))
        except (ValueError, TypeError, OverflowError, OSError):
            return []
    if limit <= midnight:
        return []
    rows = []
    for item in activities if isinstance(activities, (list, tuple)) else []:
        if not isinstance(item, dict):
            continue
        start = _minute(item.get("time"))
        activity = str(item.get("activity", "")).strip()
        explicit = "end_time" in item
        end = _minute(item.get("end_time"), ending=True) if explicit else None
        if start is None or (explicit and (end is None or end == start)):
            continue
        rows.append((start, end, activity))
    starts = sorted({row[0] for row in rows})
    boundaries = sorted(
        midnight + timedelta(days=day, minutes=start) for day in (-1, 0, 1) for start in starts
    )

    def raw_bounds(start, end, offset):
        at = midnight + timedelta(days=offset, minutes=start)
        if end is None:
            following = [minute for minute in starts if minute > start]
            if not following:
                return None
            stop = midnight + timedelta(days=offset, minutes=following[0])
        else:
            stop = midnight + timedelta(days=offset + (end < start), minutes=end)
        return at, stop

    blocked = []
    for start, end, activity in rows:
        if _shareable(activity):
            continue
        for offset in (-1, 0):
            bounds = raw_bounds(start, end, offset)
            if bounds:
                low, high = max(bounds[0], midnight), min(bounds[1], limit)
                if high > low:
                    # Keep the explicit quiet deadline even if an overlapping
                    # activity begins earlier. Quiet intervals take precedence.
                    blocked.append((low, high))
    result = []
    seen = set()
    for start, end, activity in rows:
        if not _shareable(activity):
            continue
        for offset in (-1, 0):
            bounds = raw_bounds(start, end, offset)
            if not bounds:
                continue
            at, stop = bounds
            following = [boundary for boundary in boundaries if boundary > at]
            if following:
                stop = min(stop, following[0])
            low, high = max(at, midnight), min(stop, limit)
            if any(low < quiet_end and high > quiet_start for quiet_start, quiet_end in blocked):
                continue
            identity = (low, high, activity)
            if high > low and identity not in seen:
                result.append({"start": low, "end": high, "activity": activity})
                seen.add(identity)
    return sorted(result, key=lambda row: (row["start"], row["end"], row["activity"]))


def select_slots(
    activities,
    now: datetime,
    fingerprint: str,
    target: int,
    settings,
    *,
    seed: str = "",
    valid_until=None,
) -> list[dict]:
    """Choose within the still-current part of real activities; never fill gaps."""
    if target <= 0:
        return []
    rng = random.Random(f"{seed}:{now.date()}:{fingerprint}")
    candidates = []
    for interval in normalize_intervals(activities, now, valid_until=valid_until):
        low = max(interval["start"], now)
        high = interval["end"]
        delay_max = min(20, int((high - low).total_seconds() // 60) - 1)
        if delay_max < 1:
            continue
        at = low + timedelta(minutes=rng.randint(min(5, delay_max), delay_max))
        candidates.append((at, interval, min(high, at + timedelta(hours=1))))
    rng.shuffle(candidates)
    chosen = []
    for at, interval, deadline in candidates:
        if all(
            abs((at - old[0]).total_seconds()) >= settings.min_gap_minutes * 60 for old in chosen
        ):
            chosen.append((at, interval, deadline))
            if len(chosen) >= target:
                break
    return [
        {
            "plan_id": f"{now.date()}:{fingerprint}:{at.hour:02}{at.minute:02}",
            "day": str(now.date()),
            "schedule_hash": fingerprint,
            "slot_at": at.timestamp(),
            "expires_at": deadline.timestamp(),
            "activity_start": interval["start"].timestamp(),
            "activity_end": interval["end"].timestamp(),
            "activity": interval["activity"],
            "status": "pending",
        }
        for at, interval, deadline in sorted(chosen, key=lambda row: row[0])
    ]


def plan_matches_schedule(plan, activities, now: datetime, *, valid_until=None) -> bool:
    """Revalidate cached plans with the exact same intervals used by selection."""
    try:
        slot = float(plan["slot_at"])
        expiry = float(plan.get("expires_at", slot + 3600))
        current = now.timestamp()
        if plan["day"] != str(now.date()) or not slot <= current < expiry:
            return False
        for interval in normalize_intervals(activities, now, valid_until=valid_until):
            start, end = interval["start"].timestamp(), interval["end"].timestamp()
            if (
                str(plan.get("activity", "")) == interval["activity"]
                and start <= slot <= current < end
                and expiry <= end
            ):
                return True
    except (KeyError, TypeError, ValueError):
        return False
    return False


def daily_target(day: str, bot_id: str, minimum: int, maximum: int) -> int:
    return random.Random(f"qzone:{bot_id}:{day}").randint(minimum, maximum)
