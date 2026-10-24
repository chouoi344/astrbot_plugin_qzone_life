"""Remember observed schedule owners without changing the upstream cache.

Life Scheduler 2.8 does not record which persona generated its cached day.
Changing the chat/default persona therefore cannot by itself authenticate that
same old schedule for the new character. This small ledger survives reloads.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, timedelta


_KEY = "life_cache_ownership:v1"
_KEEP = 32


def _owner(binding):
    return [str(binding.get("bot_id") or ""), str(binding.get("persona_id") or "")]


def _load(store):
    state = store.get_value(_KEY, {})
    if not isinstance(state, dict):
        state = {}
    records = state.get("records", [])
    state["records"] = [item for item in records if isinstance(item, dict)][-_KEEP:]
    return state


def _cache(life):
    if not isinstance(life, dict) or not str(life.get("schedule") or "").strip():
        return None
    meta = life.get("meta") or {}
    if not isinstance(meta, dict):
        return None
    day = str(meta.get("qzone_schedule_day") or "")
    try:
        day = date.fromisoformat(day).isoformat()
    except ValueError:
        day = ""
    # An outfit-only change does not establish that a different character owns
    # the same day's activities. The execution planner separately hashes outfit.
    contents = [day, str(life.get("schedule") or "")]
    fingerprint = hashlib.sha256(json.dumps(contents, ensure_ascii=False).encode()).hexdigest()
    try:
        valid_until = float(meta.get("qzone_valid_until") or 0)
    except (TypeError, ValueError, OverflowError):
        valid_until = 0
    if not math.isfinite(valid_until):
        valid_until = 0
    return {"fingerprint": fingerprint, "day": day, "valid_until": valid_until}


def _remember(state, cache, owner):
    records = [item for item in state["records"] if item.get("fingerprint") != cache["fingerprint"]]
    record = {**cache, "owner": owner}
    state["records"] = [*records, record][-_KEEP:]
    state["last_observed"] = record


def _business_day_to_wait_for(state, now):
    previous = state.get("last_observed") or {}
    valid_until = previous.get("valid_until", 0)
    if isinstance(valid_until, (int, float)) and math.isfinite(valid_until) and valid_until > 0:
        try:
            refresh = datetime.fromtimestamp(valid_until, tz=now.tzinfo)
            boundary = now.replace(
                hour=refresh.hour, minute=refresh.minute, second=0, microsecond=0
            )
            return (now.date() - timedelta(days=1) if now < boundary else now.date()).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    # With no observed refresh clock, waiting past this calendar day's business
    # cache is conservative by at most one extra refresh. Midnight alone never
    # grants permission to reuse a prior persona's cache.
    return now.date().isoformat()


def note_persona_change(store, old_binding, new_binding, life, now):
    """Record a persona transition before replacing the active binding.

    ``now`` is the service's timezone-aware datetime. A scope-only change does
    not invalidate a character's schedule. Historical cache owners never move.
    """
    old_owner, new_owner = _owner(old_binding), _owner(new_binding)
    if old_owner == new_owner or not all(old_owner) or not all(new_owner):
        return
    state = _load(store)
    cache = _cache(life)
    blocked_day = None
    previous_transition = state.get("transition") or {}
    if cache:
        known = next(
            (item for item in state["records"] if item.get("fingerprint") == cache["fingerprint"]),
            None,
        )
        # A cache first seen during the transition may have been generated on
        # either side of it. Do not invent an owner that a later switch-back
        # could trust; only a previously observed ownership remains reusable.
        _remember(state, cache, known["owner"] if known else None)
    meta = life.get("meta", {}) if isinstance(life, dict) else {}
    generating = isinstance(meta, dict) and meta.get("qzone_generation_in_progress") is True
    if not cache or generating:
        blocked_day = _business_day_to_wait_for(state, now)
    if previous_transition.get("blocked_day"):
        blocked_day = max(blocked_day or "", previous_transition["blocked_day"])
    state["active_owner"] = new_owner
    state["transition"] = {"owner": new_owner, "blocked_day": blocked_day}
    store.set_value(_KEY, state)


def life_is_current(store, binding, life, now):
    """Allow a matching known cache, or a fresh cache after the old barrier.

    Valid activity times and expiration are checked by the caller. This gate
    only authenticates cache ownership from observations made by this plugin.
    """
    cache = _cache(life)
    owner = _owner(binding)
    if cache is None or not all(owner):
        return False
    state = _load(store)
    if state.get("active_owner") is not None and state["active_owner"] != owner:
        return False
    known = next(
        (item for item in state["records"] if item.get("fingerprint") == cache["fingerprint"]),
        None,
    )
    transition = state.get("transition") or {}
    if known:
        if known.get("owner") != owner:
            return False
    elif transition.get("owner") == owner and transition.get("blocked_day"):
        day = cache["day"]
        if not day or day <= transition["blocked_day"]:
            return False
    elif transition and transition.get("owner") != owner:
        # A task holding an older binding must not register a new cache after a
        # concurrent persona switch, even before its final binding check.
        return False
    _remember(state, cache, owner)
    state["active_owner"] = owner
    if transition.get("owner") == owner and (
        not transition.get("blocked_day") or cache["day"] > transition["blocked_day"]
    ):
        state.pop("transition", None)
    store.set_value(_KEY, state)
    return True
