"""Bounded comment reads with durable interaction-aware scheduling.

List responses are useful observations, never proof that all nested comments
were included.  Each watched post therefore retains its own detail deadline.
Only a newly observed interaction advances activity; polling never does.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time

from .request_policy import QzoneDeferred
from .settings import admin_ids
from .transport import AccountIdentityError, QzonePostDeletedError


ACTIVE_SECONDS = 2 * 3600
INACTIVE_SECONDS = 24 * 3600
ACTIVE_INTERVAL = 180
NORMAL_INTERVAL = 900
INACTIVE_INTERVAL = 3600
LIST_INTERVAL = 900
DETAILS_PER_ROUND = 3
READ_BUDGET_SECONDS = 15
REPLIES_PER_ROUND = 2


def _timestamp(value, fallback=0.0):
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return fallback
    return number if math.isfinite(number) and number >= 0 else fallback


def _post_key(post):
    return f"comment_poll:{post['binding']['bot_id']}:{post['tid']}"


def post_reported_deleted(store, post):
    """Whether the upstream explicitly reported deletion, without inventing a local delete."""
    return store.get_value(_post_key(post), {}).get("remote_deleted_observed_at") is not None


def _list_key(binding):
    owner = json.dumps(
        [binding.get(key, "") for key in ("bot_id", "persona_id", "umo", "scope")],
        ensure_ascii=False,
    )
    return "comment_list:" + hashlib.sha256(owner.encode()).hexdigest()[:24]


def _save_errors(store, key, errors, *, clear_empty=False):
    """Publish fixed diagnostics without treating an idle tick as recovery."""
    if store.get_value(key) != errors:
        store.set_value(key, errors)
    if "account" in errors:
        message = "空间连接暂未恢复，已暂停评论检查"
    elif "list" in errors:
        message = "空间列表暂时无法读取，稍后再检查"
    elif any(source.startswith("post:") for source in errors):
        message = "部分说说暂时无法读取，已延后检查"
    elif errors:
        message = "部分评论暂时未能回复，已保留处理记录"
    elif clear_empty:
        message = ""
    else:
        return
    if store.get_value("last_comment_error") != message:
        store.set_value("last_comment_error", message)


def _defer_detail(store, post, now):
    state = _state(store, post, now)
    failures = min(4, int(state.get("read_failures", 0)) + 1)
    state.update(read_failures=failures, next_detail_at=now + min(3600, 300 * 2 ** (failures - 1)))
    store.set_value(_post_key(post), state)


def _state(store, post, now):
    state = store.get_value(_post_key(post))
    if isinstance(state, dict):
        return state
    latest = min(now, _timestamp(post.get("published_at"), now))
    # Preserve prior-version interactions and confirmed replies on upgrade.
    # Local history is bounded; future polls contribute remote observations.
    for comment in store.list_comments(post["tid"], limit=100):
        latest = max(latest, min(now, _timestamp(comment.get("created_at"))))
        if comment.get("status") == "replied":
            latest = max(latest, min(now, _timestamp((comment.get("reply") or {}).get("created_at"))))
    state = {
        "last_interaction_at": latest,
        "next_detail_at": 0.0,
        "last_detail_at": 0.0,
        "detail_initialized": store.get_value("comments_seen:" + post["tid"]) is not None,
        "read_failures": 0,
    }
    store.set_value(_post_key(post), state)
    return state


def interval_for(post, state, now):
    latest = max(
        _timestamp(post.get("published_at")),
        _timestamp(state.get("last_interaction_at")),
    )
    idle = max(0, now - latest)
    if idle >= INACTIVE_SECONDS:
        return INACTIVE_INTERVAL
    return ACTIVE_INTERVAL if idle < ACTIVE_SECONDS else NORMAL_INTERVAL


def note_interaction(store, post, occurred_at=None):
    """Record a genuine new interaction or a confirmed successful own reply."""
    now = time.time()
    state = _state(store, post, now)
    occurred = min(now, _timestamp(occurred_at, now))
    if occurred > _timestamp(state.get("last_interaction_at")):
        state["last_interaction_at"] = occurred
        # A newly found interaction wakes an idle post, but does not duplicate
        # the read that just discovered it.
        deadline = now + interval_for(post, state, now)
        existing = _timestamp(state.get("next_detail_at"))
        if existing:
            state["next_detail_at"] = min(existing, deadline)
        store.set_value(_post_key(post), state)


def invalidate_list(store, binding):
    """Make one subsequent list refresh due after an actual publication."""
    store.set_value(_list_key(binding), {"next_at": 0.0})


def _observe(service, post, detail, binding, now):
    store = service.store
    baseline_key = "comments_seen:" + post["tid"]
    seen = set(store.get_value(baseline_key, []))
    state = _state(store, post, now)
    first_detail = not state["detail_initialized"]
    activated = _timestamp(store.get_value("activated_at", now), now)
    admins = admin_ids(service.context)
    for comment in detail.get("comments", []):
        if not isinstance(comment, dict):
            continue
        uid, cid = str(comment.get("uin", "")), str(comment.get("id", ""))
        if not uid or uid == "0" or not cid or cid == "0":
            continue
        key = f"{post['tid']}:{comment.get('parent_id') or '-'}:{cid}:{uid}"
        if key in seen:
            continue
        created_at = _timestamp(comment.get("created_at"))
        seen.add(key)
        historical_unknown = first_detail and post["source"] == "external" and not created_at
        if not historical_unknown:
            note_interaction(store, post, created_at or now)
        if uid == str(binding["bot_id"]) or uid not in admins:
            continue
        if historical_unknown or (created_at and created_at < activated):
            continue
        store.record_comment(
            {
                "key": key,
                "tid": post["tid"],
                "comment_id": cid,
                "parent_id": comment.get("parent_id"),
                "author_qq": uid,
                "author_name": str(comment.get("nickname", uid)),
                "content": str(comment.get("content", "")),
                "created_at": created_at or now,
                "binding": post["binding"],
                "raw": comment,
            }
        )
    store.set_value(baseline_key, sorted(seen))


async def _read_before_deadline(awaitable, deadline):
    remaining = max(0.001, deadline - time.monotonic())
    return await asyncio.wait_for(awaitable, timeout=remaining)


async def poll_comments(service, event, binding):
    """Observe a small batch, then attempt a bounded number of pending replies.

The service owns a dedicated comment lock; no general action lock should be
held while this routine waits for reads or language-model generation.
"""
    if not service.settings.auto_reply or service.store.get_value("paused", False):
        return
    if not admin_ids(service.context):
        return
    store, now = service.store, time.time()
    deadline = time.monotonic() + READ_BUDGET_SECONDS
    owners = {binding["umo"]: binding}
    list_key = _list_key(binding)
    error_key = list_key + ":errors"
    errors = store.get_value(error_key, {})
    errors = errors if isinstance(errors, dict) else {}
    list_state = store.get_value(list_key, {})
    remote = []
    if now >= _timestamp(list_state.get("next_at")):
        # Claim before awaiting so cancellation/restarts never cause a rapid
        # series of repeated discovery requests.
        store.set_value(list_key, {"next_at": now + LIST_INTERVAL})
        try:
            remote = await _read_before_deadline(service.refresh_posts(event, binding), deadline)
        except (AccountIdentityError, QzoneDeferred):
            errors["account"] = "account_unavailable"
            _save_errors(store, error_key, errors)
            raise
        except Exception:
            errors["list"] = "list_read_failed"
            _save_errors(store, error_key, errors)
            return
        errors.pop("list", None)
        errors.pop("account", None)
        _save_errors(store, error_key, errors, clear_empty=True)
    posts = store.list_posts(
        limit=service.settings.watch_posts,
        bot_id=binding["bot_id"],
        persona_id=binding["persona_id"],
    )
    watched = {}
    for post in posts:
        if (not post_reported_deleted(store, post)
                and await service.comment_owner_is_current(post["binding"], binding, owners)):
            watched[post["tid"]] = post
    removed = False
    for source in tuple(errors):
        if source.startswith("post:"):
            resolved = source.removeprefix("post:") not in watched
        elif source.startswith("reply:"):
            record = store.get_comment(source.removeprefix("reply:"))
            resolved = (
                not record or record["tid"] not in watched
                or record["status"] in ("replied", "cancelled")
                or record["author_qq"] not in admin_ids(service.context)
            )
        else:
            resolved = False
        if resolved:
            errors.pop(source)
            removed = True
    _save_errors(store, error_key, errors, clear_empty=removed)
    # Reuse every included observation; never infer completeness from totals.
    for item in remote or []:
        post = watched.get(str(item.get("tid", "")))
        if post is not None:
            _observe(service, post, item, binding, now)
    due = []
    for post in watched.values():
        state = _state(store, post, now)
        next_at = _timestamp(state.get("next_detail_at"))
        if now >= next_at:
            due.append((next_at, _timestamp(state.get("last_detail_at")), post["tid"], post))
    # Earliest deadlines first ensure the three-read budget does not starve
    # older posts behind the newest posts on every tick.
    due.sort(key=lambda item: item[:3])
    for _, _, _, post in due[:DETAILS_PER_ROUND]:
        if time.monotonic() >= deadline or store.get_value("paused", False):
            break
        state = _state(store, post, now)
        state["next_detail_at"] = now + interval_for(post, state, now)
        store.set_value(_post_key(post), state)
        try:
            detail = await _read_before_deadline(
                (await service.transport(event)).get_post(post["tid"]), deadline
            )
        except QzonePostDeletedError:
            # Preserve publication and memory facts. This is an observation by
            # the upstream, not a deletion performed by this plugin.
            state = _state(store, post, time.time())
            state["remote_deleted_observed_at"] = time.time()
            store.set_value(_post_key(post), state)
            errors.pop("post:" + post["tid"], None)
            _save_errors(store, error_key, errors, clear_empty=True)
            continue
        except (AccountIdentityError, QzoneDeferred):
            errors["account"] = "account_unavailable"
            _save_errors(store, error_key, errors)
            raise
        except asyncio.TimeoutError:
            _defer_detail(store, post, time.time())
            errors["post:" + post["tid"]] = "detail_timeout"
            _save_errors(store, error_key, errors)
            # Windows timers may wake just before monotonic reaches the exact
            # deadline. The expired wait budget still ends this entire batch.
            break
        except Exception:
            _defer_detail(store, post, time.time())
            errors["post:" + post["tid"]] = "detail_read_failed"
            _save_errors(store, error_key, errors)
            continue
        _observe(service, post, detail, binding, time.time())
        completed = time.time()
        state = _state(store, post, completed)
        state.update(
            next_detail_at=completed + interval_for(post, state, completed),
            last_detail_at=completed,
            detail_initialized=True,
            read_failures=0,
        )
        store.set_value(_post_key(post), state)
        errors.pop("post:" + post["tid"], None)
        errors.pop("account", None)
        _save_errors(store, error_key, errors, clear_empty=True)
    # Detection is durable before reply generation, preserving memory events
    # and the original comment even when generation fails.
    candidates = []
    for status in ("pending", "failed"):
        candidates.extend(store.list_comments(
            limit=500, status=status, bot_id=binding["bot_id"], persona_id=binding["persona_id"]
        ))
    candidates.sort(key=lambda item: (_timestamp(item.get("created_at")), item["key"]))
    attempted = 0
    for existing in candidates:
        if attempted >= min(service.settings.max_replies_per_poll, REPLIES_PER_ROUND):
            break
        if store.get_value("paused", False) or not service.settings.auto_reply:
            break
        key = existing["key"]
        if time.time() < _timestamp(store.get_value("reply_retry:" + key, 0)):
            continue
        post = store.get_post(existing["tid"])
        if (not post or post["status"] == "deleted" or post_reported_deleted(store, post)
                or existing["author_qq"] not in admin_ids(service.context)):
            store.set_comment_status(key, "cancelled")
            continue
        if not await service.comment_owner_is_current(post["binding"], binding, owners):
            continue
        attempted += 1
        try:
            await service.reply_to_comment(post, existing)
        except (AccountIdentityError, QzoneDeferred):
            errors["account"] = "account_unavailable"
            _save_errors(store, error_key, errors)
            raise
        except Exception:
            # The service owns attempt counts and terminal reply decisions.
            # Never overwrite its longer backoff or revive a terminal record.
            retry_at = max(time.time() + ACTIVE_INTERVAL, _timestamp(store.get_value("reply_retry:" + key, 0)))
            store.set_value("reply_retry:" + key, retry_at)
            errors["reply:" + key] = "reply_failed"
            _save_errors(store, error_key, errors)
        else:
            current = store.get_comment(key)
            if current and current["status"] in ("replied", "cancelled"):
                errors.pop("reply:" + key, None)
                if current["status"] == "replied":
                    errors.pop("account", None)
                _save_errors(store, error_key, errors, clear_empty=True)
            elif current and current["status"] in ("failed", "failed_exhausted", "uncertain"):
                errors["reply:" + key] = "reply_failed"
                _save_errors(store, error_key, errors)
