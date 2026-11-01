"""Read-only schedule snapshots, refreshed by completion events and reconciliation.

Only QQ空间's own store is written. The upstream generator and scheduler jobs
remain untouched; manual edits are picked up by a cheap periodic cache read.
"""

from __future__ import annotations

import asyncio
import copy
import math

from .integrations import find_plugin


_KEY = "life_snapshot:v1"
_POLL_SECONDS = 60
_ERROR_RETRY_SECONDS = 300


class LifeSnapshots:
    def __init__(self, context, store, integrations, now):
        self.context = context
        self.store = store
        self.integrations = integrations
        self.now = now
        saved = store.get_value(_KEY, {})
        self._snapshot = saved if isinstance(saved, dict) else {}
        self._source = None
        self._scheduler = None
        self._listener = self._on_event
        self._dirty = True
        self._revision = 0
        self._next_sync_at = 0.0
        self._error = None
        self._closed = False
        self._lock = asyncio.Lock()

    def _on_event(self, event):
        if self._closed or getattr(event, "job_id", None) != "daily_schedule_gen":
            return
        # A normally returned job may contain status='failed'. Never accept its
        # retval as a usable schedule: reconcile the actual saved cache instead.
        # This synchronous listener neither writes data nor starts generation.
        self._revision += 1
        self._dirty = True
        self._next_sync_at = 0.0

    def _detach(self):
        scheduler, self._scheduler = self._scheduler, None
        remove = getattr(scheduler, "remove_listener", None)
        if callable(remove):
            remove(self._listener)

    def _attach(self, peer):
        wrapper = getattr(peer, "scheduler", None)
        scheduler = getattr(wrapper, "scheduler", None)
        config = getattr(peer, "config", {})
        get_config = getattr(config, "get", None)
        clock = get_config("schedule_time") if callable(get_config) else None
        source = (id(peer), id(scheduler), str(clock), str(getattr(wrapper, "timezone", "")))
        if source == self._source:
            return
        self._detach()
        self._source = source
        self._dirty = True
        self._next_sync_at = 0.0
        self._error = None
        add = getattr(scheduler, "add_listener", None)
        if callable(add):
            try:
                from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
            except ImportError:
                # Peers exposing only a cache or public getter can still work.
                # A real Life Scheduler already depends on APScheduler.
                return
            add(self._listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            self._scheduler = scheduler

    def _current(self, peer, at):
        expires = self._snapshot.get("expires_at", 0)
        if not isinstance(expires, (int, float)) or not math.isfinite(expires) or at >= expires:
            return {}
        life = copy.deepcopy(self._snapshot.get("life", {}))
        if not life:
            return {}
        meta = life.setdefault("meta", {})
        # Observe generation even between cache reads, so a persona transition
        # can retain the existing in-flight generation barrier.
        meta["qzone_generation_in_progress"] = (
            getattr(getattr(peer, "generator", None), "_generating", False) is True
        )
        return life

    def _save(self, life, at):
        life = copy.deepcopy(life)
        if not isinstance(life, dict):
            raise ValueError("日程快照格式不兼容")
        if life:
            meta = life.setdefault("meta", {})
            if not isinstance(meta, dict):
                raise ValueError("日程快照元数据格式不兼容")
            meta.pop("qzone_generation_in_progress", None)
            expires = meta.get("qzone_valid_until", at + _POLL_SECONDS)
            if not isinstance(expires, (int, float)) or not math.isfinite(expires):
                raise ValueError("日程有效期格式不兼容")
            if expires <= at:
                life = {}
                expires = 0
        else:
            expires = 0
        saved = {"life": life, "expires_at": expires}
        if saved != self._snapshot:
            self.store.set_value(_KEY, saved)
            self._snapshot = saved

    async def get(self, force=False):
        """Return an isolated current snapshot; force rechecks saved data only.

        Source failures back off even when callers request a final check. The
        original exception is retained for diagnosis; callers can coalesce it.
        """
        async with self._lock:
            if self._closed:
                return {}
            peer = find_plugin(self.context, "astrbot_plugin_life_scheduler")
            self._attach(peer)
            at = self.now().timestamp()
            if self._error is not None and at < self._next_sync_at:
                raise self._error
            if force or self._dirty or at >= self._next_sync_at:
                revision = self._revision
                generating_before = (
                    getattr(getattr(peer, "generator", None), "_generating", False) is True
                )
                try:
                    life = await self.integrations.life_context()
                    # Plugin replacement during an asynchronous read invalidates
                    # the old instance's return value, including its identity.
                    if find_plugin(self.context, "astrbot_plugin_life_scheduler") is not peer:
                        self._dirty = True
                        return {}
                    self._save(life, self.now().timestamp())
                except Exception as exc:
                    self._error = exc
                    self._dirty = False
                    self._next_sync_at = self.now().timestamp() + _ERROR_RETRY_SECONDS
                    self.store.set_value("schedule_sync_error", str(exc)[:500])
                    raise
                self._error = None
                self._dirty = revision != self._revision
                self._next_sync_at = self.now().timestamp() + _POLL_SECONDS
                self.store.set_value("schedule_sync_error", "")
                current = self._current(peer, self.now().timestamp())
                if current and (
                    generating_before
                    or (life.get("meta") or {}).get("qzone_generation_in_progress") is True
                ):
                    current["meta"]["qzone_generation_in_progress"] = True
                return current
            return self._current(peer, at)

    def close(self):
        self._closed = True
        self._detach()
