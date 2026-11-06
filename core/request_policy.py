"""Per-account request pacing and persistent, privacy-free failure state."""

from __future__ import annotations

import asyncio
import math
import time


class QzoneDeferred(RuntimeError):
    """No new request should be sent until the account's cooldown expires."""

    def __init__(self, reason: str, retry_at: float):
        self.reason = reason
        self.retry_at = retry_at
        super().__init__("空间访问暂时等待恢复，稍后自动继续")


class RequestPolicy:
    """One lock and failure budget shared by every transport for one account.

    The store contains timing, fixed reason codes and counters only. No URLs,
    response bodies, account credentials or chat content are retained here.
    """

    NETWORK_DELAYS = (60, 300, 900, 1800)

    def __init__(
        self, store=None, account="", *, clock=None, sleeper=None,
        read_interval=1.0, write_interval=5.0,
    ):
        self.store = store
        self.key = "request_policy:" + str(account)
        self._clock = clock or time.time
        self._sleep = sleeper or asyncio.sleep
        self.read_interval = max(0.0, float(read_interval))
        self.write_interval = max(self.read_interval, float(write_interval))
        self.lock = asyncio.Lock()
        saved = store.get_value(self.key, {}) if store is not None else {}
        saved = saved if isinstance(saved, dict) else {}
        self.retry_at = self._number(saved.get("retry_at"))
        self.reason = str(saved.get("reason") or "")
        self.failures = int(self._number(saved.get("failures")))
        self.last_http_at = self._number(saved.get("last_http_at"))
        self.last_write_at = self._number(saved.get("last_write_at"))
        self.counts = {
            str(key): int(self._number(value))
            for key, value in (saved.get("counts") or {}).items()
        } if isinstance(saved.get("counts"), dict) else {}

    @staticmethod
    def _number(value):
        try:
            result = float(value or 0)
            return result if math.isfinite(result) and result >= 0 else 0.0
        except (ValueError, TypeError, OverflowError):
            return 0.0

    def snapshot(self):
        return {
            "retry_at": self.retry_at, "reason": self.reason,
            "failures": self.failures, "last_http_at": self.last_http_at,
            "last_write_at": self.last_write_at, "counts": dict(self.counts),
        }

    def _save(self):
        if self.store is not None:
            self.store.set_value(self.key, self.snapshot())

    def count(self, name):
        self.counts[name] = self.counts.get(name, 0) + 1
        self._save()

    def check(self):
        if self._clock() < self.retry_at:
            raise QzoneDeferred(self.reason, self.retry_at)

    def failure(self, reason="network", *, retry_after=0):
        self.failures += 1
        self.reason = reason
        if reason in ("authentication", "restricted"):
            delay = 900 if self.failures == 1 else 1800
        else:
            delay = self.NETWORK_DELAYS[min(self.failures - 1, len(self.NETWORK_DELAYS) - 1)]
        self.retry_at = max(self.retry_at, self._clock() + delay, self._clock() + retry_after)
        self.counts[reason + "_failures"] = self.counts.get(reason + "_failures", 0) + 1
        self._save()

    def success(self):
        if self.failures or self.retry_at or self.reason:
            self.failures = 0
            self.retry_at = 0.0
            self.reason = ""
            self._save()

    async def wait_turn(self, *, write):
        """Called while holding lock; a cancelled wait sends no HTTP request."""
        self.check()
        now = self._clock()
        target = self.last_http_at + self.read_interval
        if write:
            target = max(target, self.last_write_at + self.write_interval)
        delay = max(0.0, target - now)
        if delay:
            await self._sleep(delay)
        self.check()

    def sent(self, *, write):
        now = self._clock()
        self.last_http_at = now
        if write:
            self.last_write_at = now
        self.counts["http_writes" if write else "http_reads"] = (
            self.counts.get("http_writes" if write else "http_reads", 0) + 1
        )
        self._save()
