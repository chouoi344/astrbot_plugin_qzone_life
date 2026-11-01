"""Completion-driven sync and recovery without touching the upstream generator."""

from __future__ import annotations

import asyncio
import copy
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core.schedule_sync import LifeSnapshots


class Store:
    def __init__(self):
        self.values = {}
        self.writes = []

    def get_value(self, key, default=None):
        return copy.deepcopy(self.values.get(key, default))

    def set_value(self, key, value):
        self.values[key] = copy.deepcopy(value)
        self.writes.append(key)


class Scheduler:
    def __init__(self):
        self.listeners = []

    def add_listener(self, callback, mask):
        self.listeners.append(callback)

    def remove_listener(self, callback):
        self.listeners.remove(callback)

    def done(self, result=None, job="daily_schedule_gen", exception=None):
        for listener in list(self.listeners):
            listener(types.SimpleNamespace(job_id=job, retval=result, exception=exception))


class SnapshotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        events = types.ModuleType("apscheduler.events")
        events.EVENT_JOB_EXECUTED, events.EVENT_JOB_ERROR = 1, 2
        self.modules = patch.dict(sys.modules, {"apscheduler.events": events})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.now = datetime(2026, 11, 2, 8, tzinfo=timezone.utc)
        self.store = Store()
        self.scheduler = Scheduler()
        self.peer = self.make_peer(self.scheduler)
        self.metadata = types.SimpleNamespace(activated=True, star_cls=self.peer)
        self.context = types.SimpleNamespace(get_registered_star=lambda plugin: self.metadata)
        self.life = self.day()
        self.bridge = types.SimpleNamespace(life_context=AsyncMock(side_effect=lambda: self.life))
        self.snapshots = LifeSnapshots(self.context, self.store, self.bridge, lambda: self.now)
        self.addCleanup(self.snapshots.close)

    def make_peer(self, scheduler):
        return types.SimpleNamespace(
            config={"schedule_time": "07:00"},
            scheduler=types.SimpleNamespace(scheduler=scheduler, timezone=timezone.utc),
            generator=types.SimpleNamespace(_generating=False),
        )

    def day(self):
        return {
            "schedule": "08:00-09:00 公园散步",
            "outfit": "白外套",
            "meta": {
                "qzone_schedule_day": str(self.now.date()),
                "qzone_valid_until": (self.now + timedelta(hours=23)).timestamp(),
            },
        }

    async def test_first_read_is_durable_and_returned_data_is_isolated(self):
        first = await self.snapshots.get()
        first["schedule"] = "调用者修改"
        first["meta"]["qzone_schedule_day"] = "wrong"
        self.assertEqual((await self.snapshots.get())["schedule"], self.life["schedule"])
        self.assertEqual(self.bridge.life_context.await_count, 1)
        self.assertEqual(self.store.values["life_snapshot:v1"]["life"]["schedule"], self.life["schedule"])
        self.assertNotIn("qzone_generation_in_progress", self.life["meta"])

    async def test_completion_syncs_immediately_and_duplicate_content_is_not_written(self):
        await self.snapshots.get()
        self.life["outfit"] = "黑外套"
        self.scheduler.done(types.SimpleNamespace(status="ok"))
        self.assertEqual((await self.snapshots.get())["outfit"], "黑外套")
        writes = self.store.writes.count("life_snapshot:v1")
        self.scheduler.done(types.SimpleNamespace(status="ok"))
        await self.snapshots.get()
        self.assertEqual(self.store.writes.count("life_snapshot:v1"), writes)

    async def test_failed_return_does_not_replace_valid_cache_with_failure_text(self):
        previous = await self.snapshots.get()
        self.scheduler.done(types.SimpleNamespace(status="failed", schedule="生成失败"))
        self.assertEqual(await self.snapshots.get(), previous)

    async def test_unrelated_jobs_are_ignored(self):
        await self.snapshots.get()
        self.scheduler.done(job="unrelated_job")
        await self.snapshots.get()
        self.assertEqual(self.bridge.life_context.await_count, 1)

    async def test_manual_changes_are_reconciled_and_force_checks_before_publish(self):
        await self.snapshots.get()
        self.life["schedule"] = "08:00-09:00 休息"
        self.now += timedelta(seconds=30)
        self.assertIn("公园", (await self.snapshots.get())["schedule"])
        self.now += timedelta(seconds=31)
        self.assertIn("休息", (await self.snapshots.get())["schedule"])
        self.life["outfit"] = "新衣服"
        self.assertEqual((await self.snapshots.get(force=True))["outfit"], "新衣服")

    async def test_expired_day_waits_for_new_data_and_never_extends_yesterday(self):
        await self.snapshots.get()
        self.now += timedelta(days=1)
        self.assertEqual(await self.snapshots.get(), {})
        self.life = self.day()
        self.scheduler.done(types.SimpleNamespace(status="ok"))
        result = await self.snapshots.get()
        self.assertEqual(result["meta"]["qzone_schedule_day"], "2026-11-03")

    async def test_removed_cache_invalidates_snapshot(self):
        await self.snapshots.get()
        self.life = {}
        self.assertEqual(await self.snapshots.get(force=True), {})
        self.assertEqual(self.store.values["life_snapshot:v1"]["life"], {})

    async def test_reload_detaches_old_scheduler_and_attaches_current_instance(self):
        await self.snapshots.get()
        replacement = Scheduler()
        self.metadata.star_cls = self.make_peer(replacement)
        self.life["outfit"] = "新实例衣服"
        self.assertEqual((await self.snapshots.get())["outfit"], "新实例衣服")
        self.assertEqual(self.scheduler.listeners, [])
        self.assertEqual(len(replacement.listeners), 1)
        self.snapshots.close()
        self.assertEqual(replacement.listeners, [])
        self.assertEqual(await self.snapshots.get(), {})

    async def test_generation_flag_is_live_between_snapshot_reads(self):
        await self.snapshots.get()
        self.peer.generator._generating = True
        self.assertTrue((await self.snapshots.get())["meta"]["qzone_generation_in_progress"])
        self.peer.generator._generating = False
        self.assertFalse((await self.snapshots.get())["meta"]["qzone_generation_in_progress"])
        self.assertEqual(self.bridge.life_context.await_count, 1)

    async def test_error_backoff_applies_to_final_checks_but_completion_can_recover(self):
        self.bridge.life_context.side_effect = TypeError("internal source error")
        for force in (False, True, False):
            with self.assertRaisesRegex(TypeError, "internal source error"):
                await self.snapshots.get(force=force)
            self.now += timedelta(seconds=30)
        self.assertEqual(self.bridge.life_context.await_count, 1)
        self.bridge.life_context.side_effect = lambda: self.life
        self.scheduler.done(types.SimpleNamespace(status="ok"))
        self.assertIn("公园", (await self.snapshots.get())["schedule"])
        self.assertEqual(self.store.values["schedule_sync_error"], "")

    async def test_source_replaced_during_read_is_not_accepted(self):
        async def replacement():
            self.metadata.star_cls = self.make_peer(Scheduler())
            return self.life

        self.bridge.life_context.side_effect = replacement
        self.assertEqual(await self.snapshots.get(), {})
        self.assertNotIn("life_snapshot:v1", self.store.values)
        self.bridge.life_context.side_effect = lambda: self.life
        self.assertTrue(await self.snapshots.get())

    async def test_completion_during_read_is_not_lost(self):
        async def notify_during_read():
            self.scheduler.done(types.SimpleNamespace(status="ok"))
            await asyncio.sleep(0)
            return self.life

        self.bridge.life_context.side_effect = notify_during_read
        await self.snapshots.get()
        self.bridge.life_context.side_effect = lambda: self.life
        await self.snapshots.get()
        self.assertEqual(self.bridge.life_context.await_count, 2)


if __name__ == "__main__":
    unittest.main()
