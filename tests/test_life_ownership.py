"""Old daily caches never become a new persona's life just by changing defaults."""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core.life_ownership import life_is_current, note_persona_change
from astrbot_plugin_qzone_life.core.store import StateStore


class LifeOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "state.sqlite3"
        self.store = StateStore(self.db_path)
        self.zone = ZoneInfo("Asia/Shanghai")
        self.now = datetime(2026, 10, 25, 0, 30, tzinfo=self.zone)
        self.a = {"bot_id": "99", "persona_id": "A", "scope": "session:42"}
        self.b = {**self.a, "persona_id": "B"}

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def life(self, *, day="2026-10-24", activity="A的咖啡店日常", outfit="旧风衣"):
        refresh = datetime.fromisoformat(day).replace(tzinfo=self.zone)
        refresh += timedelta(days=1, hours=7)
        return {
            "schedule": f"00:00-02:00 {activity}",
            "outfit": outfit,
            "meta": {"qzone_schedule_day": day, "qzone_valid_until": refresh.timestamp()},
        }

    def test_unchanged_persona_accepts_its_existing_cache_without_waiting(self):
        cached = self.life()
        for _ in range(3):
            self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        note_persona_change(self.store, self.a, {**self.a, "scope": "shared"}, cached, self.now)
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))

    def test_switching_persona_rejects_same_cache_and_survives_restart(self):
        cached = self.life()
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        self.assertFalse(life_is_current(self.store, self.b, cached, self.now))
        self.store.close()
        self.store = StateStore(self.db_path)
        self.assertFalse(life_is_current(self.store, self.b, cached, self.now))

    def test_switch_without_prior_observation_still_marks_visible_cache_as_old_persona(self):
        cached = self.life()
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        self.assertFalse(life_is_current(self.store, self.b, cached, self.now))

    def test_refreshing_schedule_allows_new_persona_and_keeps_old_owner(self):
        cached = self.life()
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        changed = self.life(activity="B的公园散步", outfit="新的蓝外套")
        self.assertTrue(life_is_current(self.store, self.b, changed, self.now))
        self.assertFalse(life_is_current(self.store, self.b, cached, self.now))
        note_persona_change(self.store, self.b, self.a, changed, self.now)
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        self.assertFalse(life_is_current(self.store, self.a, changed, self.now))

    def test_outfit_only_change_cannot_reassign_old_schedule_to_new_persona(self):
        cached = self.life()
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        changed_outfit = self.life(outfit="B刚换的新衣服")
        self.assertFalse(life_is_current(self.store, self.b, changed_outfit, self.now))
        note_persona_change(self.store, self.b, self.a, changed_outfit, self.now)
        self.assertTrue(life_is_current(self.store, self.a, changed_outfit, self.now))

    def test_generation_started_before_switch_cannot_reassign_its_late_result(self):
        cached = self.life()
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        cached["meta"]["qzone_generation_in_progress"] = True
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        late_result = self.life(activity="A的旧请求刚刚生成的新日程")
        late_result["meta"]["qzone_generation_in_progress"] = False
        self.assertFalse(life_is_current(self.store, self.b, late_result, self.now))
        self.store.close()
        self.store = StateStore(self.db_path)
        self.assertFalse(life_is_current(self.store, self.b, late_result, self.now))
        self.now = self.now.replace(hour=7, minute=1)
        fresh_day = self.life(day="2026-10-25", activity="B的新日程")
        self.assertTrue(life_is_current(self.store, self.b, fresh_day, self.now))

    def test_switch_back_to_known_cache_does_not_clear_inflight_generation_barrier(self):
        a_cache = self.life()
        self.assertTrue(life_is_current(self.store, self.a, a_cache, self.now))
        note_persona_change(self.store, self.a, self.b, a_cache, self.now)
        b_cache = self.life(activity="B已确认的日程")
        self.assertTrue(life_is_current(self.store, self.b, b_cache, self.now))
        note_persona_change(self.store, self.b, self.a, b_cache, self.now)
        self.assertTrue(life_is_current(self.store, self.a, a_cache, self.now))
        a_cache["meta"]["qzone_generation_in_progress"] = True
        note_persona_change(self.store, self.a, self.b, a_cache, self.now)
        self.assertTrue(life_is_current(self.store, self.b, b_cache, self.now))
        late_result = self.life(activity="A的旧请求稍后落盘")
        self.assertFalse(life_is_current(self.store, self.b, late_result, self.now))

    def test_unobserved_transition_cache_does_not_gain_a_false_owner_on_switch_back(self):
        cached = self.life(activity="检测到切换时才首次看见的缓存")
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        self.assertFalse(life_is_current(self.store, self.b, cached, self.now))
        note_persona_change(self.store, self.b, self.a, cached, self.now)
        self.assertFalse(life_is_current(self.store, self.a, cached, self.now))

    def test_new_business_day_cache_is_fresh_even_if_its_text_is_identical(self):
        cached = self.life()
        note_persona_change(self.store, self.a, self.b, cached, self.now)
        self.now = self.now.replace(hour=7, minute=1)
        self.assertTrue(life_is_current(self.store, self.b, self.life(day="2026-10-25"), self.now))

    def test_missing_cache_uses_observed_refresh_clock_not_midnight(self):
        cached = self.life()
        self.assertTrue(life_is_current(self.store, self.a, cached, self.now))
        note_persona_change(self.store, self.a, self.b, {}, self.now)
        self.assertFalse(life_is_current(self.store, self.b, cached, self.now))
        # An unknown cache from the same business day could also be from A.
        self.assertFalse(
            life_is_current(self.store, self.b, self.life(activity="另一份旧安排"), self.now)
        )
        self.now = self.now.replace(hour=7, minute=1)
        self.assertTrue(life_is_current(self.store, self.b, self.life(day="2026-10-25"), self.now))

    def test_missing_cache_after_refresh_waits_for_next_business_day(self):
        self.assertTrue(life_is_current(self.store, self.a, self.life(), self.now))
        self.now = self.now.replace(hour=9)
        note_persona_change(self.store, self.a, self.b, None, self.now)
        self.assertFalse(life_is_current(self.store, self.b, self.life(day="2026-10-25"), self.now))
        self.now = self.now.replace(day=26, hour=7, minute=1)
        self.assertTrue(life_is_current(self.store, self.b, self.life(day="2026-10-26"), self.now))

    def test_missing_cache_without_history_has_bounded_conservative_wait(self):
        note_persona_change(self.store, self.a, self.b, {}, self.now)
        self.assertFalse(life_is_current(self.store, self.b, self.life(), self.now))
        self.assertFalse(life_is_current(self.store, self.b, self.life(day="2026-10-25"), self.now))
        self.now = self.now.replace(day=26, hour=7, minute=1)
        self.assertTrue(life_is_current(self.store, self.b, self.life(day="2026-10-26"), self.now))

    def test_task_holding_previous_binding_cannot_claim_new_cache(self):
        note_persona_change(self.store, self.a, self.b, self.life(), self.now)
        changed = self.life(activity="B的公园散步")
        self.assertFalse(life_is_current(self.store, self.a, changed, self.now))
        self.assertTrue(life_is_current(self.store, self.b, changed, self.now))
        self.assertFalse(
            life_is_current(self.store, self.a, self.life(activity="另一条新缓存"), self.now)
        )
