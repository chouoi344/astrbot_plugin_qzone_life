"""Actual image bytes, SDK-shaped enums, and schedule-boundary regressions."""

from __future__ import annotations

import base64
import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core import media
from astrbot_plugin_qzone_life.core.planning import (
    daily_target,
    normalize_intervals,
    parse_schedule,
    plan_matches_schedule,
    select_slots,
)
from astrbot_plugin_qzone_life.core.settings import Settings


class ComponentType(str, Enum):
    """Use the SDK's str/Enum semantics without installing or faking AstrBot."""

    Image = "Image"
    Reply = "Reply"
    Plain = "Plain"


def image_bytes(color, *, image_format="JPEG"):
    with Image.new("RGB", (12, 8), color) as picture:
        buffer = io.BytesIO()
        picture.save(buffer, format=image_format)
        return buffer.getvalue()


def image_segment(color):
    return {
        "type": "image",
        "data": {"file": "base64://" + base64.b64encode(image_bytes(color)).decode()},
    }


class MediaTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.cache = Path(self.temp.name) / "cache"

    def tearDown(self):
        self.temp.cleanup()

    def event(self, chain, *, raw_reply=None):
        bot = SimpleNamespace(get_msg=AsyncMock(return_value=raw_reply or {}))
        return SimpleNamespace(get_messages=lambda: chain, bot=bot)

    def assert_pixel_color(self, path, expected):
        with Image.open(path) as picture:
            self.assertEqual(picture.format, "PNG")
            self.assertEqual(picture.size, (12, 8))
            # Source JPEG may round RGB values by one or two channels.
            actual = picture.convert("RGB").getpixel((3, 3))
            self.assertTrue(all(abs(a - b) <= 3 for a, b in zip(actual, expected)))

    async def test_sdk_str_enum_image_component_is_recognized(self):
        self.assertNotEqual(str(ComponentType.Image).lower(), "image")
        segment = SimpleNamespace(
            type=ComponentType.Image, url=None, file=image_segment("red")["data"]["file"]
        )
        result = await media.collect_images(self.event([segment]), self.cache)
        self.assertEqual(len(result), 1)
        self.assert_pixel_color(result[0], (255, 0, 0))

    async def test_quoted_photo_takes_priority_over_current_attachment(self):
        reply = SimpleNamespace(type=ComponentType.Reply, id="123", chain=[image_segment("red")])
        event = self.event([reply, image_segment("blue")])
        result = await media.collect_images(event, self.cache)
        self.assertEqual(len(result), 1)
        self.assert_pixel_color(result[0], (255, 0, 0))
        event.bot.get_msg.assert_not_awaited()

    async def test_missing_reply_chain_fetches_original_message_once(self):
        reply = SimpleNamespace(type=ComponentType.Reply, id="123", chain=None)
        event = self.event(
            [reply, image_segment("blue")], raw_reply={"message": [image_segment("red")]}
        )
        result = await media.collect_images(event, self.cache)
        event.bot.get_msg.assert_awaited_once_with(message_id=123)
        self.assert_pixel_color(result[0], (255, 0, 0))

    async def test_onebot_dictionary_reply_can_fetch_original_photo(self):
        event = self.event(
            [{"type": "reply", "data": {"id": "456"}}],
            raw_reply={"message": [image_segment("blue")]},
        )
        result = await media.collect_images(event, self.cache)
        event.bot.get_msg.assert_awaited_once_with(message_id=456)
        self.assert_pixel_color(result[0], (0, 0, 255))

    async def test_base64_jpeg_is_persisted_as_real_normalized_png(self):
        result = await media.collect_images(self.event([image_segment("red")]), self.cache)
        saved = Path(result[0])
        self.assertTrue(saved.is_file())
        self.assertTrue(saved.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
        self.assert_pixel_color(saved, (255, 0, 0))
        second = await media.collect_images(self.event([image_segment("red")]), self.cache)
        self.assertEqual(result, second)
        self.assertEqual(len(list(self.cache.iterdir())), 1)

    async def test_non_image_bytes_are_rejected_before_publication(self):
        segment = {
            "type": "image",
            "data": {"file": "base64://" + base64.b64encode(b"not an image").decode()},
        }
        with self.assertRaises((ValueError, OSError)):
            await media.collect_images(self.event([segment]), self.cache)
        self.assertFalse(list(self.cache.glob("*.png")))

    async def test_image_count_limit_is_checked_before_loading(self):
        with patch.object(media, "read_image", AsyncMock()) as loader:
            with self.assertRaises(ValueError):
                await media.collect_images(
                    self.event([image_segment("red"), image_segment("blue")]), self.cache, limit=1
                )
            loader.assert_not_awaited()

    async def test_quoted_image_limit_never_silently_falls_back_to_attachment(self):
        reply = SimpleNamespace(
            type=ComponentType.Reply, id="1", chain=[image_segment("red"), image_segment("blue")]
        )
        with self.assertRaises(ValueError):
            await media.collect_images(
                self.event([reply, image_segment("green")]), self.cache, limit=1
            )

    async def test_text_only_message_is_rejected(self):
        with self.assertRaises(ValueError):
            await media.collect_images(
                self.event([{"type": "text", "data": {"text": "发空间"}}]), self.cache
            )

    async def test_invalid_base64_and_oversized_input_are_rejected(self):
        with self.assertRaises(ValueError):
            await media.read_image("base64://not-valid%%")
        with patch.object(media, "MAX_BYTES", 32):
            with self.assertRaises(ValueError):
                await media.read_image(image_segment("red")["data"]["file"])

    async def test_percent_encoded_local_file_uri(self):
        path = Path(self.temp.name) / "有 空格.jpg"
        path.write_bytes(image_bytes("blue"))
        result = await media.read_image(path.as_uri())
        self.assertTrue(result.startswith(b"\x89PNG"))


class PlanningTests(unittest.TestCase):
    def test_transient_generation_flag_does_not_change_schedule_identity(self):
        from astrbot_plugin_qzone_life.core.planning import schedule_hash

        life = {"schedule": "10:00-12:00 看书", "outfit": "衬衫", "meta": {"qzone_schedule_day": "2026-10-25"}}
        fingerprint = schedule_hash(life)
        for generating in (True, False):
            life["meta"]["qzone_generation_in_progress"] = generating
            self.assertEqual(schedule_hash(life), fingerprint)
        life["outfit"] = "外套"
        self.assertNotEqual(schedule_hash(life), fingerprint)

    def setUp(self):
        self.settings = Settings()
        self.now = datetime(2026, 10, 24, 7, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    def test_chinese_periods_and_minute_notation(self):
        items = parse_schedule("上午8点半 早餐；下午3点15分 咖啡；晚上7：20 散步")
        self.assertEqual([item["time"] for item in items], ["08:30", "15:15", "19:20"])
        self.assertEqual([item["activity"] for item in items], ["早餐", "咖啡", "散步"])

    def test_ranges_preserve_activity_without_leaking_end_time_into_it(self):
        items = parse_schedule("08:00-09:00 早餐\n下午3点到4点 咖啡店休息\n19:00～20:00 公园散步")
        self.assertEqual([item["time"] for item in items], ["08:00", "15:00", "19:00"])
        self.assertEqual([item["activity"] for item in items], ["早餐", "咖啡店休息", "公园散步"])

    def test_midnight_is_not_misinterpreted_as_noon(self):
        items = parse_schedule("凌晨12点 休息\n中午12点 午餐")
        self.assertEqual([item["time"] for item in items], ["00:00", "12:00"])

    def test_night_twelve_ends_at_midnight_instead_of_next_noon(self):
        items = parse_schedule("晚上11点到12点半 看书")
        self.assertEqual(items, [{"time": "23:00", "end_time": "00:30", "activity": "看书"}])
        now = self.now.replace(hour=3)
        intervals = normalize_intervals(items, now, valid_until=now.replace(hour=7).timestamp())
        self.assertEqual([(row["start"].hour, row["end"].hour, row["end"].minute) for row in intervals], [(0, 0, 30)])
        self.assertEqual(
            select_slots(items, now, "night-twelve", 1, self.settings,
                         valid_until=now.replace(hour=7).timestamp()),
            [],
        )
        for prefix in ("晚上", "午夜", "深夜"):
            self.assertEqual(parse_schedule(prefix + "12点半 看书")[0]["time"], "00:30")

    def test_explicit_next_day_sleep_keeps_its_quiet_boundary(self):
        now = self.now.replace(hour=1, minute=15)
        for prefix in ("次日", "翌日", "第二天", "明日"):
            for period in ("", "上午"):
                with self.subTest(prefix=prefix, period=period):
                    items = parse_schedule(f"晚上11点—{prefix}{period}8点 睡觉\n01:00-02:00 看书")
                    self.assertEqual(items[0], {"time": "23:00", "end_time": "08:00", "activity": "睡觉"})
                    self.assertEqual(select_slots(items, now, "next-day-sleep", 1, self.settings), [])

    def test_invalid_explicit_range_keeps_a_non_shareable_boundary(self):
        for ending in ("次日25:00", "09:90", "??", "次日10:00"):
            with self.subTest(ending=ending):
                items = parse_schedule(f"08:00 早餐\n09:00-{ending} 出门\n10:00-11:00 咖啡")
                self.assertEqual(items[1], {"time": "09:00", "activity": ""})
                intervals = normalize_intervals(items, self.now.replace(hour=9, minute=30))
                self.assertEqual([row["activity"] for row in intervals], ["早餐", "咖啡"])
                self.assertEqual(intervals[0]["end"].hour, 9)

    def test_hyphen_between_single_time_and_activity_is_preserved(self):
        items = parse_schedule("08:00 - 早餐\n09:00 - 咖啡")
        self.assertEqual(items, [{"time": "08:00", "activity": "早餐"}, {"time": "09:00", "activity": "咖啡"}])

    def test_2400_is_only_valid_as_exact_midnight_ending(self):
        items = parse_schedule("23:00-24:00 看书\n24:00 出门\n23:00-24:01 散步")
        self.assertEqual(items, [
            {"time": "23:00", "end_time": "24:00", "activity": "看书"},
            {"time": "23:00", "activity": ""},
        ])
        slots = select_slots(items, self.now, "midnight", 1, self.settings, seed="99")
        self.assertEqual(len(slots), 1)
        midnight = self.now.replace(hour=0) + timedelta(days=1)
        self.assertEqual(slots[0]["expires_at"], midnight.timestamp())
        self.assertEqual(slots[0]["activity"], "看书")

    def test_midnight_ending_still_uses_earlier_next_activity_or_one_hour_limit(self):
        for text, expected_hour in (
            ("21:00-24:00 看书\n21:30 入睡", 21),
            ("21:00-24:00 看书", 22),
        ):
            with self.subTest(text=text):
                slots = select_slots(parse_schedule(text), self.now, "midnight", 1, self.settings)
                self.assertEqual(len(slots), 1)
                deadline = datetime.fromtimestamp(slots[0]["expires_at"], self.now.tzinfo)
                self.assertEqual(deadline.hour, expected_hour)
                if expected_hour == 21:
                    self.assertEqual(deadline.minute, 30)
                else:
                    self.assertEqual(slots[0]["expires_at"] - slots[0]["slot_at"], 3600)

    def test_hidden_retry_configuration_uses_builtin_policy(self):
        settings = Settings.from_config(
            {
                "auto_retry_count": 99,
                "auto_retry_first_minutes": 500,
                "auto_retry_second_minutes": -1,
            }
        )
        self.assertEqual(settings.auto_retry_count, 2)
        self.assertEqual(settings.auto_retry_first_minutes, 10)
        self.assertEqual(settings.auto_retry_second_minutes, 30)

    def test_invalid_time_does_not_become_a_post_opportunity(self):
        self.assertEqual(parse_schedule("25:00 出门\n14:99 咖啡\n没有时间的活动"), [])

    def test_zero_remaining_daily_target_selects_nothing(self):
        result = select_slots(
            [{"time": "10:00", "activity": "咖啡"}], self.now, "hash", 0, self.settings
        )
        self.assertEqual(result, [])

    def test_selection_is_bounded_spaced_and_stable_across_restarts(self):
        activities = [
            {"time": hour, "activity": "活动" + hour}
            for hour in ("08:00", "08:30", "12:00", "18:00", "23:00")
        ]
        slots = select_slots(activities, self.now, "hash", 2, self.settings, seed="99")
        self.assertEqual(
            slots, select_slots(activities, self.now, "hash", 2, self.settings, seed="99")
        )
        self.assertEqual(len(slots), 2)
        self.assertGreaterEqual(
            slots[1]["slot_at"] - slots[0]["slot_at"], self.settings.min_gap_minutes * 60
        )
        for item in slots:
            timestamp = datetime.fromtimestamp(item["slot_at"], self.now.tzinfo)
            self.assertGreaterEqual(timestamp, self.now)
            self.assertEqual(timestamp.date(), self.now.date())

    def test_expired_activities_are_not_backfilled(self):
        now = self.now.replace(hour=15)
        slots = select_slots(
            [{"time": "08:00", "activity": "早餐"}, {"time": "12:00", "activity": "咖啡"}],
            now,
            "hash",
            2,
            self.settings,
        )
        self.assertEqual(slots, [])

    def test_selected_time_does_not_outlive_explicit_short_activity(self):
        now = self.now.replace(hour=12)
        items = parse_schedule("12:00-12:01 短暂休息")
        slots = select_slots(items, now, "short", 1, self.settings, seed="99")
        for slot in slots:
            self.assertLessEqual(slot["slot_at"], now.replace(minute=1).timestamp())

    def test_plan_keeps_explicit_activity_end_for_restart_recovery(self):
        now = self.now.replace(hour=12)
        slots = select_slots(
            parse_schedule("12:00-12:10 咖啡"), now, "short-ten", 1, self.settings, seed="99"
        )
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["expires_at"], now.replace(minute=10).timestamp())
        self.assertLess(slots[0]["slot_at"], slots[0]["expires_at"])

    def test_next_activity_start_ends_current_activity_even_if_next_is_sleep(self):
        now = self.now.replace(hour=12)
        slots = select_slots(
            parse_schedule("12:00 咖啡\n12:15 入睡"), now, "following", 1, self.settings, seed="99"
        )
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["activity"], "咖啡")
        self.assertEqual(slots[0]["expires_at"], now.replace(minute=15).timestamp())
        self.assertLess(slots[0]["slot_at"], slots[0]["expires_at"])

    def test_real_late_activity_has_no_fixed_2300_cutoff(self):
        now = self.now.replace(hour=22, minute=45)
        slots = select_slots(
            parse_schedule("22:50-24:00 看书"), now, "night", 1, self.settings, seed="99"
        )
        self.assertEqual(len(slots), 1)
        self.assertGreater(slots[0]["expires_at"], now.replace(hour=23, minute=0).timestamp())
        self.assertLessEqual(
            slots[0]["expires_at"], (now.replace(hour=0, minute=0) + timedelta(days=1)).timestamp()
        )

    def test_long_activity_opportunity_expires_after_one_hour(self):
        now = self.now.replace(hour=12)
        slots = select_slots(
            parse_schedule("12:00-16:00 逛展览"), now, "long", 1, self.settings, seed="99"
        )
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["expires_at"] - slots[0]["slot_at"], 3600)

    def test_sleep_and_invalid_entries_are_skipped(self):
        slots = select_slots(
            [{"time": "12:00", "activity": "入睡"}, {"time": "not-time", "activity": "活动"}, {}],
            self.now,
            "hash",
            2,
            self.settings,
        )
        self.assertEqual(slots, [])

    def test_en_dash_arrow_and_overnight_chinese_ranges_parse(self):
        items = parse_schedule("08:00–09:00 早餐\n12:00→14:00 咖啡\n晚上10点到2点 看书")
        self.assertEqual(
            [(i["time"], i["end_time"]) for i in items],
            [("08:00", "09:00"), ("12:00", "14:00"), ("22:00", "02:00")],
        )

    def test_sleep_aliases_and_empty_periods_do_not_create_opportunities(self):
        now = self.now.replace(hour=10)
        for activity in (
            "午睡",
            "午休",
            "就寝",
            "补觉",
            "小憩",
            "打盹",
            "熟睡",
            "睡着",
            "关灯休息",
            "无安排",
            "无日程",
            "待定",
            "留白",
            "空白时段",
        ):
            with self.subTest(activity=activity):
                self.assertEqual(
                    select_slots(
                        parse_schedule(f"12:00-14:00 {activity}"), now, "quiet", 1, self.settings
                    ),
                    [],
                )
        for activity in ("咖啡店休息", "穿睡衣看电影", "睡前阅读"):
            with self.subTest(activity=activity):
                self.assertEqual(
                    len(
                        select_slots(
                            parse_schedule(f"12:00-14:00 {activity}"),
                            now,
                            "awake",
                            1,
                            self.settings,
                        )
                    ),
                    1,
                )

    def test_day_gaps_and_isolated_last_point_stay_empty(self):
        now = self.now.replace(hour=10)
        self.assertEqual(
            select_slots(
                parse_schedule("08:00-09:00 早餐\n12:00 咖啡"), now, "gap", 2, self.settings
            ),
            [],
        )
        plan = {
            "day": str(now.date()),
            "activity": "早餐",
            "slot_at": now.replace(hour=8, minute=30).timestamp(),
            "expires_at": now.replace(hour=11).timestamp(),
        }
        self.assertFalse(
            plan_matches_schedule(plan, parse_schedule("08:00-09:00 早餐\n12:00-13:00 午餐"), now)
        )

    def test_timed_empty_rows_preserve_the_boundary_and_gap(self):
        now = self.now.replace(hour=12, minute=30)
        activities = parse_schedule("11:00 咖啡\n12:00-14:00\n18:00-19:00 晚餐")
        self.assertEqual(activities[1], {"time": "12:00", "end_time": "14:00", "activity": ""})
        slots = select_slots(activities, now, "empty-boundary", 2, self.settings)
        self.assertTrue(all(slot["activity"] != "咖啡" for slot in slots))
        old = {
            "day": str(now.date()),
            "slot_at": now.replace(hour=11, minute=30).timestamp(),
            "expires_at": now.replace(hour=13).timestamp(),
            "activity": "咖啡",
        }
        self.assertFalse(plan_matches_schedule(old, activities, now))

    def test_decorated_empty_labels_stay_quiet(self):
        now = self.now.replace(hour=12)
        for label in ("【无安排】", "（空白）", "**待定**", "`留白`", "[暂无日程]"):
            with self.subTest(label=label):
                self.assertEqual(
                    select_slots(
                        parse_schedule(f"12:00-14:00 {label}"),
                        now,
                        "decorated-blank",
                        1,
                        self.settings,
                    ),
                    [],
                )

    def test_explicit_quiet_intervals_override_overlapping_later_activities(self):
        now = self.now.replace(hour=13)
        for label in ("午睡", "无安排", ""):
            with self.subTest(label=label):
                activities = parse_schedule(f"12:00-14:00 {label}\n13:00-15:00 看书")
                self.assertEqual(
                    select_slots(activities, now, "quiet-overlap", 1, self.settings), []
                )
                old = {
                    "day": str(now.date()),
                    "slot_at": now.replace(minute=5).timestamp(),
                    "expires_at": now.replace(hour=14, minute=5).timestamp(),
                    "activity": "看书",
                }
                self.assertFalse(plan_matches_schedule(old, activities, now.replace(minute=10)))

    def test_late_start_can_use_the_remainder_of_an_ongoing_activity(self):
        now = self.now.replace(hour=15)
        activities = parse_schedule("12:00-17:00 看展览")
        slots = select_slots(activities, now, "ongoing", 1, self.settings, seed="99")
        self.assertEqual(len(slots), 1)
        self.assertGreater(slots[0]["slot_at"], now.timestamp())
        self.assertLess(slots[0]["slot_at"], now.replace(hour=17).timestamp())
        at = datetime.fromtimestamp(slots[0]["slot_at"], now.tzinfo)
        self.assertTrue(plan_matches_schedule(slots[0], activities, at))
        self.assertFalse(plan_matches_schedule(slots[0], activities, now.replace(hour=17)))

    def test_clear_night_periods_and_current_overnight_segment_are_usable(self):
        now = self.now.replace(hour=0, minute=15)
        for text in ("00:00-02:00 看书", "23:30-01:30 看书"):
            with self.subTest(text=text):
                slots = select_slots(parse_schedule(text), now, "overnight", 1, self.settings)
                self.assertEqual(len(slots), 1)
                self.assertLess(slots[0]["slot_at"], now.replace(hour=2, minute=0).timestamp())
                at = datetime.fromtimestamp(slots[0]["slot_at"], now.tzinfo)
                self.assertTrue(plan_matches_schedule(slots[0], parse_schedule(text), at))
        late = self.now.replace(hour=23, minute=35)
        slots = select_slots(parse_schedule("23:30-01:30 看书"), late, "midnight", 1, self.settings)
        self.assertEqual(len(slots), 1)
        self.assertEqual(
            slots[0]["expires_at"], (late.replace(hour=0, minute=0) + timedelta(days=1)).timestamp()
        )

    def test_source_refresh_boundary_limits_old_schedule_and_final_preflight(self):
        now = self.now.replace(hour=6, minute=30)
        boundary = now.replace(hour=7, minute=0).timestamp()
        activities = parse_schedule("06:00-08:30 看书\n09:00-12:00 出门")
        slots = select_slots(activities, now, "refresh", 2, self.settings, valid_until=boundary)
        self.assertEqual(len(slots), 1)
        self.assertLessEqual(slots[0]["expires_at"], boundary)
        at = datetime.fromtimestamp(slots[0]["slot_at"], now.tzinfo)
        self.assertTrue(plan_matches_schedule(slots[0], activities, at, valid_until=boundary))
        self.assertFalse(
            plan_matches_schedule(slots[0], activities, now.replace(hour=7), valid_until=boundary)
        )
        self.assertEqual(
            select_slots(
                activities,
                now.replace(hour=7),
                "expired-source",
                2,
                self.settings,
                valid_until=boundary,
            ),
            [],
        )

    def test_sleep_boundary_ends_previous_activity_even_without_other_shareable_entries(self):
        now = self.now.replace(hour=1)
        intervals = normalize_intervals(parse_schedule("00:00-03:00 看书\n02:00-08:30 午睡"), now)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals[0]["end"], now.replace(hour=2, minute=0))

    def test_daily_target_stays_in_confirmed_range_and_is_stable(self):
        targets = [daily_target(f"2026-10-{day:02}", "99", 1, 2) for day in range(1, 29)]
        self.assertTrue(all(value in (1, 2) for value in targets))
        self.assertEqual(
            targets, [daily_target(f"2026-10-{day:02}", "99", 1, 2) for day in range(1, 29)]
        )

    def test_schema_defaults_match_effective_settings_when_present(self):
        path = Path(__file__).resolve().parents[1] / "_conf_schema.json"
        if not path.exists():
            self.skipTest("configuration schema not yet generated")
        schema = json.loads(path.read_text(encoding="utf-8"))
        defaults = vars(Settings())
        fields = {}

        def inspect_fields(value):
            if not isinstance(value, dict):
                return
            for key, definition in value.items():
                if key in defaults and isinstance(definition, dict) and "default" in definition:
                    fields[key] = definition["default"]
                if isinstance(definition, dict):
                    inspect_fields(definition.get("items", {}))
                    inspect_fields(definition.get("properties", {}))

        inspect_fields(schema)
        self.assertTrue(fields, "schema has no Settings defaults")
        effective = vars(Settings.from_config(fields))
        for key in fields:
            with self.subTest(field=key):
                self.assertEqual(effective[key], defaults[key])


if __name__ == "__main__":
    unittest.main()
