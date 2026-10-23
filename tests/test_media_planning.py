"""Actual image bytes, SDK-shaped enums, and schedule-boundary regressions."""

from __future__ import annotations

import base64
import io
import json
import sys
import tempfile
import unittest
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core import media
from astrbot_plugin_qzone_life.core.planning import daily_target, parse_schedule, select_slots
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
            self.assertGreaterEqual(timestamp.hour, self.settings.active_start)
            self.assertLess(timestamp.hour, self.settings.active_end)

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

    def test_active_end_is_saved_as_deadline_not_just_checked_at_selection(self):
        now = self.now.replace(hour=22, minute=45)
        slots = select_slots(
            parse_schedule("22:50 看书"), now, "night", 1, self.settings, seed="99"
        )
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["expires_at"], now.replace(hour=23, minute=0).timestamp())
        self.assertLess(slots[0]["slot_at"], slots[0]["expires_at"])

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
