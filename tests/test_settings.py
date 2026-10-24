"""Simple preferences migrate once; removed knobs never remain hidden controls."""

from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core.settings import Settings, normalize_config


class SimpleSettingsTests(unittest.TestCase):
    def test_schema_has_the_five_experience_choices_and_one_slider(self):
        schema = json.loads(
            (Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            list(schema),
            ["auto_publish", "daily_post_limit", "auto_reply", "llm_provider_id", "failure_policy"],
        )
        self.assertEqual(
            [key for key, item in schema.items() if "slider" in item], ["daily_post_limit"]
        )
        self.assertEqual(schema["daily_post_limit"]["slider"], {"min": 0, "max": 5, "step": 1})
        self.assertTrue(schema["auto_publish"]["default"])
        self.assertTrue(schema["auto_reply"]["default"])
        self.assertEqual(schema["daily_post_limit"]["default"], 2)
        self.assertEqual(schema["llm_provider_id"]["_special"], "select_provider")
        self.assertEqual(schema["llm_provider_id"]["default"], "")
        self.assertEqual(schema["failure_policy"]["options"], ["直接跳过", "稍后重试"])
        self.assertEqual(schema["failure_policy"]["default"], "稍后重试")

    def test_defaults_are_complete_and_saving_normalized_choices_is_idempotent(self):
        config = normalize_config({})
        self.assertEqual(
            config,
            {
                "auto_publish": True,
                "daily_post_limit": 2,
                "auto_reply": True,
                "llm_provider_id": "",
                "failure_policy": "稍后重试",
            },
        )
        self.assertEqual(normalize_config(config), config)

    def test_user_switches_are_preserved_without_coercing_text_to_boolean(self):
        config = normalize_config({"auto_publish": False, "auto_reply": False})
        self.assertFalse(config["auto_publish"])
        self.assertFalse(config["auto_reply"])
        self.assertTrue(normalize_config({"auto_publish": "false"})["auto_publish"])

    def test_single_slider_limits_internal_target_without_a_hidden_minimum(self):
        for limit in (1, 2, 5):
            with self.subTest(limit=limit):
                settings = Settings.from_config({"daily_post_limit": limit, "daily_min": 2})
                self.assertEqual((settings.daily_min, settings.daily_max), (1, limit))

    def test_zero_preserves_manual_and_comment_features_while_disabling_daily_target(self):
        config = normalize_config({"daily_post_limit": 0})
        settings = Settings.from_config(config)
        self.assertEqual(config["daily_post_limit"], 0)
        self.assertEqual((settings.daily_min, settings.daily_max), (0, 0))
        self.assertTrue(settings.auto_publish)
        self.assertTrue(settings.auto_reply)
        self.assertEqual(normalize_config(config), config)

    def test_legacy_maximum_wins_over_an_automatically_inserted_new_default(self):
        legacy = {"daily_min": 1, "daily_max": 1, "daily_post_limit": 2}
        self.assertEqual(normalize_config(legacy)["daily_post_limit"], 1)
        self.assertEqual(Settings.from_config(legacy).daily_max, 1)
        # After migration drops the legacy key, later edits use the new slider.
        migrated = normalize_config(legacy)
        migrated["daily_post_limit"] = 2
        self.assertEqual(Settings.from_config(migrated).daily_max, 2)

    def test_invalid_and_out_of_range_limits_have_bounded_defaults(self):
        for raw, expected in (
            ("1", 1),
            ("0", 0),
            (0, 0),
            (-10, 0),
            (5, 5),
            (50, 5),
            (None, 2),
            ("bad", 2),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(
                    normalize_config({"daily_post_limit": raw})["daily_post_limit"], expected
                )

    def test_hidden_legacy_operational_values_no_longer_change_runtime(self):
        legacy = {
            "admin_commands_only": False,
            "active_start": 8,
            "active_end": 23,
            "min_gap_minutes": 30,
            "auto_retry_first_minutes": 1,
            "auto_retry_second_minutes": 2,
            "poll_seconds": 3600,
            "watch_posts": 1,
            "max_replies_per_poll": 1,
            "selfie_timeout": 60,
            "llm_timeout": 15,
            "request_timeout": 10,
            "cookie_ttl": 60,
            "max_images": 1,
            "persona_id": "old-role",
            "vision_provider_id": "old-image-model",
            "timezone": "America/New_York",
        }
        settings = Settings.from_config(legacy)
        defaults = Settings()
        for field in legacy:
            if field in ("active_start", "active_end"):
                self.assertFalse(hasattr(settings, field))
            else:
                self.assertEqual(getattr(settings, field), getattr(defaults, field), field)
        self.assertEqual(settings.poll_seconds, 60)
        self.assertEqual(settings.selfie_timeout, 600)
        self.assertEqual(
            (settings.auto_retry_first_minutes, settings.auto_retry_second_minutes), (10, 30)
        )

    def test_text_model_choice_and_explicit_empty_choice_are_preserved(self):
        for provider in ("chosen-text-model", ""):
            with self.subTest(provider=provider):
                normalized = normalize_config({"llm_provider_id": provider})
                self.assertEqual(normalized["llm_provider_id"], provider)
                self.assertEqual(Settings.from_config(normalized).llm_provider_id, provider)
                self.assertEqual(normalize_config(normalized), normalized)

    def test_photo_failure_choice_controls_retries_with_fixed_private_timing(self):
        for policy, retries in (("直接跳过", 0), ("稍后重试", 2)):
            with self.subTest(policy=policy):
                settings = Settings.from_config(
                    {
                        "failure_policy": policy,
                        "auto_retry_count": 99,
                        "auto_retry_first_minutes": 1,
                        "auto_retry_second_minutes": 2,
                    }
                )
                self.assertEqual(settings.auto_retry_count, retries)
                self.assertEqual(
                    (settings.auto_retry_first_minutes, settings.auto_retry_second_minutes),
                    (10, 30),
                )

    def test_legacy_retry_disabled_migrates_once_and_explicit_choice_takes_precedence(self):
        for legacy_count in (0, "0"):
            with self.subTest(legacy_count=legacy_count):
                migrated = normalize_config({"auto_retry_count": legacy_count})
                self.assertEqual(migrated["failure_policy"], "直接跳过")
                self.assertEqual(Settings.from_config(migrated).auto_retry_count, 0)
        self.assertEqual(
            Settings.from_config(
                {"auto_retry_count": 0, "failure_policy": "稍后重试"}
            ).auto_retry_count,
            2,
        )
        self.assertEqual(normalize_config({"auto_retry_count": 4})["failure_policy"], "稍后重试")

    def test_invalid_user_text_choices_have_safe_defaults(self):
        normalized = normalize_config({"llm_provider_id": None, "failure_policy": "unknown"})
        self.assertEqual(normalized["llm_provider_id"], "")
        self.assertEqual(normalized["failure_policy"], "稍后重试")

    def test_timezone_follows_host_even_when_legacy_plugin_timezone_is_present(self):
        settings = Settings.from_config(
            {"timezone": "America/New_York"}, {"timezone": "Asia/Taipei"}
        )
        self.assertEqual(settings.timezone, "Asia/Taipei")
        self.assertEqual(Settings.from_config({}, {"timezone": "UTC"}).timezone, "UTC")
        self.assertEqual(Settings.from_config({}, {}).timezone, "Asia/Shanghai")

    def test_invalid_host_timezone_uses_the_builtin_fallback(self):
        self.assertEqual(
            Settings.from_config({}, {"timezone": "invalid/timezone"}).timezone,
            "Asia/Shanghai",
        )

    def test_normalization_is_pure_and_drops_legacy_technical_keys(self):
        original = {
            "auto_publish": False,
            "daily_max": 1,
            "llm_provider_id": "private-route",
            "vision_provider_id": "old-image-model",
            "nested": {"do_not_touch": [1, 2]},
        }
        before = copy.deepcopy(original)
        result = normalize_config(original)
        self.assertEqual(original, before)
        self.assertEqual(
            set(result),
            {"auto_publish", "daily_post_limit", "auto_reply", "llm_provider_id", "failure_policy"},
        )
        self.assertEqual(result["llm_provider_id"], "private-route")
        self.assertNotIn("old-image-model", str(result))


if __name__ == "__main__":
    unittest.main()
