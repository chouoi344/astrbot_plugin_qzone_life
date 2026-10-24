"""Autonomous retries use durable state and never send background chat errors."""

import asyncio
from datetime import timedelta
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import test_service as fixtures

from astrbot_plugin_qzone_life.core.autonomy import run_autonomous
from astrbot_plugin_qzone_life.core.store import StateStore


class AutonomyTests(IsolatedAsyncioTestCase):
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    seed_due_plan = fixtures.ServiceWorkflowTests.seed_due_plan
    seed_short_activity_plan = fixtures.ServiceWorkflowTests.seed_short_activity_plan
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown

    def setUp(self):
        fixtures.ServiceWorkflowTests.setUp(self)
        self.event.send = AsyncMock()

    async def run_due(self):
        await run_autonomous(self.service, self.event, self.binding)
        self.event.send.assert_not_awaited()

    async def test_skip_policy_stops_after_first_failure_without_text_or_chat_fallback(self):
        self.seed_due_plan("skip", 60)
        self.service.settings.auto_retry_count = 0
        self.integrations.selfie.side_effect = RuntimeError("image provider failed")
        await self.run_due()
        self.now += timedelta(minutes=30)
        await self.run_due()
        plan = self.store.get_plan("skip")
        self.assertEqual((plan["status"], plan["attempt_count"]), ("skipped", 1))
        self.integrations.selfie.assert_awaited_once()
        self.service.photo_summary.assert_not_awaited()
        self.service.llm.assert_not_awaited()
        self.transport.publish.assert_not_awaited()

    async def test_switching_to_skip_retires_a_persisted_retry_immediately(self):
        self.seed_due_plan("switch-skip", 60)
        self.integrations.selfie.side_effect = RuntimeError("image provider failed")
        await self.run_due()
        self.assertEqual(self.store.get_plan("switch-skip")["status"], "pending")
        self.service.settings.auto_retry_count = 0
        # The old +10 minute deadline has not arrived; the new choice applies now.
        await self.run_due()
        self.assertEqual(self.store.get_plan("switch-skip")["status"], "skipped")
        self.now += timedelta(minutes=30)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()
        self.transport.publish.assert_not_awaited()

    async def test_daily_zero_never_generates_or_publishes(self):
        self.seed_due_plan("zero", 60)
        self.service.settings.daily_min = self.service.settings.daily_max = 0
        await self.run_due()
        self.integrations.selfie.assert_not_awaited()
        self.service.photo_summary.assert_not_awaited()
        self.service.llm.assert_not_awaited()
        self.transport.publish.assert_not_awaited()
        self.assertFalse(self.store.get_value("paused", False))

    async def test_legacy_unpublished_caption_is_regenerated_with_cached_image_and_summary(self):
        self.seed_due_plan("legacy-caption", 60)
        self.store.update_plan(
            "legacy-caption",
            cached_image_path=str(self.photo),
            cached_summary="咖啡店，蓝色外套",
            cached_caption="换了件外套。去喝咖啡。",
        )
        await self.run_due()
        self.integrations.selfie.assert_not_awaited()
        self.service.photo_summary.assert_not_awaited()
        self.service.llm.assert_awaited_once()
        self.assertEqual(self.store.get_post("sent-post")["content"], "今天在咖啡店休息。")

    async def test_legacy_failed_write_keeps_original_payload_and_skips_fragmented_caption(self):
        self.seed_due_plan("legacy-write", 60)
        draft = {
            "content": "换了件外套。去喝咖啡。",
            "images": [str(self.photo)],
            "image_summary": "咖啡店",
            "source": "autonomous",
            "binding": self.binding,
            "local_day": str(self.now.date()),
        }
        self.store.begin_operation("auto:legacy-write", "publish", {"post": draft})
        self.store.mark_operation_failed("auto:legacy-write", "legacy rejected")
        self.store.schedule_plan_retry("legacy-write", "legacy rejected", now=self.now.timestamp())
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.transport.publish.assert_not_awaited()
        self.service.llm.assert_not_awaited()
        self.assertEqual(self.store.get_plan("legacy-write")["status"], "skipped")
        self.assertEqual(
            self.store.get_operation("auto:legacy-write")["payload"]["post"]["content"],
            draft["content"],
        )

    async def test_ten_and_thirty_minute_deadlines_survive_restart_and_exhaust_silently(self):
        self.seed_due_plan("retry", 60)
        first = self.now.timestamp()
        self.integrations.selfie.side_effect = RuntimeError("secret provider URL")
        await self.run_due()
        plan = self.store.get_plan("retry")
        self.assertEqual((plan["attempt_count"], plan["next_attempt_at"]), (1, first + 600))
        self.assertEqual(plan["last_error_kind"], "selfie_failed")
        self.assertNotIn("secret", str(plan))
        self.store.close()
        self.store = StateStore(self.directory / "state.sqlite3")
        self.service.store = self.store
        with patch("astrbot_plugin_qzone_life.core.store.time.time", return_value=first + 300):
            self.store.recover_interrupted()
        self.now += timedelta(minutes=9)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()
        self.now += timedelta(minutes=1)
        await self.run_due()
        plan = self.store.get_plan("retry")
        self.assertEqual((plan["attempt_count"], plan["next_attempt_at"]), (2, first + 1800))
        self.now += timedelta(minutes=20)
        await self.run_due()
        plan = self.store.get_plan("retry")
        self.assertEqual((plan["status"], plan["attempt_count"]), ("skipped", 3))
        self.assertIsNone(plan["next_attempt_at"])
        await self.run_due()
        self.assertEqual(self.integrations.selfie.await_count, 3)
        self.transport.publish.assert_not_awaited()
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)
        self.assertEqual(self.store.due_outbox(), [])

    async def test_vision_retry_reuses_image(self):
        self.seed_due_plan("vision", 60)
        self.service.photo_summary.side_effect = [RuntimeError("vision error"), "照片内容"]
        await self.run_due()
        self.assertEqual(self.store.get_plan("vision")["cached_image_path"], str(self.photo))
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()
        self.assertEqual(self.service.photo_summary.await_count, 2)
        self.assertEqual(self.store.get_plan("vision")["status"], "done")

    async def test_slow_generation_failure_waits_full_ten_minutes_after_failure(self):
        self.seed_due_plan("slow", 60)
        first = self.now.timestamp()

        async def slow_failure(*args):
            self.now += timedelta(minutes=8)
            raise RuntimeError("provider failed after a long wait")

        self.integrations.selfie.side_effect = slow_failure
        await self.run_due()
        saved = self.store.get_plan("slow")
        self.assertEqual(saved["first_attempt_at"], first)
        self.assertEqual(saved["first_failure_at"], first + 480)
        self.assertEqual(saved["next_attempt_at"], first + 1080)
        self.now += timedelta(minutes=9)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()

    async def test_caption_retry_reuses_image_and_summary(self):
        self.seed_due_plan("caption", 60)
        self.service.llm.side_effect = [RuntimeError("caption error"), "重试写好的文案"]
        await self.run_due()
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()
        self.service.photo_summary.assert_awaited_once()
        self.assertEqual(self.service.llm.await_count, 2)
        self.assertEqual(self.store.get_plan("caption")["status"], "done")

    async def test_definite_publish_failure_reuses_all_media_and_immutable_operation(self):
        self.seed_due_plan("publish", 60)
        self.transport.publish.side_effect = [
            self.result(status="rejected"),
            self.result(tid="retry-published"),
        ]
        await self.run_due()
        self.assertEqual(self.store.get_plan("publish")["status"], "pending")
        original = self.store.get_operation("auto:publish")["payload"]
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()
        self.service.photo_summary.assert_awaited_once()
        self.service.llm.assert_awaited_once()
        operation = self.store.get_operation("auto:publish")
        self.assertEqual(operation["payload"], original)
        self.assertEqual((operation["status"], operation["attempts"]), ("done", 2))
        self.assertEqual(self.store.get_plan("publish")["post_tid"], "retry-published")

    async def test_missing_original_image_after_write_is_skipped_without_replacement(self):
        self.seed_due_plan("missing", 60)
        self.transport.publish.return_value = self.result(status="rejected")
        await self.run_due()
        self.photo.unlink()
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.assertEqual(
            self.store.get_plan("missing")["last_error_kind"], "original_image_missing"
        )
        self.integrations.selfie.assert_awaited_once()
        self.transport.publish.assert_awaited_once()

    async def test_unknown_publish_blocks_every_later_opportunity_and_never_counts_success(self):
        self.seed_due_plan("unknown", 60)
        self.seed_due_plan("later", 30)
        self.transport.publish.return_value = self.result(status="unknown")
        await self.run_due()
        self.now += timedelta(minutes=30)
        await self.run_due()
        self.assertEqual(self.store.get_plan("unknown")["status"], "uncertain")
        self.assertEqual(self.store.get_plan("later")["status"], "pending")
        self.transport.publish.assert_awaited_once()
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)
        self.assertEqual(self.store.due_outbox(), [])

    async def test_unknown_manual_publish_for_same_bot_blocks_autonomous_generation(self):
        self.seed_due_plan("blocked", 60)
        self.store.begin_operation(
            "manual",
            "publish",
            {
                "post": {
                    "source": "manual",
                    "binding": self.binding,
                }
            },
        )
        self.store.mark_operation_uncertain("manual", "unknown")
        await self.run_due()
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()

    async def test_other_bots_unknown_publish_does_not_block_current_bot(self):
        self.seed_due_plan("unblocked", 60)
        self.store.begin_operation(
            "manual",
            "publish",
            {
                "post": {
                    "source": "manual",
                    "binding": {**self.binding, "bot_id": "other"},
                }
            },
        )
        self.store.mark_operation_uncertain("manual", "unknown")
        await self.run_due()
        self.assertEqual(self.store.get_plan("unblocked")["status"], "done")

    async def test_changed_outfit_cancels_retry_without_new_generation(self):
        self.seed_due_plan("changed", 60)
        self.integrations.selfie.side_effect = RuntimeError("generation failed")
        await self.run_due()
        self.integrations.life["outfit"] = "运动服"
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.assertEqual(self.store.get_plan("changed")["status"], "cancelled")
        self.assertEqual(self.store.get_plan("changed")["last_error_kind"], "schedule_changed")
        self.assertIsNone(self.store.get_plan("changed")["next_attempt_at"])
        self.integrations.selfie.assert_awaited_once()
        self.transport.publish.assert_not_awaited()

    async def test_short_activity_has_no_retry_after_its_deadline(self):
        plan = self.seed_short_activity_plan()
        self.now = self.now.fromtimestamp(plan["slot_at"], self.now.tzinfo)
        self.integrations.selfie.side_effect = RuntimeError("generation failed")
        await self.run_due()
        saved = self.store.get_plan(plan["plan_id"])
        self.assertEqual(saved["status"], "skipped")
        self.assertEqual(saved["last_error_kind"], "retry_window_expired")

    async def test_pause_during_generation_cancels_without_publishing(self):
        self.seed_due_plan("paused", 60)

        async def generated(*args):
            self.store.set_value("paused", True)
            return self.photo, {"unsafe_provider_metadata": "not saved"}

        self.integrations.selfie.side_effect = generated
        await self.run_due()
        saved = self.store.get_plan("paused")
        self.assertEqual(saved["last_error_kind"], "paused")
        self.assertNotIn("unsafe_provider_metadata", str(saved))
        self.transport.publish.assert_not_awaited()

    async def test_binding_change_during_generation_cancels(self):
        self.seed_due_plan("binding", 60)

        async def generated(*args):
            self.integrations.binding["scope"] = "other"
            return self.photo, {}

        self.integrations.selfie.side_effect = generated
        await self.run_due()
        self.assertEqual(self.store.get_plan("binding")["last_error_kind"], "binding_changed")
        self.transport.publish.assert_not_awaited()

    async def test_success_quota_including_deleted_posts_blocks_generation(self):
        self.seed_due_plan("quota", 60)
        self.seed_post("first", source="autonomous")
        self.seed_post("second", source="autonomous")
        self.store.begin_operation("delete", "delete", {"tid": "second"})
        self.store.complete_delete("delete", "second", self.now.timestamp())
        await self.run_due()
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()

    async def test_recent_deleted_autonomous_post_still_blocks_minimum_gap(self):
        self.seed_due_plan("gap", 60)
        self.seed_post("recent", source="autonomous")
        self.store.begin_operation("delete", "delete", {"tid": "recent"})
        self.store.complete_delete("delete", "recent", self.now.timestamp())
        await self.run_due()
        self.assertEqual(self.store.get_plan("gap")["status"], "pending")
        self.integrations.selfie.assert_not_awaited()

    async def test_two_workers_claim_one_opportunity_once(self):
        self.seed_due_plan("one-worker", 60)
        await asyncio.gather(self.run_due(), self.run_due())
        self.integrations.selfie.assert_awaited_once()
        self.transport.publish.assert_awaited_once()
        self.assertEqual(self.store.get_plan("one-worker")["attempt_count"], 1)

    async def test_admin_removed_during_caption_cancels_without_publication(self):
        self.seed_due_plan("removed-admin", 60)

        async def caption(*args):
            self.context.config["admins_id"] = []
            return "原本准备发布的文案"

        self.service.llm.side_effect = caption
        await self.run_due()
        saved = self.store.get_plan("removed-admin")
        self.assertEqual(
            (saved["status"], saved["last_error_kind"]), ("cancelled", "binding_changed")
        )
        self.transport.publish.assert_not_awaited()

    async def test_midnight_cancels_previous_pending_retry(self):
        self.seed_due_plan("yesterday", 60)
        self.integrations.selfie.side_effect = RuntimeError("generation failed")
        await self.run_due()
        self.now += timedelta(days=1)
        await self.run_due()
        saved = self.store.get_plan("yesterday")
        self.assertEqual((saved["status"], saved["last_error_kind"]), ("cancelled", "day_changed"))

    async def test_interrupt_before_write_retains_retry_budget_and_cache(self):
        self.seed_due_plan("cancelled-worker", 60)
        self.service.photo_summary.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.run_due()
        saved = self.store.get_plan("cancelled-worker")
        self.assertEqual((saved["status"], saved["attempt_count"]), ("pending", 1))
        self.assertEqual(saved["cached_image_path"], str(self.photo))
        self.service.photo_summary.side_effect = None
        self.now += timedelta(minutes=10)
        await self.run_due()
        self.integrations.selfie.assert_awaited_once()
        self.assertEqual(self.store.get_plan("cancelled-worker")["status"], "done")

    async def test_restart_during_caption_recovers_cache_without_reissuing_generation(self):
        self.seed_due_plan("interrupted", 60)
        first = self.now.timestamp()
        self.store.claim_plan("interrupted", now=first, retry_delays=[600, 1800])
        self.store.update_plan(
            "interrupted",
            stage="caption",
            cached_image_path=str(self.photo),
            cached_summary="已识图",
        )
        with patch("astrbot_plugin_qzone_life.core.store.time.time", return_value=first + 20):
            self.store.recover_interrupted()
        self.now += timedelta(minutes=10, seconds=20)
        await self.run_due()
        self.integrations.selfie.assert_not_awaited()
        self.service.photo_summary.assert_not_awaited()
        self.assertEqual(self.store.get_plan("interrupted")["status"], "done")

    async def test_final_preflight_after_upload_cancels_expired_opportunity(self):
        plan = self.seed_short_activity_plan()
        self.now = self.now.fromtimestamp(plan["slot_at"], self.now.tzinfo)

        async def upload_then_check(*args, before_publish=None):
            self.now = self.now.replace(hour=12, minute=10, second=0)
            try:
                await before_publish()
            except RuntimeError:
                return self.result(status="rejected", stage="preflight")
            self.fail("Expired opportunity reached final publication")

        self.transport.publish.side_effect = upload_then_check
        await self.run_due()
        saved = self.store.get_plan(plan["plan_id"])
        self.assertEqual(
            (saved["status"], saved["last_error_kind"]), ("cancelled", "opportunity_expired")
        )
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)
