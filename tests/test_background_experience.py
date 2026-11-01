"""Exercise silent dependencies, durable sharing decisions and incident logging."""

from datetime import timedelta
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

import test_service as fixtures

from astrbot_plugin_qzone_life.core.events import BackgroundDeliveryBlocked, SilentGenerationEvent


class BackgroundExperienceTests(IsolatedAsyncioTestCase):
    setUp = fixtures.ServiceWorkflowTests.setUp
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    seed_due_plan = fixtures.ServiceWorkflowTests.seed_due_plan

    async def test_dependency_attempted_chat_delivery_is_blocked_and_retried_silently(self):
        self.seed_due_plan("silent-dependency", 60)
        self.event.send = AsyncMock()

        async def noisy_dependency(event, prompt):
            await event.send("图片没法发出去了")
            return self.photo, {}

        self.integrations.selfie.side_effect = noisy_dependency
        for attempt in range(3):
            await self.service.run_autonomous(self.event, self.binding)
            self.now += timedelta(minutes=10)
        self.event.send.assert_not_awaited()
        self.assertEqual(self.integrations.selfie.await_count, 3)
        self.assertEqual(self.store.get_plan("silent-dependency")["status"], "skipped")
        self.transport.publish.assert_not_awaited()
        # Ordinary user-requested chat still owns the original send method.
        await self.event.send("正常聊天")
        self.event.send.assert_awaited_once_with("正常聊天")

    async def test_silent_event_blocks_direct_bot_delivery_and_followup_requests(self):
        client = SimpleNamespace(call_action=AsyncMock(), send_private_msg=AsyncMock())
        original = SimpleNamespace(bot=client, request_llm=Mock(), send=AsyncMock())
        silent = SilentGenerationEvent(original)
        with self.assertRaises(BackgroundDeliveryBlocked):
            await silent.bot.call_action("send_private_msg", user_id=42, message="failed")
        with self.assertRaises(BackgroundDeliveryBlocked):
            await silent.bot.send_private_msg(user_id=42, message="failed")
        with self.assertRaises(BackgroundDeliveryBlocked):
            silent.request_llm(prompt="report failure")
        with self.assertRaises(BackgroundDeliveryBlocked):
            silent.set_extra("_gitee_bg_completion", True)
        client.call_action.assert_not_awaited()
        original.request_llm.assert_not_called()
        await silent.bot.call_action("get_login_info")
        client.call_action.assert_awaited_once_with("get_login_info")

    async def test_role_can_skip_opportunity_without_generating_a_photo(self):
        self.seed_due_plan("nothing", 60)
        self.service.prepare_share.return_value = ({}, {"share": False, "focus": ""})
        await self.service.run_autonomous(self.event, self.binding)
        await self.service.run_autonomous(self.event, self.binding)
        self.service.prepare_share.assert_awaited_once()
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()
        plan = self.store.get_plan("nothing")
        self.assertEqual((plan["status"], plan["last_error_kind"]), ("skipped", "nothing_to_share"))

    async def test_sharing_context_survives_image_failure_without_changing_intent(self):
        self.seed_due_plan("focus", 60)
        context = {"evidence": [{"id": "chat:1", "text": "喜欢蓝色"}], "recent_posts": []}
        self.service.prepare_share.return_value = (
            context, {"share": True, "focus": "蓝色外套与午后咖啡", "related_evidence": ["chat:1"]}
        )
        self.integrations.selfie.side_effect = [RuntimeError("image unavailable"), (self.photo, {})]
        await self.service.run_autonomous(self.event, self.binding)
        self.now += timedelta(minutes=10)
        await self.service.run_autonomous(self.event, self.binding)
        self.service.prepare_share.assert_awaited_once()
        self.assertEqual(self.store.get_plan("focus")["cached_sharing_context"], context)
        self.assertIn("蓝色外套与午后咖啡", self.integrations.selfie.await_args.args[1])
        self.assertEqual(self.store.get_plan("focus")["status"], "done")

    async def test_schedule_change_during_intent_stops_old_photo(self):
        self.seed_due_plan("changed", 60)

        async def change(*args):
            self.integrations.life["outfit"] = "新的穿搭"
            return {}, {"share": True, "focus": "旧计划"}

        self.service.prepare_share.side_effect = change
        await self.service.run_autonomous(self.event, self.binding)
        self.integrations.selfie.assert_not_awaited()
        self.assertEqual(self.store.get_plan("changed")["status"], "cancelled")

    def test_repeated_incident_is_coalesced_and_recovery_is_recorded_once(self):
        with patch.object(fixtures.service_module, "logger") as logger:
            for _ in range(25):
                self.service.background_error("schedule", "日程同步", TypeError("internal fault"))
            logger.warning.assert_called_once()
            self.assertTrue(logger.warning.call_args.kwargs["exc_info"])
            self.service.background_recovered("schedule", "日程同步")
            self.service.background_recovered("schedule", "日程同步")
            logger.info.assert_called_once_with("%s已恢复", "日程同步")
            self.service.background_error("schedule", "日程同步", TypeError("a new fault"))
            self.assertEqual(logger.warning.call_count, 2)
