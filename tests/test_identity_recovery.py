"""Account recovery needs no chat activation and never reassigns old work."""

import copy
import time
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import test_service as fixtures

from astrbot_plugin_qzone_life.core.transport import AccountIdentityError, QzoneTransport
from astrbot_plugin_qzone_life.core.autonomy import _quota_full, _too_close


class IdentityRecoveryTests(IsolatedAsyncioTestCase):
    setUp = fixtures.ServiceWorkflowTests.setUp
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    seed_due_plan = fixtures.ServiceWorkflowTests.seed_due_plan

    def real_identity(self, *, login="99", cookie="99"):
        self.bot.get_login_info.return_value = {"user_id": login}
        self.bot._wsr_api_clients = {login: object()}
        self.bot.get_cookies = AsyncMock(return_value={
            "cookies": f"uin=o{cookie}; skey=fake-key; p_skey=fake-key",
        })
        self.service.transport_factory = QzoneTransport

    async def test_stale_saved_account_is_automatically_replaced_only_after_cookie_check(self):
        self.seed_post("old-account-post")
        self.seed_due_plan("old-account-plan", 60)
        old_post = self.store.get_post("old-account-post")
        old_memory = self.store.get_outbox("qzone:99:old-account-post:publish")
        fingerprint = self.service.plan_fingerprint(self.integrations.life)
        self.real_identity(login="100", cookie="100")

        ready = await self.service.ensure_ready()

        self.assertEqual((ready["bot_id"], ready["admin_id"]), ("100", "42"))
        self.assertEqual(self.store.get_value("binding")["bot_id"], "100")
        self.assertEqual(self.store.get_post("old-account-post"), old_post)
        self.assertEqual(self.store.get_outbox("qzone:99:old-account-post:publish"), old_memory)
        self.assertEqual(self.store.get_plan("old-account-plan")["status"], "cancelled")
        self.assertNotEqual(self.service.plan_fingerprint(self.integrations.life), fingerprint)
        self.assertIsNone(self.service._transport._http)

    async def test_conflicting_cookie_stops_background_then_recovers_without_message(self):
        self.real_identity(login="100", cookie="99")
        self.service.poll_comments = AsyncMock()
        self.service.run_autonomous = AsyncMock()
        self.service.process_outbox = AsyncMock()
        clock = SimpleNamespace(time=time.time, monotonic=lambda: 100)
        with patch.object(fixtures.service_module, "time", clock):
            await self.service.tick()
            calls = self.bot.get_cookies.await_count
            await self.service.tick()
        self.assertEqual(self.bot.get_cookies.await_count, calls)
        self.assertEqual(self.store.get_value("binding"), self.binding)
        self.service.poll_comments.assert_not_awaited()
        self.service.run_autonomous.assert_not_awaited()
        self.bot.get_cookies.return_value = {"cookies": "uin=o100; p_skey=fake-key"}
        clock.monotonic = lambda: 161
        policy = self.service.request_policy("100")
        recovered_at = policy.retry_at + 1
        policy._clock = lambda: recovered_at
        with patch.object(fixtures.service_module, "time", clock):
            await self.service.tick()
        self.assertEqual(self.store.get_value("binding")["bot_id"], "100")
        self.service.run_autonomous.assert_awaited_once()
        self.assertIsNone(self.service._setup_error)

    async def test_same_client_account_change_rebuilds_transport_and_cancels_old_tasks(self):
        self.real_identity()
        await self.service.ensure_ready()
        old_transport = self.service._transport
        self.seed_due_plan("old-plan", 60)
        self.bot.get_login_info.return_value = {"user_id": "100"}
        self.bot._wsr_api_clients = {"100": object()}
        self.bot.get_cookies.return_value = {"cookies": "uin=o100; p_skey=fake-key"}
        # A normal request forces recognition even inside the short idle TTL.
        event = fixtures.FakeEvent(self.bot, self_id="100")
        ready = await self.service.ensure_ready(event)
        self.assertEqual(ready["bot_id"], "100")
        self.assertIsNot(self.service._transport, old_transport)
        self.assertEqual(self.service._transport.expected_uin, 100)
        self.assertEqual(self.store.get_plan("old-plan")["status"], "cancelled")
        with self.assertRaises(fixtures.service_module.RequestProblem):
            await self.service.verify_binding(self.event, self.binding)

    async def test_extra_connection_after_setup_pauses_instead_of_choosing_an_account(self):
        self.real_identity()
        await self.service.ensure_ready()
        self.bot._wsr_api_clients["100"] = object()
        self.service.run_autonomous = AsyncMock()
        self.service.process_outbox = AsyncMock()
        # Even a recently checked account is revalidated by the real read path.
        await self.service.tick()
        self.service.run_autonomous.assert_not_awaited()
        self.assertEqual(self.store.get_value("binding"), self.binding)
        self.assertIsNotNone(self.service._setup_error)
        self.assertIsNone(self.service._transport._http)

    async def test_account_change_during_preflight_prevents_external_write(self):
        self.real_identity()
        await self.service.ensure_ready()
        self.bot.get_login_info.return_value = {"user_id": "100"}
        self.bot._wsr_api_clients = {"100": object()}
        with self.assertRaises(AccountIdentityError):
            await self.service.verify_binding(self.event, self.binding)
        self.assertEqual(self.store.get_value("binding"), self.binding)
        self.assertIsNone(self.service._transport._http)

    async def test_idle_tick_resolves_persona_once_and_next_tick_sees_change(self):
        self.service.poll_comments = AsyncMock()
        self.service.process_outbox = AsyncMock()
        self.service.ensure_plan = AsyncMock(return_value=None)
        original = self.integrations.resolve_binding
        self.integrations.resolve_binding = AsyncMock(side_effect=original)
        await self.service.tick()
        self.assertEqual(self.integrations.resolve_binding.await_count, 1)
        self.integrations.resolve_binding.reset_mock()
        self.integrations.binding["persona_id"] = "new-role"
        await self.service.tick()
        self.assertEqual(self.integrations.resolve_binding.await_count, 1)
        self.assertEqual(self.store.get_value("binding")["persona_id"], "new-role")

    async def test_final_preflight_still_detects_persona_change_after_idle_check(self):
        ready = copy.deepcopy(await self.service.ensure_ready())
        self.integrations.binding["persona_id"] = "new-role"
        with self.assertRaises(ValueError):
            await self.service.verify_binding(self.event, ready)
        self.transport.publish.assert_not_awaited()
        self.transport.reply.assert_not_awaited()

    async def test_ordinary_request_keeps_human_admin_separate_from_bot_account(self):
        self.real_identity()
        ready = await self.service.ensure_ready(self.event)
        self.assertEqual(ready["admin_id"], "42")
        self.assertEqual(ready["bot_id"], "99")
        self.assertEqual(self.context.config["admins_id"], ["42"])

    async def test_old_account_posts_do_not_consume_new_account_quota_or_interval(self):
        self.seed_post("old-auto-1", source="autonomous")
        self.seed_post("old-auto-2", source="autonomous")
        self.real_identity(login="100", cookie="100")
        ready = await self.service.ensure_ready()
        day = str(self.now.date())
        self.assertEqual(self.store.count_autonomous(day, "99"), 2)
        self.assertEqual(self.store.count_autonomous(day, "100"), 0)
        self.assertFalse(_quota_full(self.service, ready))
        self.assertFalse(_too_close(self.service, ready))
        self.assertTrue(_quota_full(self.service, self.binding))
        self.assertTrue(_too_close(self.service, self.binding))
        self.assertEqual(self.store.count_autonomous(day), 2)


