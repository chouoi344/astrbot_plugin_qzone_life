"""Fresh installs prepare themselves; host admins remain the only authority."""

from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import test_service as fixtures

RequestProblem = fixtures.service_module.RequestProblem


class AutoSetupWorkflowTests(IsolatedAsyncioTestCase):
    setUp = fixtures.ServiceWorkflowTests.setUp
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    seed_due_plan = fixtures.ServiceWorkflowTests.seed_due_plan

    async def test_fresh_install_background_prepares_without_any_chat_message(self):
        self.store.set_value("binding", None)
        self.service.run_autonomous = AsyncMock()
        self.event.send = AsyncMock()
        with patch.object(fixtures.service_module, "discover_session", return_value=self.event) as discover:
            await self.service.tick()
            await self.service.tick()
        discover.assert_awaited_once()
        self.assertEqual(self.store.get_value("binding")["admin_id"], "42")
        self.transport.identity.assert_awaited_once()
        self.event.send.assert_not_awaited()
        self.assertEqual(self.service.run_autonomous.await_count, 2)

    async def test_ordinary_user_has_no_authority_to_initialize_or_change_session(self):
        self.store.set_value("binding", None)
        outsider = fixtures.FakeEvent(self.bot, sender="777")
        with patch.object(fixtures.service_module, "discover_session", new_callable=AsyncMock) as discover:
            with self.assertRaises(PermissionError):
                await self.service.ensure_ready(outsider)
        discover.assert_not_awaited()
        self.transport.identity.assert_not_awaited()
        self.assertIsNone(self.store.get_value("binding"))

    async def test_existing_session_rechecks_login_and_cookies_then_reuses_short_check(self):
        with patch.object(fixtures.service_module, "discover_session", return_value=self.event) as discover:
            self.assertEqual(await self.service.ensure_ready(), self.binding)
            self.assertEqual(await self.service.ensure_ready(), self.binding)
        discover.assert_awaited_once()
        self.transport.identity.assert_awaited_once_with()

    async def test_admin_removal_adopts_current_admin_and_preserves_historical_ownership(self):
        self.seed_post("old-post")
        self.seed_due_plan("old-plan", 60)
        self.context.config["admins_id"] = ["84"]
        self.integrations.binding["scope"] = "session:84"
        replacement = fixtures.FakeEvent(self.bot, sender="84")
        with patch.object(fixtures.service_module, "discover_session", return_value=replacement):
            ready = await self.service.ensure_ready()
        self.assertEqual(ready["admin_id"], "84")
        self.assertEqual(ready["scope"], "session:84")
        self.assertEqual(self.store.get_plan("old-plan")["status"], "cancelled")
        self.assertEqual(self.store.get_post("old-post")["binding"], self.binding)

    async def test_startup_failure_waits_and_recovers_without_chat_activation(self):
        self.store.set_value("binding", None)
        missing = RequestProblem("qq_waiting", {"result": "QQ尚未连接"}, status="waiting")
        clock = SimpleNamespace(now=100.0)
        with patch.object(fixtures.service_module.time, "monotonic", side_effect=lambda: clock.now), patch.object(
            fixtures.service_module, "discover_session", side_effect=[missing, self.event]
        ) as discover:
            self.assertIsNone(await self.service.ensure_ready(required=False))
            clock.now = 130
            self.assertIsNone(await self.service.ensure_ready(required=False))
            discover.assert_awaited_once()
            clock.now = 161
            self.assertEqual((await self.service.ensure_ready())["admin_id"], "42")
            self.assertEqual(discover.await_count, 2)

    async def test_mismatched_space_identity_leaves_install_unprepared(self):
        self.store.set_value("binding", None)
        self.transport.identity.return_value = {"uin": "another-account"}
        with patch.object(fixtures.service_module, "discover_session", return_value=self.event):
            with self.assertRaises(RequestProblem) as caught:
                await self.service.ensure_ready()
        self.assertEqual(caught.exception.code, "account_changed")
        self.assertIsNone(self.store.get_value("binding"))
        self.transport.publish.assert_not_awaited()
