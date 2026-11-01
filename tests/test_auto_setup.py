"""Automatic administrator/session discovery does not send or create chats."""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from astrbot_plugin_qzone_life.core import auto_setup
from astrbot_plugin_qzone_life.core.dialogue import RequestProblem


class AutoSetupTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {"admins_id": ["42", "43"]}
        self.platform = self.make_platform("qq", "99")
        self.manager = SimpleNamespace(platform_insts=[self.platform])
        self.context = SimpleNamespace(
            get_config=lambda: self.config, platform_manager=self.manager,
            send_message=AsyncMock(), conversation_manager=Mock(),
        )
        self.factory = Mock(side_effect=lambda context, binding: binding)

    def make_platform(self, identity, qq, *, name="aiocqhttp"):
        client = SimpleNamespace(get_login_info=AsyncMock(return_value={"user_id": qq}), send=AsyncMock())
        return SimpleNamespace(
            meta=lambda: SimpleNamespace(id=identity, name=name), get_client=lambda: client,
        )

    def hint(self, *, platform="qq", bot="99", sender="42"):
        return SimpleNamespace(
            unified_msg_origin=f"{platform}:FriendMessage:{sender}",
            get_sender_id=lambda: sender, get_self_id=lambda: bot,
            get_platform_name=lambda: "aiocqhttp",
        )

    async def discover(self, **kwargs):
        return await auto_setup.discover_session(self.context, self.factory, **kwargs)

    async def test_install_with_one_online_qq_uses_first_configured_admin_without_message(self):
        result = await self.discover()
        self.assertEqual(result, {
            "umo": "qq:FriendMessage:42", "bot_id": "99", "admin_id": "42", "admin_name": "管理员",
        })
        self.context.send_message.assert_not_awaited()
        self.context.conversation_manager.assert_not_called()
        self.platform.get_client().send.assert_not_awaited()

    async def test_invalid_admin_entries_skipped_and_host_order_preserved(self):
        self.config["admins_id"] = [None, True, "", "webchat:x", "００４２", "0", "0042", "43", 42, "43"]
        self.assertEqual(auto_setup.configured_admins(self.context), ["43", "42"])
        self.assertEqual((await self.discover())["admin_id"], "43")

    async def test_existing_admin_and_platform_stay_selected(self):
        self.manager.platform_insts.append(self.make_platform("other", "88"))
        previous = {"umo": "qq:FriendMessage:43", "bot_id": "99", "admin_id": "43"}
        self.assertEqual((await self.discover(previous=previous))["umo"], previous["umo"])
        self.manager.platform_insts[1].get_client().get_login_info.assert_not_awaited()

    async def test_removed_admin_automatically_uses_current_first_admin(self):
        previous = {"umo": "qq:FriendMessage:7", "bot_id": "99", "admin_id": "7"}
        self.assertEqual((await self.discover(previous=previous))["umo"], "qq:FriendMessage:42")

    async def test_missing_admin_waits_without_querying_qq(self):
        self.config["admins_id"] = []
        with self.assertRaises(RequestProblem) as caught:
            await self.discover()
        self.assertEqual(caught.exception.code, "admin_required")
        self.platform.get_client().get_login_info.assert_not_awaited()

    async def test_non_admin_hint_cannot_activate_or_discover_account(self):
        with self.assertRaises(PermissionError):
            await self.discover(hint=self.hint(sender="100"))
        self.platform.get_client().get_login_info.assert_not_awaited()

    async def test_admin_hint_selects_platform_but_keeps_configured_primary_user(self):
        self.manager.platform_insts.append(self.make_platform("other", "88"))
        result = await self.discover(hint=self.hint(platform="other", bot="88", sender="43"))
        self.assertEqual(result["umo"], "other:FriendMessage:42")

    async def test_multiple_online_accounts_wait_for_backend_selection(self):
        self.manager.platform_insts.append(self.make_platform("other", "88"))
        with self.assertRaises(RequestProblem) as caught:
            await self.discover()
        self.assertEqual(caught.exception.code, "qq_account_ambiguous")
        self.factory.assert_not_called()

    async def test_offline_and_non_qq_platforms_do_not_block_unique_online_account(self):
        other = self.make_platform("offline", "88")
        other.get_client().get_login_info.side_effect = RuntimeError("private-token-in-url")
        self.manager.platform_insts += [other, self.make_platform("web", "77", name="webchat")]
        self.assertEqual((await self.discover())["bot_id"], "99")

    async def test_empty_reverse_connection_map_is_waiting_without_timeout(self):
        self.platform.get_client()._wsr_api_clients = {}
        with self.assertRaises(RequestProblem) as caught:
            await self.discover()
        self.assertEqual(caught.exception.code, "qq_connection_waiting")
        self.platform.get_client().get_login_info.assert_not_awaited()

    async def test_multiple_accounts_in_one_adapter_are_not_randomly_selected(self):
        self.platform.get_client()._wsr_api_clients = {"99": object(), "88": object()}
        with self.assertRaises(RequestProblem) as caught:
            await self.discover()
        self.assertEqual(caught.exception.code, "qq_account_ambiguous")
        self.factory.assert_not_called()

    async def test_wrapped_response_and_identity_conflicts(self):
        client = self.platform.get_client()
        client.get_login_info.return_value = {"status": "ok", "retcode": 0, "data": {"user_id": 99}}
        self.assertEqual((await self.discover())["bot_id"], "99")
        for raw in (
            {"status": "failed", "retcode": 1, "data": {"user_id": 99}},
            {"status": "ok", "retcode": 1, "data": {"user_id": 99}},
            {"status": "ok", "retcode": False, "data": {"user_id": 99}},
            {"user_id": 99, "self_id": 88}, {"user_id": True},
            {"status": "ok", "data": []}, {"nickname": "99"},
        ):
            with self.subTest(raw=raw):
                client.get_login_info.return_value = raw
                with self.assertRaises(RequestProblem):
                    await self.discover()

    async def test_changed_bot_or_missing_saved_platform_never_switches_account(self):
        previous = {"umo": "qq:FriendMessage:42", "bot_id": "88", "admin_id": "42"}
        with self.assertRaises(RequestProblem) as caught:
            await self.discover(previous=previous)
        self.assertEqual(caught.exception.code, "account_changed")
        previous["umo"] = "removed:FriendMessage:42"
        with self.assertRaises(RequestProblem) as caught:
            await self.discover(previous=previous)
        self.assertEqual(caught.exception.code, "qq_connection_waiting")

    async def test_hint_cannot_replace_saved_bot_and_login_must_match_hint(self):
        previous = {"umo": "qq:FriendMessage:42", "bot_id": "99", "admin_id": "42"}
        with self.assertRaises(RequestProblem):
            await self.discover(previous=previous, hint=self.hint(bot="88"))
        self.platform.get_client().get_login_info.assert_not_awaited()
        with self.assertRaises(RequestProblem) as caught:
            await self.discover(hint=self.hint(bot="88"))
        self.assertEqual(caught.exception.code, "account_changed")

    async def test_manager_get_insts_supported_and_login_wait_is_bounded(self):
        self.manager.get_insts = lambda: [self.platform]

        async def slow():
            await asyncio.sleep(1)

        self.platform.get_client().get_login_info.side_effect = slow
        with patch.object(auto_setup, "_LOGIN_TIMEOUT", 0.01):
            with self.assertRaises(RequestProblem) as caught:
                await self.discover()
        self.assertEqual(caught.exception.code, "qq_connection_waiting")
        self.assertNotIn("private", str(caught.exception.facts))

    async def test_cancellation_propagates(self):
        self.platform.get_client().get_login_info.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.discover()
