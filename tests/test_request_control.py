"""Exercise real transport control with local OneBot/HTTP doubles only."""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core import transport as transport_module
from astrbot_plugin_qzone_life.core.request_policy import QzoneDeferred, RequestPolicy
from astrbot_plugin_qzone_life.core.store import StateStore
from astrbot_plugin_qzone_life.core.transport import (
    AccountIdentityError, AccountUnavailableError, QzonePostDeletedError,
    QzoneReadError, QzoneTransport,
)


class Clock:
    def __init__(self):
        self.now = 10_000.0
        self.waits = []

    def __call__(self):
        return self.now

    async def sleep(self, delay):
        self.waits.append(delay)
        self.now += delay


class Response:
    def __init__(self, result, status=200):
        self.result, self.status = result, status

    async def __aenter__(self):
        if isinstance(self.result, BaseException):
            raise self.result
        return self

    async def __aexit__(self, *args):
        return False

    async def text(self):
        return self.result if isinstance(self.result, str) else json.dumps(self.result)


class HTTP:
    def __init__(self, clock):
        self.clock = clock
        self.responses = []
        self.calls = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.calls.append((self.clock(), method, url, kwargs))
        result = self.responses.pop(0) if self.responses else {"code": 0}
        if isinstance(result, tuple):
            return Response(*result)
        return Response(result)

    async def close(self):
        self.closed = True


class RequestControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = Clock()
        self.store = StateStore(":memory:")
        self.policy = RequestPolicy(self.store, "99", clock=self.clock, sleeper=self.clock.sleep)
        self.client = SimpleNamespace(
            get_status=AsyncMock(return_value={"online": True}),
            get_login_info=AsyncMock(return_value={"user_id": 99, "nickname": "role"}),
            get_cookies=AsyncMock(return_value={"cookies": "uin=o99; skey=a; p_skey=b"}),
            _wsr_api_clients={"99": object()},
        )
        self.temp = tempfile.TemporaryDirectory()
        self.transport = QzoneTransport(
            self.client, "99", policy=self.policy, staging_dir=Path(self.temp.name) / "upload",
        )
        self.http = HTTP(self.clock)
        self.transport._http_session = AsyncMock(return_value=self.http)
        self.time_patch = patch.object(
            transport_module, "time", SimpleNamespace(monotonic=self.clock, time=self.clock),
        )
        self.time_patch.start()

    async def asyncTearDown(self):
        self.time_patch.stop()
        await self.transport.close()
        self.store.close()
        self.temp.cleanup()

    async def test_offline_with_live_websocket_does_not_fetch_cookie_or_http(self):
        self.client.get_status.return_value = {"online": False}
        with self.assertRaises(AccountUnavailableError):
            await self.transport.identity()
        result = await self.transport.like("post")
        self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
        self.assertTrue(result["access_wait"])
        self.client.get_cookies.assert_not_awaited()
        self.assertEqual(self.http.calls, [])

    async def test_rpc_outage_is_distinct_from_changed_login(self):
        self.client.get_login_info.side_effect = OSError("connection unavailable")
        with self.assertRaises(AccountUnavailableError):
            await self.transport.identity()
        self.client.get_cookies.assert_not_awaited()
        self.client.get_login_info.side_effect = None
        self.client.get_login_info.return_value = {"user_id": 100}
        with self.assertRaises(AccountIdentityError) as caught:
            await self.transport.identity()
        self.assertNotIsInstance(caught.exception, AccountUnavailableError)

    async def test_cookie_rpc_outage_is_temporary_but_wrong_cookie_is_identity_conflict(self):
        self.client.get_cookies.side_effect = OSError("connection unavailable")
        with self.assertRaises(AccountUnavailableError):
            await self.transport.identity()
        self.clock.now = self.policy.retry_at + 1
        self.client.get_cookies.side_effect = None
        self.client.get_cookies.return_value = {"cookies": "uin=o100; p_skey=b"}
        with self.assertRaises(AccountIdentityError) as caught:
            await self.transport.identity()
        self.assertNotIsInstance(caught.exception, AccountUnavailableError)

    async def test_temporary_offline_in_write_preflight_keeps_credentials_stage(self):
        check = AsyncMock(side_effect=AccountUnavailableError("QQ 尚未在线"))
        result = await self.transport.like("post", before_write=check)
        self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
        self.assertTrue(result["blocked"])
        self.assertTrue(result["access_wait"])
        self.assertEqual(self.http.calls, [])

    async def test_account_conflict_is_not_reported_as_temporary_access_wait(self):
        self.client.get_login_info.return_value = {"user_id": 100}
        for method, args in (
            ("like", ("post",)), ("delete", ("post",)),
            ("reply", ("post", {"id": "1", "uin": 42}, "reply")),
            ("publish", ("text", [])),
        ):
            with self.subTest(method=method):
                result = await getattr(self.transport, method)(*args)
                self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
                self.assertFalse(result["access_wait"])
                self.assertFalse(result["retryable"])
        self.assertEqual(self.http.calls, [])

    async def test_offline_between_uploads_keeps_temporary_access_wait(self):
        from test_transport import upload_response

        request = self.http.request

        def upload_then_disconnect(*args, **kwargs):
            response = request(*args, **kwargs)
            self.client.get_status.return_value = {"online": False}
            return response

        self.http.request = upload_then_disconnect
        self.http.responses = [upload_response()]
        result = await self.transport.publish("text", [b"first", b"second"])
        self.assertEqual((result["status"], result["stage"]), ("rejected", "upload"))
        self.assertTrue(result["access_wait"])
        self.assertEqual(len(self.http.calls), 1)
        self.assertEqual(self.http.calls[0][2], self.transport.UPLOAD_URL)

    async def test_offline_invalidates_previously_cached_credentials(self):
        await self.transport.identity()
        self.client.get_status.return_value = {"online": False}
        with self.assertRaises(AccountIdentityError):
            await self.transport.list_posts()
        self.assertIsNone(self.transport._credentials)
        self.client.get_cookies.assert_awaited_once()
        self.assertEqual(self.http.calls, [])

    async def test_missing_or_nonboolean_online_is_not_assumed_online(self):
        for value in ({}, {"online": "true"}, {"online": 1}, {"status": "failed", "online": True}):
            with self.subTest(value=value):
                self.client.get_status.return_value = value
                with self.assertRaises(AccountIdentityError):
                    await self.transport.identity()
        self.client.get_cookies.assert_not_awaited()

    async def test_missing_status_rpc_stops_before_cookie(self):
        del self.client.get_status
        with self.assertRaises(AccountIdentityError):
            await self.transport.identity()
        self.client.get_cookies.assert_not_awaited()

    async def test_idle_identity_checks_do_not_refresh_expired_cookie(self):
        for _ in range(61):
            await self.transport.identity()
            self.clock.now += 60
        self.client.get_cookies.assert_awaited_once()
        self.assertEqual(self.http.calls, [])
        self.http.responses = [{"code": 0, "msglist": []}]
        await self.transport.list_posts()
        self.assertEqual(self.client.get_cookies.await_count, 2)

    async def test_expired_cookie_cannot_hide_changed_login_during_identity_check(self):
        await self.transport.identity()
        self.clock.now += 1801
        self.client.get_login_info.return_value = {"user_id": 100}
        with self.assertRaises(AccountIdentityError):
            await self.transport.identity()
        self.assertIsNone(self.transport._credentials)
        self.client.get_cookies.assert_awaited_once()
        self.assertEqual(self.http.calls, [])

    async def test_reconnect_invalidates_cookie_even_if_qq_is_same(self):
        await self.transport.identity()
        self.client._wsr_api_clients = {"99": object()}
        await self.transport.identity()
        self.assertEqual(self.client.get_cookies.await_count, 2)

    async def test_restored_online_state_recovers_without_binding(self):
        self.client.get_status.return_value = {"online": False}
        with self.assertRaises(AccountIdentityError):
            await self.transport.identity()
        self.client.get_status.return_value = {"online": True}
        self.assertEqual((await self.transport.identity())["uin"], 99)
        self.client.get_cookies.assert_awaited_once()

    async def test_bad_credentials_pause_parallel_refresh_attempts(self):
        self.client.get_cookies.return_value = {"cookies": "uin=o100; p_skey=b"}
        with self.assertRaises(AccountIdentityError):
            await self.transport.identity()
        with self.assertRaises(QzoneDeferred):
            await self.transport.identity(force=True)
        self.client.get_cookies.assert_awaited_once()
        self.client.get_cookies.return_value = {"cookies": "uin=o99; p_skey=b"}
        self.clock.now = self.policy.retry_at + 1
        self.assertEqual((await self.transport.identity())["uin"], 99)

    async def test_authentication_failure_stops_remaining_reads_and_refreshes(self):
        self.http.responses = [{"code": -3000}]
        with self.assertRaises(QzoneDeferred) as caught:
            await self.transport.get_post("first")
        self.assertEqual(caught.exception.reason, "authentication")
        for tid in ("second", "third"):
            with self.assertRaises(QzoneDeferred):
                await self.transport.get_post(tid)
        self.assertEqual(len(self.http.calls), 1)
        self.client.get_cookies.assert_awaited_once()
        self.assertEqual(self.policy.retry_at, self.clock.now + 900)
        self.assertIsNone(self.transport._credentials)

    async def test_concurrent_cooldown_observers_keep_valid_cookie(self):
        await self.transport.identity()
        cached = self.transport._credentials
        self.http.responses = [asyncio.TimeoutError()]
        with self.assertRaises(QzoneDeferred):
            await self.transport.get_post("first")
        results = await asyncio.gather(
            self.transport.identity(), self.transport.identity(force=True),
            self.transport.get_post("second"), return_exceptions=True,
        )
        self.assertTrue(all(isinstance(result, QzoneDeferred) for result in results))
        self.assertIs(self.transport._credentials, cached)
        self.client.get_cookies.assert_awaited_once()
        self.assertEqual(len(self.http.calls), 1)
        self.clock.now = self.policy.retry_at + 1
        self.http.responses = [{"code": 0, "msglist": []}]
        await self.transport.list_posts()
        self.client.get_cookies.assert_awaited_once()

    async def test_reconnection_during_cooldown_still_invalidates_cookie(self):
        await self.transport.identity()
        self.policy.failure("network")
        self.client._wsr_api_clients = {"99": object()}
        with self.assertRaises(QzoneDeferred):
            await self.transport.identity()
        self.assertIsNone(self.transport._credentials)
        self.clock.now = self.policy.retry_at + 1
        await self.transport.identity()
        self.assertEqual(self.client.get_cookies.await_count, 2)

    async def test_expired_cookie_is_refreshed_after_cooldown(self):
        await self.transport.identity()
        self.clock.now += 1790
        self.policy.failure("network")
        with self.assertRaises(QzoneDeferred):
            await self.transport.identity()
        self.clock.now = self.policy.retry_at + 1
        await self.transport.identity()
        self.client.get_cookies.assert_awaited_once()
        self.http.responses = [{"code": 0, "msglist": []}]
        await self.transport.list_posts()
        self.assertEqual(self.client.get_cookies.await_count, 2)

    async def test_http_200_service_busy_stops_other_posts_and_writes(self):
        self.http.responses = [{"code": -1, "message": "使用人数过多，请稍后再试"}]
        with self.assertRaises(QzoneDeferred) as caught:
            await self.transport.get_post("first")
        self.assertEqual(caught.exception.reason, "service")
        with self.assertRaises(QzoneDeferred):
            await self.transport.get_post("second")
        self.assertEqual((await self.transport.like("post"))["stage"], "cooldown")
        self.assertEqual(len(self.http.calls), 1)
        self.assertEqual(self.policy.retry_at, self.clock.now + 60)

    async def test_http_200_rate_limit_stops_other_writes(self):
        self.http.responses = [{"code": -403, "message": "操作过于频繁，请稍后再试"}]
        result = await self.transport.delete("post")
        self.assertEqual((result["status"], result["reason"]), ("rejected", "restricted"))
        self.assertTrue(result["retryable"])
        self.assertEqual((await self.transport.like("post"))["stage"], "cooldown")
        self.assertEqual(len(self.http.calls), 1)
        self.assertEqual(self.policy.retry_at, self.clock.now + 900)

    async def test_http_200_busy_write_stays_unknown_without_replay(self):
        self.http.responses = [{"code": -1, "message": "使用人数过多，请稍后再试"}]
        result = await self.transport.publish("text", [])
        self.assertEqual(result["status"], "unknown")
        self.assertTrue(result["blocked"])
        self.assertFalse(result["retryable"])
        self.assertEqual(len(self.http.calls), 1)

    async def test_post_specific_refusal_does_not_cool_down_entire_account(self):
        self.http.responses = [{"code": -403, "message": "无权限"}, {"code": 0}]
        self.assertEqual((await self.transport.delete("post"))["status"], "rejected")
        self.assertEqual(self.policy.retry_at, 0)
        self.assertEqual((await self.transport.like("other"))["status"], "success")

    async def test_explicit_deleted_detail_has_typed_post_local_failure(self):
        for message in ("该说说已删除", "该说说已被删除", "此说说已被删除，请查看其他内容"):
            with self.subTest(message=message):
                self.http.responses = [{"code": -1, "message": message}]
                with self.assertRaises(QzonePostDeletedError):
                    await self.transport.get_post("post")
                self.assertEqual(self.policy.retry_at, 0)

    async def test_missing_or_private_post_is_not_assumed_deleted(self):
        for message in ("该说说不存在", "无权限", "该说说不存在或已被删除", "空间暂时无法访问"):
            with self.subTest(message=message):
                self.http.responses = [{"code": -1, "message": message}]
                with self.assertRaises(QzoneReadError) as caught:
                    await self.transport.get_post("post")
                self.assertNotIsInstance(caught.exception, QzonePostDeletedError)
                self.assertEqual(self.policy.retry_at, 0)

    async def test_deleted_message_in_list_or_success_content_is_not_deletion_signal(self):
        self.http.responses = [{"code": -1, "message": "该说说已删除"}]
        with self.assertRaises(QzoneReadError) as caught:
            await self.transport.list_posts()
        self.assertNotIsInstance(caught.exception, QzonePostDeletedError)
        self.http.responses = [{
            "code": 0, "tid": "post", "uin": 99, "content": "该说说已被删除",
            "message": "该说说已删除",
        }]
        self.assertEqual((await self.transport.get_post("post"))["tid"], "post")

    async def test_http_429_stops_writes_without_replay(self):
        self.http.responses = [({"code": -1}, 429)]
        result = await self.transport.delete("post")
        self.assertEqual(result["status"], "unknown")
        self.assertTrue(result["blocked"])
        later = await self.transport.like("post")
        self.assertEqual((later["status"], later["stage"]), ("rejected", "cooldown"))
        self.assertEqual(len(self.http.calls), 1)

    async def test_timeout_after_write_stays_unknown_and_blocks_next_write(self):
        self.http.responses = [asyncio.TimeoutError()]
        result = await self.transport.publish("text", [])
        self.assertEqual(result["status"], "unknown")
        result = await self.transport.publish("text", [])
        self.assertEqual(result["stage"], "cooldown")
        self.assertEqual(len(self.http.calls), 1)
        self.assertEqual(self.policy.retry_at, self.clock.now + 60)

    async def test_connect_failure_remains_definitely_unsent(self):
        connection = SimpleNamespace(host="hidden", port=443, ssl=True)
        self.http.responses = [aiohttp.ClientConnectorError(connection, OSError("hidden"))]
        result = await self.transport.delete("post")
        self.assertEqual((result["status"], result["stage"]), ("rejected", "connect"))
        self.assertTrue(result["retryable"])
        self.assertNotIn("hidden", str(result))

    async def test_pacing_is_shared_between_all_write_kinds(self):
        self.http.responses = [{"code": 0}, {"code": 0}, {"code": 0}, {"code": 0, "tid": "new"}]
        await self.transport.like("post")
        await self.transport.reply("post", {"id": "1", "uin": 42}, "reply")
        await self.transport.delete("post")
        await self.transport.publish("text", [])
        self.assertEqual([entry[0] for entry in self.http.calls], [10000, 10005, 10010, 10015])
        self.assertEqual(self.policy.counts["http_writes"], 4)

    async def test_preflight_after_wait_can_stop_every_write_kind(self):
        for kind in ("like", "reply", "delete", "publish"):
            with self.subTest(kind=kind):
                self.policy.last_write_at = self.clock()
                before = len(self.http.calls)
                check = AsyncMock(side_effect=ValueError("persona switched"))
                if kind == "reply":
                    result = await self.transport.reply("post", {"id": "1", "uin": 42}, "reply", before_write=check)
                elif kind == "publish":
                    result = await self.transport.publish("text", [], before_publish=check)
                else:
                    result = await getattr(self.transport, kind)("post", before_write=check)
                self.assertEqual(result["stage"], "preflight")
                self.assertEqual(len(self.http.calls), before)
                check.assert_awaited_once()

    async def test_preflight_may_recheck_identity_without_lock_deadlock(self):
        async def check():
            await self.transport.identity()

        result = await asyncio.wait_for(self.transport.like("post", before_write=check), 1)
        self.assertEqual(result["status"], "success")

    async def test_cookie_snapshot_change_does_not_mix_query_and_headers(self):
        await self.transport.identity()

        async def change_cookie():
            self.client.get_cookies.return_value = {"cookies": "uin=o99; p_skey=new"}
            await self.transport.identity(force=True)

        result = await self.transport.delete("post", before_write=change_cookie)
        self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
        self.assertEqual(self.http.calls, [])

    async def test_all_images_prechecked_before_first_upload_and_temp_files_removed(self):
        result = await self.transport.publish("text", [b"valid", b""])
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(self.http.calls, [])
        self.assertEqual(list(self.transport.staging_dir.iterdir()), [])

    async def test_image_snapshot_is_preserved_if_original_changes_during_upload(self):
        from test_transport import upload_response

        path = Path(self.temp.name) / "photo"
        path.write_bytes(b"original")
        original = self.http.request

        def request(*args, **kwargs):
            path.write_bytes(b"changed")
            return original(*args, **kwargs)

        self.http.request = request
        self.http.responses = [upload_response(), upload_response(), {"code": 0, "tid": "new"}]
        result = await self.transport.publish("text", [path, path])
        self.assertEqual(result["status"], "success")
        self.assertEqual([call[3]["data"]["picfile"] for call in self.http.calls[:2]], ["b3JpZ2luYWw=", "b3JpZ2luYWw="])
        self.assertEqual(list(self.transport.staging_dir.iterdir()), [])

    async def test_repeated_network_failure_escalates_and_success_resets(self):
        for delay in (60, 300, 900, 1800, 1800):
            self.http.responses = [asyncio.TimeoutError()]
            with self.assertRaises(QzoneDeferred):
                await self.transport.get_post("post")
            self.assertEqual(self.policy.retry_at - self.clock(), delay)
            self.clock.now = self.policy.retry_at + 1
        self.http.responses = [{"code": 0, "msglist": []}]
        await self.transport.list_posts()
        self.assertEqual((self.policy.reason, self.policy.failures, self.policy.retry_at), ("", 0, 0))

    async def test_cooldown_and_metrics_survive_transport_recreation(self):
        self.policy.failure("restricted")
        rebuilt = RequestPolicy(self.store, "99", clock=self.clock)
        with self.assertRaises(QzoneDeferred):
            rebuilt.check()
        self.assertEqual(rebuilt.snapshot(), self.policy.snapshot())
        other = RequestPolicy(self.store, "100", clock=self.clock)
        other.check()
        self.assertEqual(other.counts, {})

    async def test_cancelled_pacing_wait_never_sends(self):
        self.policy.last_write_at = self.clock()
        self.policy._sleep = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.transport.like("post")
        self.assertEqual(self.http.calls, [])


if __name__ == "__main__":
    unittest.main()
