"""Integrated quiet recovery and durable reply budgets, without live services."""

import time
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

import test_service as fixtures

from astrbot_plugin_qzone_life.core.transport import QzoneTransport


class RequestWorkflowTests(IsolatedAsyncioTestCase):
    setUp = fixtures.ServiceWorkflowTests.setUp
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    comment = fixtures.ServiceWorkflowTests.comment

    def real_transport(self):
        self.bot.get_cookies = AsyncMock(return_value={"cookies": "uin=o99;p_skey=test-key"})
        self.service.transport_factory = QzoneTransport

    def pending_comment(self):
        post = self.seed_post()
        raw = self.comment()
        record = {
            "key": "post-1:-:10:42", "tid": "post-1", "comment_id": "10",
            "author_qq": "42", "author_name": "管理员", "content": "好看",
            "created_at": time.time(), "binding": self.binding, "raw": raw,
        }
        self.store.record_comment(record)
        return post, self.store.list_comments("post-1")[0]

    async def test_periodic_discovery_reuses_cookie_without_suppressing_identity_checks(self):
        self.real_transport()
        await self.service.ensure_ready()
        first_logins = self.bot.get_login_info.await_count
        for _ in range(4):
            self.service._identity_checked_at = 0
            await self.service.ensure_ready()
        self.bot.get_cookies.assert_awaited_once()
        self.assertGreater(self.bot.get_login_info.await_count, first_logins)
        self.assertGreater(self.bot.get_status.await_count, 0)
        self.assertIsNone(self.service._transport._http)

    async def test_offline_qq_with_live_websocket_never_requests_cookie_or_http(self):
        self.real_transport()
        self.bot.get_status.return_value = {"online": False}
        self.service.run_autonomous = AsyncMock()
        self.assertIsNone(await self.service.ensure_ready(required=False))
        await self.service.tick()
        self.bot.get_cookies.assert_not_awaited()
        self.assertIsNone(self.service._transport._http)
        self.service.run_autonomous.assert_not_awaited()

    async def test_tick_continues_memory_when_account_preparation_is_unavailable(self):
        self.seed_post()
        self.service.ensure_ready = AsyncMock(return_value=None)
        await self.service.tick()
        self.integrations.sync_memory.assert_awaited_once()
        self.transport.like.assert_not_awaited()
        self.assertEqual(self.store.get_outbox("qzone:99:post-1:publish")["status"], "done")

    async def test_account_cooldown_stops_comments_images_and_likes_but_keeps_memory(self):
        self.seed_post()
        self.service.ensure_ready = AsyncMock(return_value=self.binding)
        self.service.poll_comments = AsyncMock()
        self.service.run_autonomous = AsyncMock()
        self.service.request_policy("99").failure("restricted")
        await self.service.tick()
        self.integrations.sync_memory.assert_awaited_once()
        self.transport.like.assert_not_awaited()
        self.service.poll_comments.assert_not_awaited()
        self.service.run_autonomous.assert_not_awaited()

    async def test_three_temporary_reply_failures_end_automatic_attempts(self):
        post, existing = self.pending_comment()
        self.transport.reply.side_effect = None
        self.transport.reply.return_value = self.result(
            status="rejected", retryable=True, stage="connect"
        )
        for _ in range(4):
            await self.service.reply_to_comment(post, existing)
        self.assertEqual(self.transport.reply.await_count, 3)
        self.service.llm.assert_awaited_once()
        self.assertEqual(self.store.list_comments("post-1")[0]["status"], "failed_exhausted")
        self.store.recover_interrupted()
        await self.service.reply_to_comment(post, self.store.list_comments("post-1")[0])
        self.assertEqual(self.transport.reply.await_count, 3)

    async def test_model_outage_has_three_attempts_and_never_sends_reply(self):
        post, existing = self.pending_comment()
        self.service.llm.side_effect = RuntimeError("provider unavailable")
        for _ in range(3):
            with self.assertRaises(RuntimeError):
                await self.service.reply_to_comment(post, existing)
        await self.service.reply_to_comment(post, existing)
        self.assertEqual(self.service.llm.await_count, 3)
        self.transport.reply.assert_not_awaited()
        self.assertEqual(self.store.list_comments("post-1")[0]["status"], "failed_exhausted")

    async def test_presend_cooldown_does_not_spend_reply_attempt_budget(self):
        post, existing = self.pending_comment()
        self.transport.reply.side_effect = None
        self.transport.reply.return_value = self.result(
            status="rejected", stage="cooldown", retryable=True, retry_at=time.time() + 600
        )
        await self.service.reply_to_comment(post, existing)
        self.assertEqual(self.store.get_value("reply_attempts:" + existing["key"]), 0)
        self.assertEqual(self.store.list_comments("post-1")[0]["status"], "failed")

    async def test_permanent_rejection_stops_reply_and_unknown_is_not_replayed(self):
        post, existing = self.pending_comment()
        self.transport.reply.side_effect = None
        self.transport.reply.return_value = self.result(status="unknown")
        await self.service.reply_to_comment(post, existing)
        await self.service.reply_to_comment(post, existing)
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.store.list_comments("post-1")[0]["status"], "uncertain")

    async def test_queue_preflight_rechecks_pause_without_sending_or_consuming_budget(self):
        post, existing = self.pending_comment()
        actual_writes = []

        async def queued(*args, before_write):
            self.store.set_value("paused", True)
            try:
                await before_write()
            except fixtures.service_module.RequestProblem:
                return self.result(status="rejected", stage="preflight")
            actual_writes.append(args)
            return self.result()

        self.transport.reply.side_effect = queued
        await self.service.reply_to_comment(post, existing)
        self.assertEqual(actual_writes, [])
        self.assertEqual(self.store.get_value("reply_attempts:" + existing["key"]), 0)

    async def test_external_deletion_found_during_generation_stops_reply(self):
        post, existing = self.pending_comment()

        async def generated(*args):
            self.store.set_value("comment_poll:99:post-1", {"remote_deleted_observed_at": time.time()})
            return "原本准备回复的内容"

        self.service.llm.side_effect = generated
        await self.service.reply_to_comment(post, existing)
        self.transport.reply.assert_not_awaited()
        self.assertEqual(self.store.get_comment(existing["key"])["status"], "cancelled")
        self.assertEqual(self.store.get_post("post-1")["status"], "published")

    async def test_partial_read_diagnostic_is_not_cleared_by_successful_tick_return(self):
        self.service.ensure_ready = AsyncMock(return_value=self.binding)
        self.service.run_autonomous = AsyncMock()

        async def partial(*_):
            self.store.set_value("last_comment_error", "部分说说暂时无法读取")

        self.service.poll_comments = partial
        await self.service.tick()
        self.assertEqual(self.store.get_value("last_comment_error"), "部分说说暂时无法读取")

    async def test_successful_publication_does_not_wait_for_memory_or_like(self):
        self.integrations.sync_memory.side_effect = RuntimeError("slow memory provider")
        result = await self.service.publish_from_chat(self.event, "测试配文")
        self.assertEqual(result.status, "done")
        self.integrations.sync_memory.assert_not_awaited()
        self.transport.like.assert_not_awaited()
        self.assertTrue(self.service._wake.is_set())
        self.assertEqual(self.store.get_outbox("qzone:99:sent-post:publish")["status"], "pending")

    async def test_deleted_post_does_not_wait_for_memory_compensation(self):
        self.seed_post()
        result = await self.service.delete_post(self.event, "post-1")
        self.assertEqual(result.status, "done")
        self.integrations.sync_memory.assert_not_awaited()
        self.assertTrue(self.service._wake.is_set())
        self.assertEqual(self.store.get_outbox("qzone:99:post-1:delete")["status"], "pending")
