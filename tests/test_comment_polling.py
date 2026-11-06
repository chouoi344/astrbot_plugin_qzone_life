"""Interaction clocks, bounded reads and durable adaptive comment polling."""

from __future__ import annotations

import asyncio
import copy
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import test_service as fixtures

from astrbot_plugin_qzone_life.core import comment_polling as polling
from astrbot_plugin_qzone_life.core.request_policy import QzoneDeferred
from astrbot_plugin_qzone_life.core.store import StateStore
from astrbot_plugin_qzone_life.core.transport import (
    AccountIdentityError, QzonePostDeletedError, QzoneReadError,
)


class AdaptiveCommentTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    comment = fixtures.ServiceWorkflowTests.comment

    def setUp(self):
        fixtures.ServiceWorkflowTests.setUp(self)
        self.clock = self.now.timestamp()
        clock = SimpleNamespace(time=lambda: self.clock, monotonic=time.monotonic)
        self.clock_patch = patch.object(polling, "time", clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)

    async def poll(self):
        await polling.poll_comments(self.service, self.event, self.binding)

    def state(self, tid="post-1"):
        return self.store.get_value("comment_poll:99:" + tid)

    def intervals(self, tid="post-1"):
        post = self.store.get_post(tid)
        state = polling._state(self.store, post, self.clock)
        return polling.interval_for(post, state, self.clock)

    async def test_no_interaction_uses_publication_and_exact_24h_boundary(self):
        self.seed_post(seconds_ago=86399)
        self.assertEqual(self.intervals(), 900)
        self.clock += 1
        self.assertEqual(self.intervals(), 3600)
        await self.poll()
        self.assertEqual(self.state()["next_detail_at"], self.clock + 3600)

    async def test_active_two_hour_boundary_and_empty_reads_do_not_extend_it(self):
        self.seed_post(seconds_ago=7199)
        await self.poll()
        self.assertEqual(self.intervals(), 180)
        original = self.state()["last_interaction_at"]
        self.clock += 180
        await self.poll()
        self.assertEqual(self.intervals(), 900)
        self.assertEqual(self.state()["last_interaction_at"], original)

    async def test_known_old_comment_and_repeated_scan_never_make_post_active_now(self):
        self.seed_post(seconds_ago=2 * 86400)
        comment = self.comment(uin=77)
        comment["created_at"] = self.clock - 86401
        self.remote_details["post-1"]["comments"] = [comment]
        await self.poll()
        self.assertEqual(self.intervals(), 3600)
        original = self.state()["last_interaction_at"]
        self.clock += 3600
        await self.poll()
        self.assertEqual(self.state()["last_interaction_at"], original)
        self.assertEqual(self.store.list_comments("post-1"), [])

    async def test_new_non_admin_nested_interaction_wakes_old_post_without_reply(self):
        self.seed_post(seconds_ago=2 * 86400)
        await self.poll()
        self.clock += 3600
        comment = self.comment("3", uin=77, parent_id="1", parent_uin=42)
        comment["created_at"] = self.clock
        self.remote_details["post-1"]["comments"] = [comment]
        await self.poll()
        self.assertEqual(self.intervals(), 180)
        self.assertEqual(self.state()["last_interaction_at"], self.clock)
        self.transport.reply.assert_not_awaited()

    async def test_successful_reply_resets_activity_but_failed_reply_does_not(self):
        post = self.seed_post(seconds_ago=2 * 86400)
        await self.poll()
        self.assertEqual(self.intervals(), 3600)
        polling.note_interaction(self.store, post, self.clock)
        self.assertEqual(self.intervals(), 180)
        self.assertEqual(self.state()["next_detail_at"], self.clock + 180)
        self.clock += 86400
        # A failed reply has no confirmed interaction callback.
        self.assertEqual(self.intervals(), 3600)

    async def test_idle_cadence_survives_store_reopen(self):
        self.seed_post(seconds_ago=2 * 86400)
        await self.poll()
        self.store.close()
        self.store = StateStore(self.directory / "state.sqlite3")
        self.service.store = self.store
        self.clock += 60
        await self.poll()
        self.transport.get_post.assert_awaited_once()
        self.transport.list_posts.assert_awaited_once()
        self.assertEqual(self.state()["next_detail_at"], self.clock - 60 + 3600)

    async def test_slow_read_exhausts_budget_and_is_cancelled_before_next_post(self):
        self.seed_post("a")
        self.seed_post("b")
        cancelled = asyncio.Event()

        async def slow_read(tid):
            try:
                await asyncio.sleep(60)
            finally:
                cancelled.set()

        self.transport.get_post.side_effect = slow_read
        with patch.object(polling, "READ_BUDGET_SECONDS", 0.02):
            await self.poll()
        self.assertTrue(cancelled.is_set())
        self.transport.get_post.assert_awaited_once_with("a")
        self.assertEqual(self.state("b")["next_detail_at"], 0)

    async def test_uses_list_comments_even_when_details_are_not_due(self):
        self.seed_post(seconds_ago=2 * 86400)
        await self.poll()
        self.clock += 900
        entry = copy.deepcopy(self.remote_details["post-1"])
        entry["comments"] = [self.comment("2")]
        entry["comments"][0]["created_at"] = self.clock
        self.transport.list_posts.return_value = [entry]
        await self.poll()
        self.transport.get_post.assert_awaited_once()
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.intervals(), 180)
        self.assertIsNotNone(self.store.get_outbox("qzone:99:post-1:comment:post-1:-:2:42"))

    async def test_partial_list_does_not_prevent_later_detail_nested_detection(self):
        self.seed_post(seconds_ago=2 * 86400)
        root = self.comment("1", uin=77)
        root["created_at"] = self.clock - 90000
        entry = copy.deepcopy(self.remote_details["post-1"])
        entry["comments"] = [root]
        nested = self.comment("1", parent_id="1", parent_uin=77)
        self.remote_details["post-1"]["comments"] = [root, nested]
        self.transport.list_posts.return_value = [entry]
        await self.poll()
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.transport.reply.await_args.args[1]["parent_id"], "1")

    async def test_partial_first_list_keeps_unknown_historical_comments_baselined(self):
        post = self.seed_post(seconds_ago=2 * 86400)
        # Imported posts with unknown comment dates retain the old activation
        # rule even when the first list omitted a second historical comment.
        self.store._db.execute(
            "UPDATE posts SET source='external',record_json=json_set(record_json,'$.source','external') WHERE tid=?",
            (post["tid"],),
        )
        self.store._db.commit()
        root = self.comment("1")
        root["created_at"] = 0
        another = self.comment("2")
        another["created_at"] = 0
        entry = copy.deepcopy(self.remote_details["post-1"])
        entry["comments"] = [root]
        self.remote_details["post-1"]["comments"] = [root, another]
        self.transport.list_posts.return_value = [entry]
        await self.poll()
        self.transport.reply.assert_not_awaited()
        self.assertEqual(self.intervals(), 3600)
        self.assertEqual(len(self.store.get_value("comments_seen:post-1")), 2)

    async def test_read_budget_rotates_unchecked_posts_instead_of_restarting_newest(self):
        for index in range(8):
            self.seed_post(f"post-{index}", seconds_ago=90000 + index)
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 3)
        self.clock += 60
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 6)
        self.clock += 60
        await self.poll()
        ids = [call.args[0] for call in self.transport.get_post.await_args_list]
        self.assertEqual(len(ids), 8)
        self.assertEqual(len(set(ids)), 8)

    async def test_old_persona_posts_do_not_consume_current_watch_slots(self):
        current = copy.deepcopy(self.binding)
        self.binding = {**self.binding, "persona_id": "old-role"}
        for index in range(25):
            self.seed_post(f"old-{index}", seconds_ago=index)
        self.binding = current
        self.seed_post("current", seconds_ago=90000)
        await self.poll()
        self.transport.get_post.assert_awaited_once_with("current")

    async def test_account_failure_stops_the_entire_batch_and_preserves_error(self):
        for tid in ("a", "b", "c"):
            self.seed_post(tid)
        self.transport.get_post.side_effect = AccountIdentityError("QQ离线")
        with self.assertRaises(AccountIdentityError):
            await self.poll()
        self.transport.get_post.assert_awaited_once()
        self.transport.reply.assert_not_awaited()

    async def test_cooldown_stops_batch_without_swallowing_account_signal(self):
        for tid in ("a", "b"):
            self.seed_post(tid)
        self.transport.get_post.side_effect = QzoneDeferred("limited", self.clock + 900)
        with self.assertRaises(QzoneDeferred):
            await self.poll()
        self.transport.get_post.assert_awaited_once()

    async def test_single_post_error_is_delayed_but_does_not_block_other_post(self):
        self.seed_post("a")
        self.seed_post("b")
        self.remote_details["a"] = QzoneReadError("单条暂时不可读")
        self.remote_details["b"]["comments"] = [self.comment()]
        await self.poll()
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.state("a")["next_detail_at"], self.clock + 300)
        self.clock += 60
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 2)

    async def test_confirmed_publish_can_request_one_fresh_discovery(self):
        self.seed_post()
        await self.poll()
        self.clock += 60
        await self.poll()
        self.transport.list_posts.assert_awaited_once()
        polling.invalidate_list(self.store, self.binding)
        await self.poll()
        self.assertEqual(self.transport.list_posts.await_count, 2)

    async def test_list_failure_does_not_retry_each_tick(self):
        self.service.refresh_posts = AsyncMock(side_effect=QzoneReadError("not ready"))
        await self.poll()
        error = self.store.get_value("last_comment_error")
        self.assertTrue(error)
        self.clock += 60
        await self.poll()
        self.service.refresh_posts.assert_awaited_once()
        self.assertEqual(self.store.get_value("last_comment_error"), error)
        self.clock += 840
        self.service.refresh_posts.side_effect = None
        self.service.refresh_posts.return_value = []
        await self.poll()
        self.assertEqual(self.store.get_value("last_comment_error"), "")

    async def test_delayed_detail_error_survives_idle_tick_until_same_post_succeeds(self):
        self.seed_post("a")
        recovered = copy.deepcopy(self.remote_details["a"])
        self.remote_details["a"] = QzoneReadError("detail temporarily unavailable")
        await self.poll()
        error = self.store.get_value("last_comment_error")
        self.assertTrue(error)
        self.clock += 60
        await self.poll()
        self.transport.get_post.assert_awaited_once()
        self.assertEqual(self.store.get_value("last_comment_error"), error)
        self.clock += 240
        self.remote_details["a"] = recovered
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 2)
        self.assertEqual(self.store.get_value("last_comment_error"), "")

    async def test_another_post_success_does_not_clear_unresolved_detail_error(self):
        self.seed_post("a")
        self.seed_post("b")
        self.remote_details["a"] = QzoneReadError("a temporarily unavailable")
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 2)
        error = self.store.get_value("last_comment_error")
        self.assertTrue(error)
        self.clock += 180
        await self.poll()
        self.assertEqual(self.transport.get_post.await_args.args[0], "b")
        self.assertEqual(self.store.get_value("last_comment_error"), error)

    async def test_deleted_post_retires_its_pending_diagnostic_without_extra_read(self):
        self.seed_post("a")
        self.remote_details["a"] = QzoneReadError("a temporarily unavailable")
        await self.poll()
        self.assertTrue(self.store.get_value("last_comment_error"))
        self.store.begin_operation("delete:a", "delete", {})
        self.store.complete_delete("delete:a", "a", self.clock)
        self.clock += 60
        await self.poll()
        self.transport.get_post.assert_awaited_once()
        self.assertEqual(self.store.get_value("last_comment_error"), "")

    async def test_remote_deletion_stops_only_that_post_and_survives_restart(self):
        gone = self.seed_post("gone")
        self.seed_post("healthy")
        entry = copy.deepcopy(self.remote_details["gone"])
        entry["comments"] = [self.comment()]
        self.transport.list_posts.return_value = [entry]
        self.remote_details["gone"] = QzonePostDeletedError("空间已明确告知这条说说被删除")
        self.remote_details["healthy"]["comments"] = [self.comment("11")]
        await self.poll()
        self.assertTrue(polling.post_reported_deleted(self.store, gone))
        self.assertEqual(self.state("gone")["remote_deleted_observed_at"], self.clock)
        self.assertEqual(self.store.get_post("gone")["status"], "published")
        self.assertEqual(self.store.list_comments("gone")[0]["status"], "cancelled")
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.transport.reply.await_args.args[0], "healthy")
        self.assertIsNone(self.store.get_outbox("qzone:99:gone:delete"))
        self.assertIsNotNone(self.store.get_outbox("qzone:99:gone:publish"))
        self.assertEqual(self.service.request_policy("99").failures, 0)
        self.store.close()
        self.store = StateStore(self.directory / "state.sqlite3")
        self.service.store = self.store
        self.clock += 86400
        await self.poll()
        requested = [call.args[0] for call in self.transport.get_post.await_args_list]
        self.assertEqual(requested.count("gone"), 1)
        self.assertEqual(requested.count("healthy"), 2)
        self.assertEqual(self.store.get_post("gone")["status"], "published")

    async def test_generic_unavailable_post_can_recover_and_is_not_retired(self):
        post = self.seed_post()
        recovered = copy.deepcopy(self.remote_details["post-1"])
        self.remote_details["post-1"] = QzoneReadError("说说不存在或暂无查看权限")
        await self.poll()
        self.assertFalse(polling.post_reported_deleted(self.store, post))
        self.clock = self.state()["next_detail_at"]
        self.remote_details["post-1"] = recovered
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 2)
        self.assertEqual(self.store.get_value("last_comment_error"), "")

    async def test_reply_error_survives_idle_tick_and_successful_detail_refresh(self):
        self.seed_post()
        self.remote_details["post-1"]["comments"] = [self.comment()]
        self.service.reply_to_comment = AsyncMock(side_effect=RuntimeError("generation failed"))
        await self.poll()
        error = self.store.get_value("last_comment_error")
        self.assertTrue(error)
        self.clock += 60
        await self.poll()
        self.transport.get_post.assert_awaited_once()
        self.service.reply_to_comment.assert_awaited_once()
        self.assertEqual(self.store.get_value("last_comment_error"), error)
        self.clock += 120
        # The read recovers, but the comment still has no confirmed reply.
        self.service.reply_to_comment.side_effect = None
        await self.poll()
        self.assertEqual(self.transport.get_post.await_count, 2)
        self.assertEqual(self.store.get_value("last_comment_error"), error)
        record = self.store.list_comments("post-1")[0]
        self.store.begin_operation("reply:" + record["key"], "reply", {})
        self.store.complete_reply("reply:" + record["key"], record["key"], {
            "content": "已实际回复", "created_at": self.clock,
        })
        await self.poll()
        self.assertEqual(self.store.get_value("last_comment_error"), "")

    async def test_completed_reply_history_initializes_clock_on_upgrade(self):
        post = self.seed_post(seconds_ago=2 * 86400)
        record = {
            "key": "historic-comment", "tid": post["tid"], "comment_id": "10",
            "author_qq": "42", "author_name": "管理员", "content": "旧评论",
            "created_at": self.clock - 90000, "binding": self.binding,
        }
        self.store.record_comment(record)
        self.store.begin_operation("reply:historic-comment", "reply", {})
        self.store.complete_reply("reply:historic-comment", "historic-comment", {
            "content": "刚才的回复", "created_at": self.clock - 60,
        })
        self.assertEqual(self.intervals(), 180)
        self.assertEqual(self.state()["last_interaction_at"], self.clock - 60)

    async def test_terminal_replies_are_never_revived(self):
        self.seed_post()
        self.remote_details["post-1"]["comments"] = [self.comment()]
        self.service.reply_to_comment = AsyncMock()
        await self.poll()
        key = self.store.list_comments("post-1")[0]["key"]
        self.store.set_comment_status(key, "cancelled")
        self.clock += 3600
        await self.poll()
        self.service.reply_to_comment.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
