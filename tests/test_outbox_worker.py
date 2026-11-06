"""Bounded follow-up work, account waits and uncertainty across restarts."""

import asyncio
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core import outbox_worker
from astrbot_plugin_qzone_life.core.request_policy import QzoneDeferred
from astrbot_plugin_qzone_life.core.store import StateStore


class OutboxWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state.sqlite3"
        self.store = StateStore(self.path)
        self.binding = {
            "umo": "qq:FriendMessage:42", "bot_id": "99", "admin_id": "42",
            "persona_id": "role", "scope": "private:42",
        }
        self.store.set_value("binding", self.binding)
        self.transport = SimpleNamespace(like=AsyncMock(return_value={"status": "success"}))
        self.policy = SimpleNamespace(check=Mock())
        self.service = SimpleNamespace(
            store=self.store, context=object(),
            event_factory=lambda context, binding: SimpleNamespace(binding=binding),
            verify_binding=AsyncMock(), transport=AsyncMock(return_value=self.transport),
            request_policy=lambda _: self.policy,
            integrations=SimpleNamespace(sync_memory=AsyncMock(return_value={"memory_id": "saved"})),
        )

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def publish(self, tid="post", *, binding=None):
        owner = binding or self.binding
        post = {
            "tid": tid, "source": "manual", "content": "今天晴天。", "images": [],
            "published_at": time.time(), "local_day": "2026-11-05", "binding": owner,
        }
        self.store.begin_operation("pub:" + tid, "publish", {"post": post})
        self.store.complete_publish("pub:" + tid, post)
        return f"qzone:{owner['bot_id']}:{tid}:self_like"

    async def test_temporary_likes_stop_after_three_actual_attempts(self):
        key = self.publish()
        self.transport.like.return_value = {"status": "rejected", "stage": "connect"}
        for expected in range(1, 4):
            self.store.defer_outbox(key, "due", 0)
            await outbox_worker.process_outbox(self.service)
            self.assertEqual(self.store.get_outbox(key)["attempts"], expected)
        self.assertEqual(self.store.get_outbox(key)["status"], "cancelled")
        self.store.defer_outbox(key, "cannot revive terminal", 0)
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.transport.like.await_count, 3)

    async def test_paused_and_account_cooldown_spend_no_attempts(self):
        key = self.publish()
        self.store.set_value("paused", True)
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.store.get_outbox(key)["attempts"], 0)
        self.store.set_value("paused", False)
        self.store.defer_outbox(key, "due", 0)
        self.policy.check.side_effect = QzoneDeferred("authentication", time.time() + 1800)
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.store.get_outbox(key)["attempts"], 0)
        self.transport.like.assert_not_awaited()
        self.assertEqual(self.service.integrations.sync_memory.await_count, 1)

    async def test_offline_preflight_keeps_like_and_still_syncs_memory(self):
        key = self.publish()
        self.service.verify_binding.side_effect = ValueError("offline")
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.store.get_outbox(key)["attempts"], 0)
        self.assertEqual(self.store.get_outbox(key)["status"], "pending")
        self.assertEqual(self.store.get_outbox("qzone:99:post:publish")["status"], "done")
        self.transport.like.assert_not_awaited()

    async def test_before_send_failures_refund_the_claim(self):
        key = self.publish()
        for stage in ("credentials", "cooldown", "preflight"):
            self.store.defer_outbox(key, "due", 0)
            self.transport.like.return_value = {"status": "rejected", "stage": stage}
            await outbox_worker.process_outbox(self.service, include_memory=False)
            self.assertEqual(self.store.get_outbox(key)["attempts"], 0)
        self.store.defer_outbox(key, "due", 0)
        self.transport.like.side_effect = QzoneDeferred("network", time.time() + 900)
        await outbox_worker.process_outbox(self.service, include_memory=False)
        self.assertEqual(self.store.get_outbox(key)["attempts"], 0)

    async def test_unknown_result_is_preserved_without_replay_after_restart(self):
        key = self.publish()
        self.transport.like.return_value = {"status": "unknown"}
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.store.get_outbox(key)["status"], "uncertain")
        self.store.close()
        self.store = StateStore(self.path)
        self.service.store = self.store
        self.store.recover_interrupted()
        await outbox_worker.process_outbox(self.service)
        self.transport.like.assert_awaited_once()
        self.assertEqual(self.store.get_outbox(key)["status"], "uncertain")

    async def test_permanent_rejection_stops_without_marking_success(self):
        key = self.publish()
        self.transport.like.return_value = {"status": "rejected", "code": 100, "retryable": False}
        await outbox_worker.process_outbox(self.service)
        result = self.store.get_outbox(key)
        self.assertEqual(result["status"], "cancelled")
        self.assertIsNone(result["result"])

    async def test_cancel_during_like_leaves_uncertainty(self):
        key = self.publish()
        entered = asyncio.Event()

        async def slow_like(tid, **kwargs):
            entered.set()
            await asyncio.Future()

        self.transport.like.side_effect = slow_like
        task = asyncio.create_task(outbox_worker.process_outbox(self.service))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.store.get_outbox(key)["status"], "uncertain")
        await outbox_worker.process_outbox(self.service)
        self.transport.like.assert_awaited_once()

    async def test_memory_timeout_preserves_reconciliation(self):
        self.publish()

        async def slow_memory(*args):
            await asyncio.Future()

        self.service.integrations.sync_memory.side_effect = slow_memory
        with patch.object(outbox_worker, "MEMORY_TIMEOUT", 0.01):
            await outbox_worker.process_outbox(self.service, allow_qq=False)
        task = self.store.get_outbox("qzone:99:post:publish")
        self.assertEqual(task["status"], "reconcile")
        self.assertGreater(task["next_attempt_at"], time.time())
        self.transport.like.assert_not_awaited()

    async def test_small_batches_do_not_drain_a_recovery_backlog(self):
        for index in range(7):
            self.publish(str(index))
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.service.integrations.sync_memory.await_count, 3)
        self.transport.like.assert_awaited_once()
        self.assertEqual(len(self.store.due_outbox("memory")), 4)
        self.assertEqual(len(self.store.due_outbox("self_like")), 6)

    async def test_old_owner_like_is_retired_but_memory_uses_original_binding(self):
        old = {**self.binding, "persona_id": "old-role"}
        key = self.publish(binding=old)
        await outbox_worker.process_outbox(self.service)
        self.assertEqual(self.store.get_outbox(key)["status"], "cancelled")
        self.transport.like.assert_not_awaited()
        event, payload = self.service.integrations.sync_memory.await_args.args
        self.assertEqual(event.binding, old)
        self.assertEqual(payload["binding"], old)

    async def test_remote_deletion_stops_like_and_preserves_publication_memory(self):
        key = self.publish()
        self.store.set_value("comment_poll:99:post", {"remote_deleted_observed_at": time.time()})
        await outbox_worker.process_outbox(self.service)
        task = self.store.get_outbox(key)
        self.assertEqual((task["status"], task["attempts"]), ("cancelled", 0))
        self.transport.like.assert_not_awaited()
        self.assertEqual(self.store.get_post("post")["status"], "published")
        self.assertEqual(self.store.get_outbox("qzone:99:post:publish")["status"], "done")
        self.assertIsNone(self.store.get_outbox("qzone:99:post:delete"))

    async def test_concurrent_worker_does_not_duplicate_claims(self):
        self.publish()
        entered, finish = asyncio.Event(), asyncio.Event()

        async def like(tid, **kwargs):
            entered.set()
            await finish.wait()
            return {"status": "success"}

        self.transport.like.side_effect = like
        task = asyncio.create_task(outbox_worker.process_outbox(self.service))
        await entered.wait()
        await outbox_worker.process_outbox(self.service)
        self.transport.like.assert_awaited_once()
        finish.set()
        await task

    async def test_memory_only_then_likes_only_do_not_process_memory_twice(self):
        for index in range(5):
            self.publish(str(index))
        await outbox_worker.process_outbox(self.service, allow_qq=False)
        await outbox_worker.process_outbox(self.service, include_memory=False)
        self.assertEqual(self.service.integrations.sync_memory.await_count, 3)
        self.transport.like.assert_awaited_once()

    async def test_pause_during_pacing_stops_at_final_preflight_without_spending_attempt(self):
        key = self.publish()

        async def like(tid, *, before_write):
            self.store.set_value("paused", True)
            with self.assertRaisesRegex(ValueError, "条件已变化"):
                await before_write()
            return {"status": "rejected", "stage": "preflight"}

        self.transport.like.side_effect = like
        await outbox_worker.process_outbox(self.service)
        task = self.store.get_outbox(key)
        self.assertEqual((task["status"], task["attempts"]), ("pending", 0))

    async def test_role_change_during_pacing_retires_old_like(self):
        key = self.publish()

        async def like(tid, *, before_write):
            self.store.set_value("binding", {**self.binding, "persona_id": "changed"})
            with self.assertRaisesRegex(ValueError, "条件已变化"):
                await before_write()
            return {"status": "rejected", "stage": "preflight"}

        self.transport.like.side_effect = like
        await outbox_worker.process_outbox(self.service)
        task = self.store.get_outbox(key)
        self.assertEqual((task["status"], task["attempts"]), ("cancelled", 0))

    async def test_remote_deletion_during_pacing_stops_like_without_spending_attempt(self):
        key = self.publish()

        async def like(tid, *, before_write):
            self.store.set_value("comment_poll:99:post", {"remote_deleted_observed_at": time.time()})
            with self.assertRaisesRegex(ValueError, "条件已变化"):
                await before_write()
            return {"status": "rejected", "stage": "preflight"}

        self.transport.like.side_effect = like
        await outbox_worker.process_outbox(self.service)
        task = self.store.get_outbox(key)
        self.assertEqual((task["status"], task["attempts"]), ("cancelled", 0))


if __name__ == "__main__":
    unittest.main()
