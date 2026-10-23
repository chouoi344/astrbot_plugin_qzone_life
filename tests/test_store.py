"""Business invariants of the durable queue; no AstrBot or network required."""

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from core.store import StateStore


class StateStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state.sqlite3"
        self.store = StateStore(self.path)
        self.binding = {
            "umo": "qq:FriendMessage:42",
            "bot_id": "99",
            "admin_id": "42",
            "persona_id": "role",
        }

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def post(self, tid="tid-1", source="autonomous"):
        return {
            "tid": tid,
            "source": source,
            "content": "今天去喝了咖啡。",
            "image_summary": "蓝色外套，咖啡店",
            "images": ["/private/image.png"],
            "published_at": 1792800000.0,
            "local_day": "2026-10-24",
            "binding": self.binding,
            "status": "published",
        }

    def publish(self, tid="tid-1", source="autonomous"):
        self.store.begin_operation("publish:" + tid, "publish", {"source": source})
        return self.store.complete_publish("publish:" + tid, self.post(tid, source))

    def comment(self):
        return {
            "key": "tid-1:comment-7",
            "tid": "tid-1",
            "comment_id": "7",
            "parent_id": "",
            "author_qq": "42",
            "author_name": "用户",
            "content": "咖啡好喝吗？",
            "created_at": 1792800100.0,
            "binding": self.binding,
        }

    def test_idempotent_publish_queues_each_followup_once(self):
        first = self.store.begin_operation("action", "publish", {"caption": "coffee"})
        second = self.store.begin_operation("action", "publish", {"caption": "coffee"})
        self.assertTrue(first["acquired"])
        self.assertFalse(second["acquired"])
        self.store.complete_publish("action", self.post())
        self.store.complete_publish("action", self.post())
        self.assertFalse(
            self.store.begin_operation("action", "publish", {"caption": "coffee"})["acquired"]
        )
        self.assertEqual(1, len(self.store.list_posts()))
        self.assertEqual(2, len(self.store.due_outbox()))
        self.assertEqual({"memory", "self_like"}, {x["kind"] for x in self.store.due_outbox()})
        with self.assertRaises(ValueError):
            self.store.begin_operation("action", "publish", {"caption": "changed"})

    def test_restart_does_not_reissue_uncertain_external_action(self):
        self.store.begin_operation("lost-response", "publish", {"text": "once"})
        self.store.close()
        self.store = StateStore(self.path)
        self.assertEqual(1, self.store.recover_interrupted()["operations"])
        action = self.store.begin_operation("lost-response", "publish", {"text": "once"})
        self.assertFalse(action["acquired"])
        self.assertEqual("uncertain", action["status"])
        self.assertEqual([], self.store.list_posts())
        # A real remote lookup may later establish success and complete it once.
        self.store.complete_publish("lost-response", self.post())
        self.assertEqual("done", self.store.get_operation("lost-response")["status"])

    def test_explicit_failure_is_retryable_but_done_cannot_regress(self):
        self.store.begin_operation("action", "publish", {})
        self.store.mark_operation_failed("action", "request never reached server")
        action = self.store.begin_operation("action", "publish", {})
        self.assertTrue(action["acquired"])
        self.assertEqual(2, action["attempts"])
        self.store.complete_publish("action", self.post())
        self.store.mark_operation_uncertain("action", "late exception")
        self.assertEqual("done", self.store.get_operation("action")["status"])

    def test_deleted_autonomous_post_still_counts_manual_never_counts(self):
        self.publish()
        self.publish("manual", "manual")
        self.assertEqual(1, self.store.count_autonomous("2026-10-24"))
        self.store.begin_operation("delete", "delete", {"tid": "tid-1"})
        self.store.complete_delete("delete", "tid-1", 1792800200.0)
        self.assertEqual(1, self.store.count_autonomous("2026-10-24"))
        self.assertEqual("deleted", self.store.get_post("tid-1")["status"])
        self.assertEqual(["manual"], [post["tid"] for post in self.store.list_posts()])
        self.assertEqual(2, len(self.store.list_posts(include_deleted=True)))
        self.assertEqual(1, self.store.summary()["outbox"]["cancelled"])

    def test_publish_transaction_rolls_back_fact_and_outbox_together(self):
        self.store.begin_operation("action", "publish", {})
        real_enqueue = self.store._enqueue
        calls = 0

        def broken_enqueue(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("simulated disk/application failure")
            return real_enqueue(*args, **kwargs)

        with patch.object(self.store, "_enqueue", broken_enqueue):
            with self.assertRaises(RuntimeError):
                self.store.complete_publish("action", self.post())
        self.assertIsNone(self.store.get_post("tid-1"))
        self.assertEqual([], self.store.due_outbox())
        self.assertEqual("inflight", self.store.get_operation("action")["status"])

    def test_comment_and_successful_reply_are_distinct_deduplicated_facts(self):
        self.publish()
        self.assertTrue(self.store.record_comment(self.comment()))
        self.assertFalse(self.store.record_comment(self.comment()))
        self.assertNotIn("reply", self.store.get_comment("tid-1:comment-7"))
        self.store.begin_operation("reply", "reply", {"comment": "7"})
        self.store.complete_reply(
            "reply",
            "tid-1:comment-7",
            {"content": "很好喝。", "created_at": 1792800110.0, "reply_id": "8"},
        )
        self.assertEqual("replied", self.store.get_comment("tid-1:comment-7")["status"])
        self.assertEqual(3, self.store.summary()["outbox_by_kind"]["memory"]["pending"])
        self.assertEqual(1, len(self.store.list_comments("tid-1")))

    def test_memory_follows_event_order_even_when_publish_retry_is_delayed(self):
        self.publish()
        self.store.begin_operation("delete", "delete", {})
        self.store.complete_delete("delete", "tid-1", 1792800300.0)
        publication = self.store.due_outbox("memory")[0]
        self.assertEqual("publish", publication["payload"]["kind"])
        self.assertTrue(self.store.claim_outbox(publication["event_key"]))
        retry_at = time.time() + 1000
        self.store.retry_outbox(publication["event_key"], "offline", retry_at)
        self.assertEqual([], self.store.due_outbox("memory"))
        self.assertTrue(self.store.claim_outbox(publication["event_key"], now=retry_at + 1))
        self.store.finish_outbox(publication["event_key"], {"memory_id": "upstream-123"})
        self.assertEqual("upstream-123", self.store.get_post("tid-1")["memory_id"])
        deletion = self.store.due_outbox("memory")[0]
        self.assertEqual("delete", deletion["payload"]["kind"])
        self.assertEqual(publication["event_key"], deletion["depends_on"])
        self.assertEqual(1792800300.0, deletion["payload"]["occurred_at"])
        self.assertEqual(self.binding, deletion["payload"]["binding"])

    def test_interrupted_memory_needs_reconciliation_and_like_is_safe_to_retry(self):
        self.publish()
        tasks = self.store.due_outbox()
        for task in tasks:
            self.assertTrue(self.store.claim_outbox(task["event_key"]))
            self.assertFalse(self.store.claim_outbox(task["event_key"]))
        self.store.close()
        self.store = StateStore(self.path)
        recovered = self.store.recover_interrupted()
        self.assertEqual(1, recovered["memory"])
        self.assertEqual(1, recovered["self_like"])
        statuses = {task["kind"]: task["status"] for task in self.store.due_outbox()}
        self.assertEqual({"memory": "reconcile", "self_like": "pending"}, statuses)

    def test_daily_plan_survives_restart_and_schedule_changes_only_cancel_pending(self):
        base = {
            "day": "2026-10-24",
            "schedule_hash": "schedule1",
            "slot_at": 1792800000.0,
            "activity": {"scene": "coffee"},
            "status": "pending",
            "post_tid": None,
        }
        self.store.save_plan(dict(base, plan_id="a"))
        self.store.save_plan(dict(base, plan_id="b"))
        self.store.set_value("daily_target:2026-10-24", 2)
        self.assertTrue(self.store.claim_plan("a"))
        self.assertFalse(self.store.claim_plan("a"))
        self.assertEqual(1, self.store.cancel_pending_plans("2026-10-24"))
        self.store.recover_interrupted()
        self.store.save_plan(dict(base, plan_id="a"))
        self.assertEqual(
            ["uncertain", "cancelled"], [x["status"] for x in self.store.list_plans(base["day"])]
        )
        self.assertFalse(self.store.claim_plan("a"))
        self.assertEqual(2, self.store.get_value("daily_target:2026-10-24"))

    def test_two_connections_cannot_claim_same_operation(self):
        other = StateStore(self.path)
        try:
            self.assertTrue(self.store.begin_operation("same", "publish", {})["acquired"])
            self.assertFalse(other.begin_operation("same", "publish", {})["acquired"])
        finally:
            other.close()

    def test_importing_older_post_does_not_invent_publication_or_consume_quota(self):
        self.store.track_post(self.post())
        self.assertEqual("external", self.store.get_post("tid-1")["source"])
        self.assertEqual(0, self.store.count_autonomous("2026-10-24"))
        self.assertEqual([], self.store.due_outbox())
        self.store.record_comment(self.comment())
        self.assertEqual(1, len(self.store.list_comments(status="pending")))
        self.publish("ours")
        self.store.track_post(self.post("ours", "manual"))
        self.assertEqual("autonomous", self.store.get_post("ours")["source"])

    def test_confirmed_publish_promotes_imported_post_without_losing_comment_chain(self):
        self.store.begin_operation("lost", "publish", {"source": "autonomous"})
        self.store.mark_operation_uncertain("lost", "response lost")
        self.store.track_post(self.post())
        self.store.record_comment(self.comment())
        comment_task = self.store.due_outbox("memory")[0]
        promoted = self.store.complete_publish("lost", self.post())
        self.assertEqual("autonomous", promoted["source"])
        self.assertEqual(1, self.store.count_autonomous("2026-10-24"))
        self.assertEqual("pending", self.store.get_comment(self.comment()["key"])["status"])
        # The previously enqueued comment remains first, even though its parent
        # publication fact was only confirmed later.
        self.assertEqual("comment", self.store.due_outbox("memory")[0]["payload"]["kind"])
        self.store.claim_outbox(comment_task["event_key"])
        self.store.finish_outbox(comment_task["event_key"], {"memory_id": 101})
        publication = self.store.due_outbox("memory")[0]
        self.assertEqual("publish", publication["payload"]["kind"])
        self.assertEqual(comment_task["event_key"], publication["depends_on"])
        self.assertEqual(1, len(self.store.due_outbox("self_like")))
        self.assertEqual("done", self.store.get_operation("lost")["status"])

    def test_promotion_preserves_subsequent_deletion_and_does_not_queue_like(self):
        self.store.begin_operation("lost", "publish", {})
        self.store.mark_operation_uncertain("lost", "timeout")
        self.store.track_post(self.post())
        self.store.record_comment(self.comment())
        self.store.begin_operation("delete", "delete", {})
        self.store.complete_delete("delete", "tid-1", 1792800250.0)
        promoted = self.store.complete_publish("lost", self.post())
        self.assertEqual("deleted", promoted["status"])
        self.assertEqual(1792800250.0, promoted["deleted_at"])
        self.assertEqual(1, self.store.count_autonomous("2026-10-24"))
        self.assertEqual([], self.store.due_outbox("self_like"))
        self.assertEqual("cancelled", self.store.get_comment(self.comment()["key"])["status"])
        kinds, previous = [], None
        while self.store.due_outbox("memory"):
            task = self.store.due_outbox("memory")[0]
            kinds.append(task["payload"]["kind"])
            self.assertEqual(previous, task["depends_on"])
            self.store.claim_outbox(task["event_key"])
            self.store.finish_outbox(task["event_key"])
            previous = task["event_key"]
        self.assertEqual(["comment", "delete", "publish"], kinds)

    def test_completed_publication_cannot_be_claimed_by_another_operation(self):
        self.publish()
        self.store.begin_operation("another", "publish", {})
        with self.assertRaises(ValueError):
            self.store.complete_publish("another", self.post(source="manual"))
        self.assertEqual("inflight", self.store.get_operation("another")["status"])
        self.assertEqual(1, self.store.count_autonomous("2026-10-24"))
        self.assertEqual(2, len(self.store.due_outbox()))

    def test_imported_post_cannot_be_promoted_across_bot_accounts(self):
        self.store.track_post(self.post())
        self.store.begin_operation("another", "publish", {})
        wrong_account = self.post()
        wrong_account["binding"] = {**self.binding, "bot_id": "100"}
        with self.assertRaises(ValueError):
            self.store.complete_publish("another", wrong_account)
        self.assertEqual("external", self.store.get_post("tid-1")["source"])
        self.assertEqual([], self.store.due_outbox())

    def test_memory_revision_updates_publication_pointer_not_to_deletion_event(self):
        self.publish()
        publication = self.store.due_outbox("memory")[0]
        self.store.claim_outbox(publication["event_key"])
        self.store.finish_outbox(publication["event_key"], {"memory_id": 11})
        self.store.begin_operation("delete", "delete", {})
        self.store.complete_delete("delete", "tid-1", 1792800250.0)
        deletion = self.store.due_outbox("memory")[0]
        self.store.claim_outbox(deletion["event_key"])
        self.store.finish_outbox(
            deletion["event_key"], {"memory_id": 99, "updated_memory_ids": {"11": 22, "22": 33}}
        )
        self.assertEqual("33", self.store.get_post("tid-1")["memory_id"])
        self.assertEqual("deleted", self.store.get_post("tid-1")["status"])
        self.assertEqual(99, self.store.get_outbox(deletion["event_key"])["result"]["memory_id"])
        self.assertEqual(11, self.store.get_outbox(publication["event_key"])["result"]["memory_id"])

    def test_failed_promotion_rolls_back_indexed_source_and_preserves_comment_work(self):
        self.store.track_post(self.post())
        self.store.record_comment(self.comment())
        self.store.begin_operation("lost", "publish", {})
        original = self.store._enqueue

        def broken_enqueue(*args, **kwargs):
            if args[1] == "self_like":
                raise RuntimeError("queue failure")
            return original(*args, **kwargs)

        with patch.object(self.store, "_enqueue", broken_enqueue):
            with self.assertRaises(RuntimeError):
                self.store.complete_publish("lost", self.post())
        self.assertEqual("external", self.store.get_post("tid-1")["source"])
        self.assertEqual(0, self.store.count_autonomous("2026-10-24"))
        self.assertEqual(1, len(self.store.list_comments()))
        self.assertEqual(1, len(self.store.due_outbox()))
        self.assertEqual("comment", self.store.due_outbox()[0]["payload"]["kind"])


if __name__ == "__main__":
    unittest.main()
