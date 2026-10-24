"""Uncertain writes remain recoverable across role changes and delayed retries."""

import copy
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import test_service as fixtures
from astrbot_plugin_qzone_life.core.store import StateStore


class ReconciliationEdges(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.ServiceWorkflowTests.setUp
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post

    def pending_publication(self, key="pending", *, retry=False):
        first = self.now.timestamp() - (1800 if retry else 0)
        draft = {
            "content": "这张照片留给今天。", "image_summary": "蓝色外套",
            "images": [str(self.photo)], "binding": copy.deepcopy(self.binding),
            "source": "manual", "local_day": str(self.now.date()),
        }
        with patch("astrbot_plugin_qzone_life.core.store.time.time", return_value=first):
            self.store.begin_operation(key, "publish", {"post": draft})
            if retry:
                self.store.mark_operation_failed(key, "explicit rejection")
        if retry:
            with patch("astrbot_plugin_qzone_life.core.store.time.time", return_value=self.now.timestamp()):
                self.store.begin_operation(key, "publish", {"post": draft})
        self.store.mark_operation_uncertain(key, "response missing")
        remote = {
            "tid": "actual-post", "uin": "99", "text": draft["content"],
            "created_at": self.now.timestamp(), "images": [], "comments": [],
        }
        self.transport.list_posts.return_value = [remote]
        self.remote_details[remote["tid"]] = remote
        return draft, remote

    async def test_original_role_unknown_publication_can_be_checked_and_reconciled(self):
        draft, _ = self.pending_publication()
        self.integrations.binding.update(persona_id="new-role", scope="new-scope")
        result = await self.service.check_pending(self.event)
        self.assertEqual(result.facts["matching_posts"], 1)
        await self.service.reconcile(self.event, "pending", "已成功", "actual-post")
        self.assertEqual(self.store.get_operation("pending")["status"], "done")
        self.assertEqual(self.store.get_post("actual-post")["binding"], draft["binding"])
        self.assertIsNone(self.service.pending_publish("99"))
        self.transport.publish.assert_not_awaited()
        memory = self.store.get_outbox("qzone:99:actual-post:publish")
        self.assertEqual(memory["payload"]["binding"], draft["binding"])

    async def test_original_role_unknown_delete_can_be_confirmed_without_another_delete(self):
        self.seed_post()
        post = self.store.get_post("post-1")
        self.store.begin_operation("old-delete", "delete", {"tid": "post-1", "post": post})
        self.store.mark_operation_uncertain("old-delete", "response missing")
        self.integrations.binding["persona_id"] = "new-role"
        await self.service.reconcile(self.event, "old-delete", "已成功")
        self.assertEqual(self.store.get_post("post-1")["status"], "deleted")
        self.assertEqual(self.store.get_post("post-1")["binding"], post["binding"])
        self.transport.delete.assert_not_awaited()

    async def test_last_retry_time_finds_the_later_publication(self):
        self.pending_publication(retry=True)
        result = await self.service.check_pending(self.event)
        self.assertEqual(result.facts["matching_posts"], 1)

    async def test_post_near_rejected_first_attempt_does_not_match_latest_attempt(self):
        _, remote = self.pending_publication(retry=True)
        remote["created_at"] -= 1800
        result = await self.service.check_pending(self.event)
        self.assertEqual(result.facts["matching_posts"], 0)

    async def test_other_account_cannot_reconcile_an_old_record(self):
        draft, _ = self.pending_publication()
        draft["binding"]["bot_id"] = "different-account"
        self.store.begin_operation("foreign", "publish", {"post": draft})
        self.store.mark_operation_uncertain("foreign", "response missing")
        with self.assertRaises(PermissionError):
            await self.service.reconcile(self.event, "foreign", "未成功")
        self.assertEqual(self.store.get_operation("foreign")["status"], "uncertain")

    async def test_poll_does_not_attribute_old_unknown_publication_to_new_role(self):
        draft, remote = self.pending_publication()
        remote["comments"] = [{
            "id": "10", "uin": 42, "nickname": "管理员", "content": "照片很好看",
            "created_at": self.now.timestamp(), "parent_id": None, "parent_uin": None,
        }]
        unrelated = {**copy.deepcopy(remote), "tid": "unrelated-post", "text": "另一次分享", "comments": []}
        self.transport.list_posts.return_value = [remote, unrelated]
        self.remote_details[unrelated["tid"]] = unrelated
        self.integrations.binding.update(persona_id="new-role", scope="new-scope")
        current = await self.service.binding_for(self.event)

        await self.service.poll_comments(self.event, current)

        self.assertIsNone(self.store.get_post(remote["tid"]))
        self.assertEqual(self.store.list_comments(remote["tid"]), [])
        self.transport.reply.assert_not_awaited()
        self.assertEqual(self.store.get_post(unrelated["tid"])["binding"], current)
        self.assertEqual(self.store.get_operation("pending")["status"], "uncertain")

        await self.service.reconcile(self.event, "pending", "已成功", remote["tid"])

        self.assertEqual(self.store.get_post(remote["tid"])["binding"], draft["binding"])
        self.assertEqual(
            self.store.get_outbox("qzone:99:" + remote["tid"] + ":publish")["payload"]["binding"],
            draft["binding"],
        )
        self.transport.publish.assert_not_awaited()
        self.transport.reply.assert_not_awaited()


class AttemptTimeMigration(unittest.TestCase):
    def test_upgrade_after_legacy_rollback_refreshes_attempt_time_once(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollback.sqlite3"
            store = StateStore(path)
            with patch("astrbot_plugin_qzone_life.core.store.time.time", return_value=100):
                store.begin_operation("retry", "publish", {"original": True})
                store.mark_operation_failed("retry", "rejected")
            store.close()
            # Older versions retain extra columns but know only updated_at.
            with sqlite3.connect(path) as db:
                db.execute("PRAGMA user_version=1")
                db.execute("UPDATE operations SET attempts=2,updated_at=1900,status='uncertain'")
            db.close()
            store = StateStore(path)
            self.assertEqual(store.get_operation("retry")["last_attempt_at"], 1900)
            store.close()
            with sqlite3.connect(path) as db:
                db.execute("UPDATE operations SET updated_at=9000")
            db.close()
            store = StateStore(path)
            try:
                self.assertEqual(store.get_operation("retry")["last_attempt_at"], 1900)
                self.assertEqual(store.get_operation("retry")["payload"], {"original": True})
            finally:
                store.close()

    def test_existing_database_gains_attempt_time_without_changing_payload_or_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE operations (key TEXT PRIMARY KEY, kind TEXT NOT NULL, "
                       "status TEXT NOT NULL, payload_json TEXT NOT NULL, result_json TEXT, "
                       "error TEXT, attempts INTEGER NOT NULL DEFAULT 1, "
                       "created_at REAL NOT NULL, updated_at REAL NOT NULL)")
            db.execute("INSERT INTO operations VALUES ('retry','publish','uncertain',?,NULL,?,2,100,1900)",
                       ('{"post":{"content":"original"}}', "missing response"))
            db.commit()
            db.close()
            store = StateStore(path)
            try:
                op = store.get_operation("retry")
                self.assertEqual(op["last_attempt_at"], 1900)
                self.assertEqual(op["created_at"], 100)
                self.assertEqual(op["status"], "uncertain")
                self.assertEqual(op["payload"], {"post": {"content": "original"}})
                store.recover_interrupted()
                self.assertEqual(store.get_operation("retry")["last_attempt_at"], 1900)
            finally:
                store.close()
