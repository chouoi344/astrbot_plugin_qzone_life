"""Workflow regression tests with durable state and no QQ/AstrBot network calls."""

from __future__ import annotations

import asyncio
import copy
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core import service as service_module
from astrbot_plugin_qzone_life.core.planning import parse_schedule, schedule_hash, select_slots
from astrbot_plugin_qzone_life.core.store import StateStore
from astrbot_plugin_qzone_life.core.transport import QzoneReadError


class FakeEvent:
    def __init__(self, bot, *, message_id="message-1", sender="42", self_id="99"):
        self.bot = bot
        self.message_obj = SimpleNamespace(message_id=message_id)
        self.unified_msg_origin = f"qq:FriendMessage:{sender}"
        self.sender, self.self_id = sender, self_id

    def get_sender_id(self):
        return self.sender

    def get_sender_name(self):
        return "管理员"

    def get_self_id(self):
        return self.self_id

    def get_group_id(self):
        return ""

    def get_platform_name(self):
        return "aiocqhttp"


class FakeContext:
    def __init__(self):
        self.config = {"admins_id": ["42"], "timezone": "Asia/Shanghai"}

    def get_config(self):
        return self.config


class FakeIntegrations:
    def __init__(self, binding, photo):
        self.binding = copy.deepcopy(binding)
        self.life = {
            "schedule": "12:00 在咖啡店休息\n18:00 在公园散步",
            "outfit": "蓝色外套",
            "meta": {},
        }
        self.life_context = AsyncMock(side_effect=lambda: copy.deepcopy(self.life))
        self.selfie = AsyncMock(return_value=(photo, {"provider": "fake"}))
        self.sync_memory = AsyncMock(
            side_effect=lambda event, payload: {"memory_id": "mem:" + payload["event_key"]}
        )
        self.persona = AsyncMock(return_value=("role", "固定角色"))
        self.check_life_persona = AsyncMock(return_value=None)

    async def resolve_binding(self, event, expected_persona_id=None):
        if expected_persona_id and expected_persona_id != self.binding["persona_id"]:
            raise ValueError("角色不一致")
        result = copy.deepcopy(self.binding)
        result.update(
            umo=event.unified_msg_origin,
            bot_id=str(event.get_self_id()),
            admin_id=str(event.get_sender_id()),
        )
        return result


class ServiceWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.photo = self.directory / "photo.png"
        self.photo.write_bytes(b"fake-image-for-mocked-transport")
        self.now = datetime(2026, 10, 24, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        self.binding = {
            "umo": "qq:FriendMessage:42",
            "bot_id": "99",
            "admin_id": "42",
            "admin_name": "管理员",
            "persona_id": "role",
            "scope": "session:42",
        }
        self.bot = object()
        self.event = FakeEvent(self.bot)
        self.context = FakeContext()
        self.integrations = FakeIntegrations(self.binding, self.photo)
        self.remote_details = {}
        self.transport = SimpleNamespace(
            identity=AsyncMock(return_value={"uin": 99, "nickname": "角色"}),
            publish=AsyncMock(return_value=self.result(tid="sent-post")),
            list_posts=AsyncMock(return_value=[]),
            get_post=AsyncMock(side_effect=self.remote_post),
            delete=AsyncMock(side_effect=lambda tid: self.result(tid=tid)),
            like=AsyncMock(side_effect=lambda tid: self.result(tid=tid)),
            reply=AsyncMock(
                side_effect=lambda tid, comment, text: self.result(tid=tid, reply_id="reply-1")
            ),
            close=AsyncMock(),
        )
        self.store = StateStore(self.directory / "state.sqlite3")
        self.service = service_module.QzoneService(
            self.context,
            {"daily_min": 2, "daily_max": 2, "timezone": "Asia/Shanghai"},
            self.directory,
            integrations=self.integrations,
            store=self.store,
            transport_factory=lambda *args, **kwargs: self.transport,
            event_factory=lambda context, binding: FakeEvent(
                self.bot,
                message_id="background",
                sender=binding["admin_id"],
                self_id=binding["bot_id"],
            ),
        )
        self.service.now = lambda: self.now
        self.service.photo_summary = AsyncMock(return_value="蓝色外套，咖啡店")
        self.service.llm = AsyncMock(return_value="今天在咖啡店休息。")
        self.store.set_value("binding", self.binding)
        self.store.set_value("activated_at", 0)
        self.image_patch = patch.object(
            service_module, "collect_images", AsyncMock(return_value=[str(self.photo)])
        )
        self.image_patch.start()

    async def asyncTearDown(self):
        self.image_patch.stop()
        await self.service.close()
        self.temp.cleanup()

    def result(self, *, status="success", tid="", **extra):
        return {
            "status": status,
            "tid": tid,
            "created_at": self.now.timestamp(),
            "message": "",
            **extra,
        }

    async def remote_post(self, tid):
        result = self.remote_details[tid]
        if isinstance(result, BaseException):
            raise result
        return copy.deepcopy(result)

    def seed_post(self, tid="post-1", *, seconds_ago=60, source="manual"):
        post = {
            "tid": tid,
            "source": source,
            "content": "今天喝了咖啡。",
            "image_summary": "咖啡杯",
            "images": [str(self.photo)],
            "published_at": self.now.timestamp() - seconds_ago,
            "local_day": str(self.now.date()),
            "binding": self.binding,
            "status": "published",
        }
        self.store.begin_operation("seed:" + tid, "publish", {"tid": tid})
        self.store.complete_publish("seed:" + tid, post)
        self.remote_details[tid] = {
            "tid": tid,
            "uin": 99,
            "text": post["content"],
            "created_at": post["published_at"],
            "images": [],
            "comments": [],
        }
        return post

    def comment(self, cid="10", *, uin=42, parent_id=None, parent_uin=None, content="好看"):
        return {
            "id": cid,
            "uin": uin,
            "nickname": f"用户{uin}",
            "content": content,
            "created_at": self.now.timestamp(),
            "parent_id": parent_id,
            "parent_uin": parent_uin,
        }

    def seed_due_plan(self, plan_id, seconds_ago):
        fingerprint = schedule_hash(self.integrations.life)
        self.store.set_value(f"plan_fingerprint:{self.now.date()}", fingerprint)
        return self.store.save_plan(
            {
                "plan_id": plan_id,
                "day": str(self.now.date()),
                "schedule_hash": fingerprint,
                "slot_at": self.now.timestamp() - seconds_ago,
                "activity": "咖啡店休息",
                "status": "pending",
            }
        )

    def seed_short_activity_plan(self):
        self.integrations.life["schedule"] = "12:00-12:10 咖啡店休息"
        fingerprint = schedule_hash(self.integrations.life)
        self.store.set_value(f"plan_fingerprint:{self.now.date()}", fingerprint)
        plans = select_slots(
            parse_schedule(self.integrations.life["schedule"]),
            self.now.replace(hour=11, minute=59),
            fingerprint,
            1,
            self.service.settings,
            seed="99",
        )
        self.assertEqual(len(plans), 1)
        return self.store.save_plan(plans[0])

    def prepare_first_external_post_poll(self, comment_time):
        self.store.set_value("activated_at", self.now.timestamp() - 60)
        comment = self.comment()
        comment["created_at"] = comment_time
        remote = {
            "tid": "external-post",
            "uin": 99,
            "text": "之前发布的说说",
            "created_at": self.now.timestamp() - 3600,
            "images": [],
            "comments": [comment],
        }
        self.remote_details["external-post"] = remote
        self.transport.list_posts.return_value = [copy.deepcopy(remote)]

    async def test_manual_publish_does_not_consume_daily_autonomous_quota(self):
        response = await self.service.publish_from_chat(self.event, "用户指定文案")
        self.assertIn("sent-post", response)
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)
        self.assertEqual(self.store.get_post("sent-post")["source"], "manual")
        self.transport.publish.assert_awaited_once_with("用户指定文案", [str(self.photo)])
        self.transport.like.assert_awaited_once_with("sent-post")
        self.assertEqual(self.integrations.sync_memory.await_args.args[1]["kind"], "publish")

    async def test_memory_failure_and_message_replay_never_republish(self):
        self.integrations.sync_memory.side_effect = RuntimeError("memory offline")
        await self.service.publish_from_chat(self.event, "文案")
        await self.service.publish_from_chat(self.event, "文案")
        self.transport.publish.assert_awaited_once()
        self.assertEqual(len(self.store.list_posts()), 1)
        queued = self.store.get_outbox("qzone:99:sent-post:publish")
        self.assertIn(queued["status"], ("pending", "reconcile"))
        self.store.retry_outbox(queued["event_key"], "test recovery", 0, needs_reconcile=True)
        self.integrations.sync_memory.side_effect = None
        self.integrations.sync_memory.return_value = {"memory_id": "recovered-memory"}
        await self.service.process_outbox()
        self.transport.publish.assert_awaited_once()
        self.assertEqual(self.store.get_post("sent-post")["memory_id"], "recovered-memory")

    async def test_replayed_delete_latest_message_does_not_delete_next_post(self):
        self.seed_post("older", seconds_ago=120)
        self.seed_post("latest", seconds_ago=60)
        await self.service.delete_post(self.event, "刚才那条")
        await self.service.delete_post(self.event, "刚才那条")
        self.transport.delete.assert_awaited_once_with("latest")
        self.assertEqual(self.store.get_post("latest")["status"], "deleted")
        self.assertEqual(self.store.get_post("older")["status"], "published")

    async def test_only_target_author_controls_admin_reply_in_nested_threads(self):
        self.seed_post()
        comments = [
            self.comment("10", uin=77),
            self.comment("1", uin=42, parent_id="10", parent_uin=77, content="管理员楼中楼"),
            self.comment("20", uin=42, content="管理员主楼"),
            self.comment("1", uin=77, parent_id="20", parent_uin=42),
            self.comment("2", uin=99, parent_id="20", parent_uin=42),
        ]
        self.remote_details["post-1"]["comments"] = comments
        await self.service.poll_comments(self.event, self.binding)
        await self.service.poll_comments(self.event, self.binding)
        requests = self.transport.reply.await_args_list
        self.assertEqual(len(requests), 2)
        self.assertEqual(
            {
                (call.args[1]["id"], call.args[1]["parent_id"], call.args[1]["uin"])
                for call in requests
            },
            {("1", "10", 42), ("20", None, 42)},
        )
        records = self.store.list_comments("post-1")
        self.assertEqual(len(records), 2)
        self.assertTrue(
            all(record["author_qq"] == "42" and record["status"] == "replied" for record in records)
        )
        # The exact parent owner survives so the transport can address the child.
        nested = next(call.args[1] for call in requests if call.args[1]["parent_id"])
        self.assertEqual(nested["parent_uin"], 77)

    async def test_first_poll_replies_to_external_post_comment_after_activation_and_queues_memories(
        self,
    ):
        self.prepare_first_external_post_poll(self.now.timestamp())
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.transport.reply.await_args.args[0], "external-post")
        self.assertEqual(self.store.get_post("external-post")["source"], "external")
        records = self.store.list_comments("external-post")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["status"], "replied")
        for kind in ("comment", "reply"):
            queued = self.store.get_outbox(f"qzone:99:external-post:{kind}:{records[0]['key']}")
            self.assertIsNotNone(queued)
            self.assertEqual(queued["kind"], "memory")
            self.assertEqual(queued["payload"]["kind"], kind)

    async def test_first_poll_does_not_back_reply_to_pre_activation_external_comment(self):
        self.prepare_first_external_post_poll(self.now.timestamp() - 120)
        await self.service.poll_comments(self.event, self.binding)
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_not_awaited()
        self.assertEqual(self.store.list_comments("external-post"), [])
        self.assertEqual(self.store.summary()["outbox"], {})

    async def test_first_poll_unknown_comment_time_is_baselined_without_back_reply(self):
        self.prepare_first_external_post_poll(0)
        await self.service.poll_comments(self.event, self.binding)
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_not_awaited()
        self.assertEqual(self.store.list_comments("external-post"), [])
        self.assertEqual(self.store.summary()["outbox"], {})

    async def test_unknown_reply_reconciliation_reuses_original_text(self):
        self.seed_post()
        self.remote_details["post-1"]["comments"] = [self.comment()]
        self.service.llm.side_effect = ["首次生成的回复", "不应该重新生成"]
        self.transport.reply.side_effect = [
            self.result(status="unknown", tid="post-1"),
            self.result(tid="post-1", reply_id="known"),
        ]
        await self.service.poll_comments(self.event, self.binding)
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_awaited_once()
        operations = self.store.list_operations(status="uncertain")
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0]["kind"], "reply")
        await self.service.reconcile(self.event, operations[0]["key"], "未成功")
        await self.service.poll_comments(self.event, self.binding)
        self.assertEqual(self.transport.reply.await_count, 2)
        self.assertEqual(
            [call.args[2] for call in self.transport.reply.await_args_list],
            ["首次生成的回复", "首次生成的回复"],
        )
        self.service.llm.assert_awaited_once()
        self.assertEqual(self.store.list_comments("post-1")[0]["status"], "replied")

    async def test_one_unreadable_post_does_not_block_later_comments(self):
        self.seed_post("good", seconds_ago=120)
        self.seed_post("unavailable", seconds_ago=60)
        self.remote_details["unavailable"] = QzoneReadError("暂时无法访问")
        self.remote_details["good"]["comments"] = [self.comment()]
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.transport.reply.await_args.args[0], "good")
        self.assertEqual(self.store.get_post("unavailable")["status"], "published")

    async def test_removed_admin_during_generation_is_not_replied_to(self):
        self.seed_post()
        self.remote_details["post-1"]["comments"] = [self.comment()]

        async def remove_admin(*args, **kwargs):
            self.context.config["admins_id"] = []
            return "这条不应发送"

        self.service.llm.side_effect = remove_admin
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_not_awaited()
        self.assertNotEqual(self.store.list_comments("post-1")[0]["status"], "replied")

    async def test_scope_change_blocks_autonomous_publish(self):
        self.seed_due_plan("scope-plan", 60)
        self.integrations.binding["scope"] = "different-scope"
        try:
            await self.service.run_autonomous(self.event, self.binding)
        except (RuntimeError, ValueError):
            pass  # A visible binding error or a recorded pause are both valid.
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)

    async def test_life_scheduler_persona_mismatch_blocks_selfie_and_publish(self):
        self.seed_due_plan("wrong-life-persona", 60)
        self.integrations.check_life_persona.side_effect = RuntimeError("日程角色与空间角色不一致")
        try:
            await self.service.run_autonomous(self.event, self.binding)
        except (RuntimeError, ValueError):
            pass
        self.integrations.check_life_persona.assert_awaited_once_with(self.binding)
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()

    async def test_pause_while_selfie_is_generating_prevents_publish(self):
        self.seed_due_plan("pause-plan", 60)

        async def pause_during_selfie(*args, **kwargs):
            await asyncio.sleep(0)
            self.store.set_value("paused", True)
            return self.photo, {}

        self.integrations.selfie.side_effect = pause_during_selfie
        await self.service.run_autonomous(self.event, self.binding)
        self.integrations.selfie.assert_awaited_once()
        self.transport.publish.assert_not_awaited()
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)

    async def test_resume_after_short_activity_ends_does_not_generate_selfie(self):
        plan = self.seed_short_activity_plan()
        self.now = self.now.replace(hour=12, minute=45)
        await self.service.run_autonomous(self.event, self.binding)
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()
        saved = next(
            item
            for item in self.store.list_plans(str(self.now.date()))
            if item["plan_id"] == plan["plan_id"]
        )
        self.assertEqual(saved["status"], "skipped")

    async def test_activity_ends_while_selfie_runs_prevents_publish(self):
        plan = self.seed_short_activity_plan()
        self.now = datetime.fromtimestamp(plan["slot_at"], self.now.tzinfo)

        async def finish_at_activity_end(*args, **kwargs):
            self.now = self.now.replace(hour=12, minute=10, second=0)
            return self.photo, {}

        self.integrations.selfie.side_effect = finish_at_activity_end
        await self.service.run_autonomous(self.event, self.binding)
        self.integrations.selfie.assert_awaited_once()
        self.transport.publish.assert_not_awaited()
        self.assertEqual(self.store.list_plans(str(self.now.date()))[0]["status"], "cancelled")

    async def test_activity_ends_while_caption_runs_prevents_final_publish(self):
        plan = self.seed_short_activity_plan()
        self.now = datetime.fromtimestamp(plan["slot_at"], self.now.tzinfo)

        async def caption_finishes_after_end(*args, **kwargs):
            self.now = self.now.replace(hour=12, minute=11, second=0)
            return "此刻已经不该发布的文案"

        self.service.llm.side_effect = caption_finishes_after_end
        await self.service.run_autonomous(self.event, self.binding)
        self.integrations.selfie.assert_awaited_once()
        self.service.llm.assert_awaited_once()
        self.transport.publish.assert_not_awaited()
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)

    async def test_uncertain_autonomous_publish_prevents_next_opportunity(self):
        self.seed_due_plan("first-plan", 60)
        self.seed_due_plan("second-plan", 30)
        self.transport.publish.side_effect = [
            self.result(status="unknown"),
            self.result(tid="must-not-exist"),
        ]
        await self.service.run_autonomous(self.event, self.binding)
        await self.service.run_autonomous(self.event, self.binding)
        self.transport.publish.assert_awaited_once()
        self.integrations.selfie.assert_awaited_once()
        self.assertEqual(
            len(
                [
                    op
                    for op in self.store.list_operations(status="uncertain")
                    if op["kind"] == "publish"
                ]
            ),
            1,
        )
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)

    async def test_changed_schedule_during_selfie_prevents_stale_post(self):
        self.seed_due_plan("changed-life", 60)

        async def change_life(*args, **kwargs):
            self.integrations.life["outfit"] = "已经换成运动服"
            return self.photo, {}

        self.integrations.selfie.side_effect = change_life
        await self.service.run_autonomous(self.event, self.binding)
        self.transport.publish.assert_not_awaited()
        self.assertEqual(self.store.list_plans(str(self.now.date()))[0]["status"], "cancelled")

    async def test_unknown_publish_can_be_reconciled_after_remote_import(self):
        self.transport.publish.return_value = self.result(status="unknown")
        await self.service.publish_from_chat(self.event, "唯一的原始文案")
        self.remote_details["actually-posted"] = {
            "tid": "actually-posted",
            "uin": 99,
            "text": "唯一的原始文案",
            "created_at": self.now.timestamp(),
            "images": [],
            "comments": [],
        }
        self.transport.list_posts.return_value = [
            copy.deepcopy(self.remote_details["actually-posted"])
        ]
        await self.service.refresh_posts(self.event, self.binding)
        self.assertEqual(self.store.get_post("actually-posted")["source"], "external")
        operation = self.store.list_operations(status="uncertain")[0]
        await self.service.reconcile(self.event, operation["key"], "已成功", "actually-posted")
        self.transport.publish.assert_awaited_once()
        self.assertEqual(self.store.get_operation(operation["key"])["status"], "done")
        post = self.store.get_post("actually-posted")
        self.assertEqual(post["source"], "manual")
        self.assertEqual(post["content"], "唯一的原始文案")
        self.assertTrue(post.get("memory_id"))


if __name__ == "__main__":
    unittest.main()
