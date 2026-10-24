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
            "schedule": "11:00-17:00 咖啡店休息\n18:00-20:00 公园散步",
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
        self.service.settings.daily_min = self.service.settings.daily_max = 2
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
        self.store.set_value(
            f"plan_fingerprint:{self.now.date()}",
            self.service.plan_fingerprint(self.integrations.life),
        )
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
        self.store.set_value(
            f"plan_fingerprint:{self.now.date()}",
            self.service.plan_fingerprint(self.integrations.life),
        )
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
        self.assertEqual(
            self.transport.publish.await_args.args, ("用户指定文案", [str(self.photo)])
        )
        self.assertTrue(callable(self.transport.publish.await_args.kwargs["before_publish"]))
        self.transport.like.assert_awaited_once_with("sent-post")
        self.assertEqual(self.integrations.sync_memory.await_args.args[1]["kind"], "publish")

    async def test_zero_autonomous_limit_preserves_manual_self_like_and_admin_replies(self):
        self.service.settings.daily_min = self.service.settings.daily_max = 0
        await self.service.publish_from_chat(self.event, "用户指定文案")
        self.transport.publish.assert_awaited_once()
        self.transport.like.assert_awaited_once_with("sent-post")
        self.seed_post("comment-post")
        self.remote_details["comment-post"]["comments"] = [self.comment()]
        await self.service.poll_comments(self.event, self.binding)
        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.store.count_autonomous(str(self.now.date())), 0)
        self.assertFalse(self.store.get_value("paused", False))

    async def test_service_uses_text_choice_and_system_vision_as_separate_routes(self):
        self.service.settings.llm_provider_id = "space-text"
        self.service.settings.vision_provider_id = "obsolete-plugin-vision"
        config = {
            **self.context.config,
            "provider_settings": {"default_image_caption_provider_id": "system-image"},
        }
        self.context.get_config = lambda **kwargs: config
        self.context.get_provider_by_id = lambda provider_id: SimpleNamespace(text_chat=AsyncMock())
        self.context.llm_generate = AsyncMock(return_value=SimpleNamespace(completion_text="模型内容"))
        await service_module.QzoneService.llm(self.service, self.event, "写文案")
        await service_module.QzoneService.llm(
            self.service, self.event, "看照片", images=[str(self.photo)], persona=False,
        )
        text_call, image_call = self.context.llm_generate.await_args_list
        self.assertEqual(text_call.kwargs["chat_provider_id"], "space-text")
        self.assertIn("固定角色", text_call.kwargs["system_prompt"])
        self.assertEqual(image_call.kwargs["chat_provider_id"], "system-image")
        self.assertEqual(image_call.kwargs["image_urls"], [str(self.photo)])

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

    async def test_unknown_publish_blocks_new_message_and_natural_retry(self):
        self.transport.publish.return_value = self.result(status="unknown")
        await self.service.publish_from_chat(self.event, "照片")
        second = FakeEvent(self.bot, message_id="new-message")
        response = await self.service.publish_from_chat(second, "照片")
        self.assertEqual(response.status, "uncertain")
        response = await self.service.retry_last_publish(second)
        self.assertEqual(response.status, "uncertain")
        self.transport.publish.assert_awaited_once()

    async def test_natural_retry_reuses_failed_original_key_image_and_caption(self):
        self.transport.publish.return_value = self.result(status="rejected")
        await self.service.publish_from_chat(self.event, "第一次原文")
        original = self.store.list_operations(status="failed")[0]
        self.transport.publish.return_value = self.result(tid="retry-post")
        self.service.llm.reset_mock()
        await self.service.retry_last_publish(FakeEvent(self.bot, message_id="retry"))
        self.assertEqual(self.transport.publish.await_args.args, ("第一次原文", [str(self.photo)]))
        self.service.llm.assert_not_awaited()
        self.assertEqual(self.store.get_operation(original["key"])["status"], "done")
        self.assertEqual(len(self.store.list_operations()), 1)

    async def test_publish_uses_persisted_draft_instead_of_new_arguments(self):
        self.transport.publish.return_value = self.result(status="rejected")
        await self.service.publish_from_chat(self.event, "原图原文")
        operation = self.store.list_operations(status="failed")[0]
        self.transport.publish.return_value = self.result(tid="original")
        await self.service._publish(
            self.event,
            {"bot_id": "wrong"},
            "新文案",
            ["missing"],
            "新摘要",
            "autonomous",
            operation["key"],
        )
        self.assertEqual(self.transport.publish.await_args.args, ("原图原文", [str(self.photo)]))
        post = self.store.get_post("original")
        self.assertEqual(post["source"], "manual")
        self.assertEqual(post["image_summary"], "蓝色外套，咖啡店")

    async def test_removed_admin_during_delete_list_refresh_prevents_delete(self):
        self.seed_post()

        async def revoked(*args, **kwargs):
            self.context.config["admins_id"] = []
            return []

        self.transport.list_posts.side_effect = revoked
        with self.assertRaises(PermissionError):
            await self.service.delete_post(self.event, "刚才那条")
        self.transport.delete.assert_not_awaited()
        self.assertEqual(self.store.get_post("post-1")["status"], "published")

    async def test_permission_change_after_upload_prevents_final_publish(self):
        async def upload_then_check(text, images, *, before_publish):
            self.context.config["admins_id"] = []
            try:
                await before_publish()
            except PermissionError:
                return self.result(status="rejected", stage="preflight")
            self.fail("Final permission check passed after removal")

        self.transport.publish.side_effect = upload_then_check
        response = await self.service.publish_from_chat(self.event, "文案")
        self.assertEqual(response.status, "failed")
        self.assertEqual(self.store.list_posts(), [])

    async def test_explicit_rebind_follows_current_persona(self):
        self.integrations.binding["persona_id"] = "new-role"
        response = await self.service.bind(self.event)
        self.assertEqual(response.status, "done")
        self.assertEqual(self.store.get_value("binding")["persona_id"], "new-role")

    async def test_self_like_revalidates_permission_and_defers_on_pause(self):
        self.seed_post()
        self.context.config["admins_id"] = []
        await self.service.process_outbox()
        self.transport.like.assert_not_awaited()
        self.assertNotEqual(self.store.get_outbox("qzone:99:post-1:self_like")["status"], "done")

    async def test_post_choice_resolves_the_displayed_order(self):
        self.seed_post("older", seconds_ago=120)
        self.seed_post("newer", seconds_ago=60)
        response = await self.service.list_posts(self.event)
        self.assertNotIn("newer", str(response.facts))
        await self.service.delete_post(self.event, "第二条")
        self.transport.delete.assert_awaited_once_with("older")

    async def test_pause_control_is_immediate_while_background_work_holds_lock(self):
        async with self.service._lock:
            response = self.service.control(self.event, "pause")
            self.assertEqual(response.status, "done")
            self.assertTrue(self.store.get_value("paused"))

    async def test_slider_change_replans_without_reusing_cancelled_plan_ids(self):
        self.service.settings.daily_min = 1
        identities = []
        for limit in (1, 2, 1):
            self.service.settings.daily_max = limit
            await self.service.ensure_plan(self.event, self.binding)
            pending = {
                p["plan_id"]
                for p in self.store.list_plans(str(self.now.date()))
                if p["status"] == "pending"
            }
            self.assertTrue(pending)
            self.assertTrue(all(not pending.intersection(old) for old in identities))
            identities.append(pending)
        self.transport.publish.assert_not_awaited()

    async def test_ambiguous_schedule_is_skipped_without_model_inventing_time_ranges(self):
        self.integrations.life["schedule"] = "空下来再喝咖啡，今天没有明确时间安排。"
        await self.service.run_autonomous(self.event, self.binding)
        self.service.llm.assert_not_awaited()
        self.integrations.selfie.assert_not_awaited()
        self.assertEqual(self.store.list_plans(str(self.now.date())), [])

    def test_confirmation_requires_whole_current_message_and_excludes_negation(self):
        Plain = type("Plain", (), {})

        def confirm(text, quote, outcome):
            part = Plain()
            part.text = text
            event = SimpleNamespace(get_messages=lambda: [part])
            return self.service._explicit_confirmation(event, quote, outcome)

        for text in ("我确认还没有删除成功", "我确认没有发布成功", "我确认未回复成功"):
            self.assertFalse(confirm(text, text, "已成功"))
            self.assertTrue(confirm(text, text, "未成功"))
        for text, quote in (
            ("我没有确认已经发了", "确认已经发了"),
            ("他说明确我确认已经发了", "我确认已经发了"),
            ("我确认发出去了吗", "我确认发出去了吗"),
        ):
            self.assertFalse(confirm(text, quote, "已成功"))
        self.assertTrue(confirm("我确认已经发了", "我确认已经发了", "已成功"))
        self.assertFalse(confirm("我确认已经发了", "我确认已经发了", "未成功"))
        self.assertTrue(
            confirm("我确认刚才那条已经发出去了", "我确认刚才那条已经发出去了", "已成功")
        )
        self.assertTrue(
            confirm("我确认刚才那条确实没发出去", "我确认刚才那条确实没发出去", "未成功")
        )
        for text in (
            "我确认没有发错，已经发出去了",
            "我确认没删错，那条已经删了",
            "我确认不是没发出去",
            "我没有确认没发出去",
        ):
            self.assertFalse(confirm(text, text, "未成功"))

    async def test_inflight_operation_is_reported_as_in_progress(self):
        self.store.begin_operation(
            "working", "publish", {"post": {"binding": self.binding, "content": "原文"}}
        )
        response = self.service.unresolved_for_chat(self.event)
        self.assertEqual(len(response.facts["pending"]), 1)
        response = await self.service.check_pending(self.event)
        self.assertEqual(response.status, "uncertain")
        self.assertIn("仍在处理", response.facts["result"])
        self.transport.list_posts.assert_not_awaited()

    async def test_delete_check_does_not_claim_remote_inspection_or_publication(self):
        self.seed_post()
        self.store.begin_operation(
            "delete-check", "delete", {"tid": "post-1", "post": self.store.get_post("post-1")}
        )
        self.store.mark_operation_uncertain("delete-check", "response missing")
        response = await self.service.check_pending(self.event)
        self.assertEqual(response.status, "uncertain")
        self.assertEqual(response.facts["action"], "delete")
        self.assertIn("尚未确认", response.facts["result"])
        self.transport.list_posts.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
