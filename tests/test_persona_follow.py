"""Selected chat personas follow through without rewriting historical ownership."""

from __future__ import annotations

import copy
import unittest
from unittest.mock import AsyncMock, Mock

import test_service as fixtures


class PersonaFollowTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.ServiceWorkflowTests.setUp
    asyncTearDown = fixtures.ServiceWorkflowTests.asyncTearDown
    result = fixtures.ServiceWorkflowTests.result
    remote_post = fixtures.ServiceWorkflowTests.remote_post
    seed_post = fixtures.ServiceWorkflowTests.seed_post
    comment = fixtures.ServiceWorkflowTests.comment
    seed_due_plan = fixtures.ServiceWorkflowTests.seed_due_plan

    async def test_persona_change_waits_for_new_schedule_before_autonomous_generation(self):
        original = await self.service.current_life(self.binding)
        self.assertTrue(original)
        self.seed_due_plan("old-life", 60)
        self.integrations.binding["persona_id"] = "new-role"
        current = await self.service.binding_for(self.event)
        await self.service.run_autonomous(self.event, current)
        self.integrations.selfie.assert_not_awaited()
        self.transport.publish.assert_not_awaited()
        self.assertEqual(await self.service.current_life(current), {})
        self.integrations.life.update(schedule="11:00-17:00 图书馆阅读", outfit="新角色的衬衫")
        self.assertEqual(await self.service.current_life(current), self.integrations.life)

    async def test_explicit_bind_cannot_bypass_old_schedule_guard(self):
        await self.service.current_life(self.binding)
        self.integrations.binding["persona_id"] = "new-role"
        await self.service.bind(self.event)
        current = self.store.get_value("binding")
        self.assertEqual(current["persona_id"], "new-role")
        self.assertEqual(await self.service.current_life(current), {})

    async def test_missing_schedule_during_role_change_does_not_block_manual_photos(self):
        self.integrations.binding["persona_id"] = "new-role"
        self.integrations.life_context.side_effect = RuntimeError("schedule plugin unavailable")
        await self.service.publish_from_chat(self.event, "用户指定文案")
        self.transport.publish.assert_awaited_once()
        self.assertEqual(self.store.get_post("sent-post")["binding"]["persona_id"], "new-role")

    def historical_snapshot(self, tid):
        return {
            "post": self.store.get_post(tid),
            "operation": self.store.get_operation("seed:" + tid),
            "memory": self.store.get_outbox(f"qzone:99:{tid}:publish")["payload"],
            "self_like": self.store.get_outbox(f"qzone:99:{tid}:self_like")["payload"],
        }

    def seed_pending_comment(self, tid, cid, *, seconds_ago=30):
        raw = self.comment(cid)
        raw["created_at"] = self.now.timestamp() - seconds_ago
        key = f"{tid}:-:{cid}:42"
        self.store.record_comment(
            {
                "key": key,
                "tid": tid,
                "comment_id": cid,
                "parent_id": None,
                "author_qq": "42",
                "author_name": "管理员",
                "content": raw["content"],
                "created_at": raw["created_at"],
                "binding": self.store.get_post(tid)["binding"],
                "raw": raw,
            }
        )
        return key

    async def test_binding_for_follows_selected_persona_and_cancels_only_pending_plans(self):
        self.seed_post("historical")
        historical = self.historical_snapshot("historical")
        pending = self.seed_due_plan("old-role-pending", 10)
        completed = self.seed_due_plan("old-role-completed", 20)
        self.store.finish_plan(completed["plan_id"], "done", "historical")
        self.integrations.binding["persona_id"] = "new-role"

        binding = await self.service.binding_for(self.event)

        self.assertEqual(binding["persona_id"], "new-role")
        self.assertEqual(self.store.get_value("binding"), binding)
        self.assertEqual(self.store.get_plan(pending["plan_id"])["status"], "cancelled")
        self.assertEqual(self.store.get_plan(completed["plan_id"])["status"], "done")
        self.assertEqual(self.historical_snapshot("historical"), historical)

    async def test_scope_change_also_refreshes_binding_and_cancels_pending_plan(self):
        pending = self.seed_due_plan("old-scope-pending", 10)
        self.integrations.binding["scope"] = "shared:new-scope"

        binding = await self.service.binding_for(self.event)

        self.assertEqual(binding["persona_id"], "role")
        self.assertEqual(binding["scope"], "shared:new-scope")
        self.assertEqual(self.store.get_value("binding"), binding)
        self.assertEqual(self.store.get_plan(pending["plan_id"])["status"], "cancelled")

    async def test_background_tick_refreshes_before_passing_binding_to_workflows(self):
        self.seed_post("historical")
        historical = self.historical_snapshot("historical")
        pending = self.seed_due_plan("tick-old-role", 10)
        self.integrations.binding.update(persona_id="new-role", scope="shared:new-scope")
        self.service.run_autonomous = AsyncMock()
        self.service.poll_comments = AsyncMock()
        self.service.process_outbox = AsyncMock()

        await self.service.tick()

        current = self.store.get_value("binding")
        self.assertEqual(current["persona_id"], "new-role")
        self.assertEqual(current["scope"], "shared:new-scope")
        self.service.run_autonomous.assert_awaited_once()
        event, binding = self.service.run_autonomous.await_args.args
        self.assertEqual(binding, current)
        self.assertEqual(event.unified_msg_origin, current["umo"])
        self.service.poll_comments.assert_awaited_once_with(event, current)
        self.assertEqual(self.store.get_plan(pending["plan_id"])["status"], "cancelled")
        self.assertEqual(self.historical_snapshot("historical"), historical)

    def test_planning_fingerprint_changes_when_selected_persona_or_scope_changes(self):
        original = self.service.plan_fingerprint(self.integrations.life)
        new_binding = {**self.binding, "persona_id": "new-role"}
        self.store.set_value("binding", new_binding)
        new_role = self.service.plan_fingerprint(self.integrations.life)
        self.assertNotEqual(original, new_role)
        self.store.set_value("binding", {**new_binding, "scope": "shared:new-scope"})
        new_scope = self.service.plan_fingerprint(self.integrations.life)
        self.assertNotEqual(new_role, new_scope)

    async def test_old_role_and_old_scope_backlogs_leave_reply_budget_for_current_role(self):
        self.service.settings.max_replies_per_poll = 1
        self.seed_post("old-role")
        old_role_key = self.seed_pending_comment("old-role", "1", seconds_ago=120)
        self.remote_details["old-role"]["comments"] = [self.comment("new-old-role-comment")]
        self.binding = {**self.binding, "persona_id": "new-role"}
        self.seed_post("old-scope")
        old_scope_key = self.seed_pending_comment("old-scope", "2", seconds_ago=90)
        self.remote_details["old-scope"]["comments"] = [self.comment("new-old-scope-comment")]
        self.integrations.binding.update(persona_id="new-role", scope="shared:new-scope")
        current = await self.service.binding_for(self.event)
        self.binding = current
        self.seed_post("current-role")
        self.remote_details["current-role"]["comments"] = [self.comment("3")]
        old_role = self.store.get_comment(old_role_key)
        old_scope = self.store.get_comment(old_scope_key)

        await self.service.poll_comments(self.event, current)

        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.transport.reply.await_args.args[0], "current-role")
        self.assertEqual(self.store.list_comments("current-role")[0]["status"], "replied")
        self.assertEqual(self.store.get_comment(old_role_key), old_role)
        self.assertEqual(self.store.get_comment(old_scope_key), old_scope)
        self.assertEqual(len(self.store.list_comments("old-role")), 1)
        self.assertEqual(len(self.store.list_comments("old-scope")), 1)
        self.assertEqual(
            [call.args[0] for call in self.transport.get_post.await_args_list], ["current-role"]
        )

    async def test_verification_keeps_old_snapshot_strict_after_persona_changes(self):
        original = copy.deepcopy(self.binding)
        self.integrations.binding["persona_id"] = "new-role"

        with self.assertRaises((ValueError, fixtures.service_module.RequestProblem)):
            await self.service.verify_binding(self.event, original)

        self.assertEqual(original, self.binding)
        self.transport.publish.assert_not_awaited()
        self.transport.reply.assert_not_awaited()
        self.transport.delete.assert_not_awaited()

    async def test_other_admin_old_scope_backlog_leaves_budget_for_current_session(self):
        self.context.config["admins_id"] = ["42", "77"]
        self.service.settings.max_replies_per_poll = 1
        primary = copy.deepcopy(self.binding)
        self.binding = {
            **primary,
            "umo": "qq:FriendMessage:77",
            "admin_id": "77",
            "scope": "session:77",
        }
        self.seed_post("other-admin-old-scope")
        old_key = self.seed_pending_comment("other-admin-old-scope", "1", seconds_ago=120)
        original = self.historical_snapshot("other-admin-old-scope")
        old_comment = self.store.get_comment(old_key)
        # A host-wide scope change applies to both administrator sessions.
        self.integrations.binding["scope"] = "shared:new-global-scope"
        current = await self.service.binding_for(self.event)
        self.binding = current
        self.seed_post("current-session")
        self.remote_details["current-session"]["comments"] = [self.comment("2")]

        await self.service.poll_comments(self.event, current)

        self.transport.reply.assert_awaited_once()
        self.assertEqual(self.transport.reply.await_args.args[0], "current-session")
        self.assertEqual(self.store.list_comments("current-session")[0]["status"], "replied")
        self.assertEqual(self.store.get_comment(old_key), old_comment)
        self.assertEqual(self.historical_snapshot("other-admin-old-scope"), original)
        self.assertEqual(
            [call.args[0] for call in self.transport.get_post.await_args_list], ["current-session"]
        )

    async def test_deleting_old_persona_post_keeps_its_original_memory_owner(self):
        self.seed_post("historical")
        original = copy.deepcopy(self.binding)
        publish_operation = self.store.get_operation("seed:historical")
        self.integrations.binding.update(persona_id="new-role", scope="shared:new-scope")

        await self.service.delete_post(self.event, "historical")
        # The deletion event follows the publication memory in the durable outbox.
        await self.service.process_outbox()

        self.transport.delete.assert_awaited_once_with("historical")
        current = self.store.get_value("binding")
        self.assertEqual(current["persona_id"], "new-role")
        post = self.store.get_post("historical")
        self.assertEqual(post["status"], "deleted")
        self.assertEqual(post["binding"], original)
        self.assertEqual(self.store.get_operation("seed:historical"), publish_operation)
        operation = self.store.get_operation(self.service.message_key(self.event, "delete"))
        self.assertEqual(operation["status"], "done")
        self.assertEqual(operation["payload"]["post"]["binding"], original)
        self.assertEqual(operation["result"]["binding"], original)
        for kind in ("publish", "delete"):
            memory = self.store.get_outbox(f"qzone:99:historical:{kind}")
            self.assertEqual(memory["payload"]["binding"], original)
            self.assertEqual(memory["payload"]["post"]["binding"], original)
        deletion_memories = [
            call.args[1]
            for call in self.integrations.sync_memory.await_args_list
            if call.args[1]["kind"] == "delete"
        ]
        self.assertEqual(len(deletion_memories), 1)
        self.assertEqual(deletion_memories[0]["binding"], original)

    async def test_other_session_resolves_primary_session_without_replacing_its_identity(self):
        other_event = fixtures.FakeEvent(self.bot, sender="77")
        factory = Mock(
            side_effect=lambda context, binding: fixtures.FakeEvent(
                self.bot, sender=binding["admin_id"], self_id=binding["bot_id"]
            )
        )
        self.service.event_factory = factory

        async def resolve(event, expected_persona_id=None):
            persona = "primary-new-role"
            if expected_persona_id and expected_persona_id != persona:
                raise ValueError("角色不一致")
            return {
                **copy.deepcopy(self.binding),
                "umo": event.unified_msg_origin,
                "admin_id": event.get_sender_id(),
                "persona_id": persona,
                "scope": "session:" + event.get_sender_id(),
            }

        self.integrations.resolve_binding = AsyncMock(side_effect=resolve)

        other = await self.service.binding_for(other_event)

        self.assertEqual(other["umo"], other_event.unified_msg_origin)
        self.assertEqual(other["persona_id"], "primary-new-role")
        self.assertEqual(other["scope"], "session:77")
        saved = self.store.get_value("binding")
        self.assertEqual(saved["umo"], self.binding["umo"])
        self.assertEqual(saved["admin_id"], "42")
        self.assertEqual(saved["persona_id"], "primary-new-role")
        self.assertEqual(saved["scope"], "session:42")
        factory.assert_called()
        resolved_umos = {
            call.args[0].unified_msg_origin
            for call in self.integrations.resolve_binding.await_args_list
        }
        self.assertEqual(resolved_umos, {self.binding["umo"], other_event.unified_msg_origin})


if __name__ == "__main__":
    unittest.main()
