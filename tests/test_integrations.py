from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "core" / "integrations.py"
SPEC = importlib.util.spec_from_file_location("qzone_life_integrations_test", MODULE_PATH)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class Event:
    unified_msg_origin = "qq:FriendMessage:123"

    def get_self_id(self):
        return "456"

    def get_sender_id(self):
        return "123"

    def get_group_id(self):
        return ""

    def get_platform_name(self):
        return "aiocqhttp"

    def get_messages(self):
        return []

    def get_extra(self, key, default=None):
        return default

    async def send(self, *args):
        raise AssertionError("integration must not send chat messages")


class Cursor:
    def __init__(self, cursor):
        self.cursor = cursor

    async def fetchall(self):
        return self.cursor.fetchall()

    async def close(self):
        self.cursor.close()


class ReadConnection:
    def __init__(self, db):
        self.db = db
        self.fail = False

    async def execute(self, statement, args):
        if self.fail:
            raise OSError("database unavailable")
        if not statement.startswith("SELECT"):
            raise AssertionError("bridge must not write directly to the database")
        return Cursor(self.db.execute(statement, args))


class Engine:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE documents(id INTEGER PRIMARY KEY AUTOINCREMENT,text TEXT,metadata TEXT)"
        )
        self.db_connection = ReadConnection(self.db)
        self.add_calls = 0
        self.replace_calls = 0
        self.fail_replace_number = None

    async def add_memory(
        self, *, content, session_id, persona_id, importance, metadata, atoms=None
    ):
        self.add_calls += 1
        full = dict(metadata, session_id=session_id, persona_id=persona_id, importance=importance)
        cursor = self.db.execute(
            "INSERT INTO documents(text,metadata) VALUES (?,?)", (content, json.dumps(full))
        )
        self.db.commit()
        return cursor.lastrowid

    async def replace_memory(self, old, *, content, metadata, importance):
        self.replace_calls += 1
        if self.replace_calls == self.fail_replace_number:
            raise OSError("temporary replacement failure")
        metadata = dict(metadata, previous_id=old)
        result = await self.add_memory(
            content=content,
            session_id=metadata["session_id"],
            persona_id=metadata["persona_id"],
            importance=importance,
            metadata=metadata,
        )
        self.db.execute("DELETE FROM documents WHERE id=?", (old,))
        self.db.commit()
        return result

    def rows(self):
        return [dict(row) for row in self.db.execute("SELECT * FROM documents ORDER BY id")]


class Processor:
    def build_memory_from_structured_data(
        self, *, structured_data, is_group_chat, fallback_excerpt
    ):
        data = structured_data
        metadata = {
            "persona_summary": data["summary"],
            "canonical_summary": data["summary"],
            "topics": data["topics"],
            "key_facts": data["key_facts"],
        }
        return data["summary"], metadata, data["importance"]

    def classify_atoms_from_metadata(self, **kwargs):
        return []


class Storage:
    def __init__(self):
        self.values = {}
        self.error = None

    async def get_async(self, *, scope, scope_id, key, default):
        if self.error:
            raise self.error
        return self.values.get(key, default)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = Engine()
        self.storage = Storage()
        self.event = Event()
        self.scope = "livingmemory:user:aiocqhttp:123"
        self.persona_id = "角色 A"
        self.allowed = True
        peer_class = type("LivingMemoryPlugin", (), {"__module__": "test_lmem.main"})
        self.peer = peer_class()
        self.peer.config_manager = {}
        self.peer.initializer = types.SimpleNamespace(
            is_initialized=True, memory_engine=self.engine, memory_processor=Processor()
        )
        self.peers = {
            "astrbot_plugin_livingmemory": types.SimpleNamespace(activated=True, star_cls=self.peer)
        }
        self.context = types.SimpleNamespace(get_registered_star=lambda name: self.peers.get(name))
        scope_module = types.ModuleType("test_lmem.core.memory_scope")
        scope_module.is_event_memory_allowed = lambda cfg, event: self.allowed
        scope_module.resolve_memory_scope = lambda cfg, event: self.scope
        utils = types.ModuleType("test_lmem.core.utils")
        utils.sp = self.storage

        async def get_persona_id(context, event):
            return self.persona_id

        utils.get_persona_id = get_persona_id
        api = types.ModuleType("astrbot.api")
        api.sp = self.storage
        self.module_patch = patch.dict(
            sys.modules,
            {
                "test_lmem.core.memory_scope": scope_module,
                "test_lmem.core.utils": utils,
                "astrbot.api": api,
            },
        )
        self.module_patch.start()
        self.bridge = mod.Integrations(self.context)

    def tearDown(self):
        self.module_patch.stop()
        self.engine.db.close()

    async def payload(self, kind="publish"):
        binding = await self.bridge.resolve_binding(self.event)
        return {
            "event_key": f"post:abc:{kind}",
            "kind": kind,
            "tid": "abc",
            "binding": binding,
            "occurred_at": "2026-10-24T09:30:00+08:00",
            "post": {
                "tid": "abc",
                "content": "晨光很好",
                "image_summary": "白色外套，公园树下",
                "source": "quoted_photo",
                "images": ["https://example.invalid/image"],
                "binding": binding,
            },
            "comment": {"content": "好好看", "author_qq": "123", "author_name": "管理员"},
            "reply": {"content": "特意拍给你看的"},
        }

    async def test_repeated_event_is_reconciled_even_with_fresh_adapter(self):
        payload = await self.payload()
        first = await self.bridge.sync_memory(self.event, payload)
        second = await mod.Integrations(self.context).sync_memory(self.event, payload)
        self.assertEqual(first["memory_id"], second["memory_id"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(self.engine.add_calls, 1)
        row = self.engine.rows()[0]
        self.assertIn("管理员提供的照片", row["text"])
        self.assertIn("白色外套", row["text"])
        self.assertNotIn("https://", row["text"])
        self.assertEqual(json.loads(row["metadata"])["session_id"], self.scope)

    async def test_independent_same_text_posts_are_not_merged(self):
        first = await self.payload()
        second = copy.deepcopy(first)
        second.update(event_key="post:def:publish", tid="def")
        second["post"]["tid"] = "def"
        await self.bridge.sync_memory(self.event, first)
        await self.bridge.sync_memory(self.event, second)
        self.assertEqual(len(self.engine.rows()), 2)

    async def test_changed_scope_and_persona_do_not_silently_rebind(self):
        payload = await self.payload()
        self.scope = "livingmemory:global"
        with self.assertRaises(mod.BindingMismatch):
            await self.bridge.sync_memory(self.event, payload)
        self.scope = payload["binding"]["scope"]
        self.persona_id = "角色 B"
        with self.assertRaises(mod.BindingMismatch):
            await self.bridge.sync_memory(self.event, payload)
        self.assertEqual(self.engine.add_calls, 0)

    async def test_whitelist_initialization_and_disable_are_enforced(self):
        self.allowed = False
        with self.assertRaises(mod.AccessDenied):
            await self.bridge.resolve_binding(self.event)
        self.allowed = True
        self.peer.initializer.is_initialized = False
        with self.assertRaises(mod.DependencyUnavailable):
            await self.bridge.resolve_binding(self.event)
        self.peer.initializer.is_initialized = True
        self.peers["astrbot_plugin_livingmemory"].activated = False
        with self.assertRaises(mod.DependencyUnavailable):
            await self.bridge.resolve_binding(self.event)

    async def test_session_disabled_and_unreadable_settings_block_writes(self):
        self.storage.values["session_plugin_config"] = {
            self.event.unified_msg_origin: {"disabled_plugins": ["LivingMemory"]}
        }
        with self.assertRaises(mod.AccessDenied):
            await self.bridge.resolve_binding(self.event)
        self.storage.values.clear()
        self.storage.error = OSError("storage offline")
        with self.assertRaises(OSError):
            await self.bridge.resolve_binding(self.event)

    async def test_qzone_disabled_in_bound_session_blocks_background_binding(self):
        self.storage.values["session_plugin_config"] = {
            self.event.unified_msg_origin: {"disabled_plugins": ["astrbot_plugin_qzone_life"]}
        }
        with self.assertRaises(mod.AccessDenied):
            await self.bridge.resolve_binding(self.event)

    async def test_failed_reconciliation_is_not_treated_as_new_memory(self):
        payload = await self.payload()
        self.engine.db_connection.fail = True
        with self.assertRaises(OSError):
            await self.bridge.sync_memory(self.event, payload)
        self.assertEqual(self.engine.add_calls, 0)

    async def test_delete_revises_post_comment_reply_and_keeps_history(self):
        old_ids = []
        for kind in ("publish", "comment", "reply"):
            result = await self.bridge.sync_memory(self.event, await self.payload(kind))
            old_ids.append(result["memory_id"])
        result = await self.bridge.sync_memory(self.event, await self.payload("delete"))
        self.assertEqual(set(result["updated_memory_ids"]), {str(x) for x in old_ids})
        rows = self.engine.rows()
        self.assertEqual(len(rows), 4)
        self.assertTrue(
            all(json.loads(row["metadata"])["qzone_post_status"] == "deleted" for row in rows)
        )
        for row in rows:
            metadata = json.loads(row["metadata"])
            self.assertIn("删除", metadata["persona_summary"])
            self.assertEqual(metadata["persona_summary"], metadata["canonical_summary"])
        repeated = await self.bridge.sync_memory(self.event, await self.payload("delete"))
        self.assertEqual(result["memory_id"], repeated["memory_id"])
        self.assertEqual(len(self.engine.rows()), 4)
        self.assertEqual(self.engine.replace_calls, 3)

    async def test_partial_deletion_retry_skips_done_revisions_and_recovers_mapping(self):
        published = await self.bridge.sync_memory(self.event, await self.payload())
        await self.bridge.sync_memory(self.event, await self.payload("comment"))
        deletion = await self.payload("delete")
        self.engine.fail_replace_number = 2
        with self.assertRaises(OSError):
            await self.bridge.sync_memory(self.event, deletion)
        self.engine.fail_replace_number = None
        resumed = await mod.Integrations(self.context).sync_memory(self.event, deletion)
        self.assertIn(str(published["memory_id"]), resumed["updated_memory_ids"])
        self.assertEqual(len(self.engine.rows()), 3)
        self.assertEqual(self.engine.replace_calls, 3)

    async def test_late_comment_after_deletion_is_marked_historical(self):
        await self.bridge.sync_memory(self.event, await self.payload("delete"))
        await self.bridge.sync_memory(self.event, await self.payload("comment"))
        row = self.engine.rows()[-1]
        self.assertIn("删除前的历史记录", row["text"])
        self.assertEqual(json.loads(row["metadata"])["qzone_post_status"], "deleted")

    async def test_scope_none_uses_upstream_write_fallback_not_new_global_scope(self):
        self.scope = None
        binding = await self.bridge.resolve_binding(self.event)
        self.assertEqual(binding["scope"], self.event.unified_msg_origin)

    async def test_external_reply_preserves_actual_comment_and_reply_times(self):
        payload = await self.payload("reply")
        payload["post"]["source"] = "external"
        payload["comment"]["created_at"] = "2026-10-23T08:00:00+08:00"
        payload["reply"]["created_at"] = "2026-10-24T09:30:00+08:00"
        await self.bridge.sync_memory(self.event, payload)
        text = self.engine.rows()[0]["text"]
        self.assertIn("既有 QQ空间说说", text)
        self.assertIn("2026-10-23T08:00:00+08:00", text)
        self.assertIn("于2026-10-24T09:30:00+08:00实际回复", text)
        self.assertNotIn("制作了自拍", text)

    async def test_life_read_does_not_generate_or_retry_legacy_method(self):
        life = types.SimpleNamespace(get_life_context=AsyncMock(return_value={"outfit": "外套"}))
        self.peers["astrbot_plugin_life_scheduler"] = types.SimpleNamespace(
            star_cls=life, activated=True
        )
        self.assertEqual(await self.bridge.life_context(), {"outfit": "外套"})
        life.get_life_context.assert_awaited_once_with(allow_generate=False)
        calls = []

        async def legacy():
            calls.append(True)
            return {}

        life.get_life_context = legacy
        with self.assertRaises(mod.IntegrationIncompatible):
            await self.bridge.life_context()
        self.assertEqual(calls, [])

    async def test_selfie_returns_file_without_chat_delivery(self):
        with tempfile.TemporaryDirectory() as folder:
            image = Path(folder) / "selfie.png"
            image.write_bytes(b"test fixture")
            generate = AsyncMock(return_value=(image, {"mode": "selfie_ref"}))
            peer = types.SimpleNamespace(_generate_selfie_image_with_meta=generate)
            self.peers["astrbot_plugin_gitee_aiimg"] = types.SimpleNamespace(
                star_cls=peer, activated=True
            )
            result, metadata = await self.bridge.selfie(self.event, "公园中的白外套自拍")
            self.assertEqual(result, image)
            self.assertEqual(metadata["mode"], "selfie_ref")
            generate.assert_awaited_once_with(self.event, "公园中的白外套自拍", backend=None)

    async def test_selfie_obeys_existing_provider_concurrency_limit(self):
        generate = AsyncMock()
        begin = AsyncMock(return_value=False)
        end = AsyncMock()
        peer = types.SimpleNamespace(
            _generate_selfie_image_with_meta=generate, _begin_user_job=begin, _end_user_job=end
        )
        self.peers["astrbot_plugin_gitee_aiimg"] = types.SimpleNamespace(
            star_cls=peer, activated=True
        )
        with self.assertRaises(mod.DependencyUnavailable):
            await self.bridge.selfie(self.event, "自拍")
        begin.assert_awaited_once_with("123", kind="image")
        generate.assert_not_awaited()
        end.assert_not_awaited()

    async def test_selfie_releases_provider_slot_on_success_failure_and_cancel(self):
        with tempfile.TemporaryDirectory() as folder:
            image = Path(folder) / "selfie.png"
            image.write_bytes(b"fixture")
            begin = AsyncMock(return_value=True)
            end = AsyncMock()
            generate = AsyncMock(return_value=(image, {}))
            peer = types.SimpleNamespace(
                _generate_selfie_image_with_meta=generate, _begin_user_job=begin, _end_user_job=end
            )
            self.peers["astrbot_plugin_gitee_aiimg"] = types.SimpleNamespace(
                star_cls=peer, activated=True
            )
            await self.bridge.selfie(self.event, "自拍")
            generate.side_effect = OSError("provider failed")
            with self.assertRaises(OSError):
                await self.bridge.selfie(self.event, "自拍")
            generate.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await self.bridge.selfie(self.event, "自拍")
            self.assertEqual(begin.await_count, 3)
            self.assertEqual(end.await_count, 3)
            end.assert_awaited_with("123", kind="image")

    async def test_life_default_persona_must_match_bound_role(self):
        getter = AsyncMock(return_value={"name": "角色 A", "prompt": "人设"})
        self.context.persona_manager = types.SimpleNamespace(get_default_persona_v3=getter)
        binding = {"persona_id": "角色 A"}
        await self.bridge.check_life_persona(binding)
        getter.assert_awaited_once_with()
        getter.return_value = {"name": "角色 B"}
        with self.assertRaises(mod.BindingMismatch):
            await self.bridge.check_life_persona(binding)

    async def test_persona_uses_real_host_selection(self):
        resolver = AsyncMock(return_value=("角色 A", {"prompt": "角色设定原文"}, [], [], {}))
        self.context.persona_manager = types.SimpleNamespace(resolve_selected_persona=resolver)
        self.context.conversation_manager = types.SimpleNamespace(
            get_curr_conversation_id=AsyncMock(return_value="conversation"),
            get_conversation=AsyncMock(return_value=types.SimpleNamespace(persona_id="会话人设")),
        )
        self.context.get_config = lambda umo: {"provider_settings": {"a": 1}}
        self.assertEqual(await self.bridge.persona(self.event), ("角色 A", "角色设定原文"))
        self.assertEqual(resolver.call_args.kwargs["conversation_persona_id"], "会话人设")

    async def test_missing_dependency_does_not_hide_failure(self):
        self.peers.clear()
        with self.assertRaises(mod.DependencyUnavailable):
            await self.bridge.life_context()
        with self.assertRaises(mod.DependencyUnavailable):
            await self.bridge.resolve_binding(self.event)


if __name__ == "__main__":
    unittest.main()
