"""Provider routing follows the installed AstrBot 4.29 configuration contract."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core.dialogue import RequestProblem
from astrbot_plugin_qzone_life.core.providers import resolve_provider_id


def provider(modalities):
    return SimpleNamespace(provider_config={"modalities": modalities}, text_chat=AsyncMock())


def context(*, caption="", current="session-chat", providers=None, fallbacks=None):
    config = {
        "provider_settings": {"default_image_caption_provider_id": caption},
        "agent_runner": {"config": {"model": {"fallback_provider_ids": fallbacks or []}}},
        # The builtin group-context feature's separate setting is unrelated.
        "provider_ltm_settings": {"image_caption_provider_id": "unrelated-group-model"},
    }
    return SimpleNamespace(
        get_config=Mock(return_value=config),
        get_current_chat_provider_id=AsyncMock(return_value=current),
        get_provider_by_id=Mock(side_effect=(providers or {}).get),
    )


class ProviderRoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.event = SimpleNamespace(unified_msg_origin="platform:FriendMessage:123")

    async def test_explicit_text_choice_does_not_read_system_or_session_route(self):
        ctx = context()
        self.assertEqual(
            await resolve_provider_id(ctx, self.event, text_provider_id=" selected-text "),
            "selected-text",
        )
        ctx.get_config.assert_not_called()
        ctx.get_current_chat_provider_id.assert_not_awaited()
        ctx.get_provider_by_id.assert_not_called()

    async def test_empty_text_choice_uses_the_current_conversation(self):
        ctx = context()
        self.assertEqual(await resolve_provider_id(ctx, self.event), "session-chat")
        ctx.get_current_chat_provider_id.assert_awaited_once_with(self.event.unified_msg_origin)
        ctx.get_config.assert_not_called()

    async def test_missing_chat_route_is_a_safe_controlled_fact(self):
        ctx = context()
        ctx.get_current_chat_provider_id.side_effect = ValueError("private-upstream-key")
        with self.assertRaises(RequestProblem) as caught:
            await resolve_provider_id(ctx, self.event)
        self.assertEqual(caught.exception.code, "chat_model_unavailable")
        self.assertNotIn("private-upstream-key", repr(caught.exception.facts))

    async def test_system_image_choice_uses_effective_session_config(self):
        ctx = context(caption="system-image", providers={"system-image": provider(["image"])})
        self.assertEqual(
            await resolve_provider_id(ctx, self.event, vision=True, text_provider_id="text-only"),
            "system-image",
        )
        ctx.get_config.assert_called_once_with(umo=self.event.unified_msg_origin)
        ctx.get_current_chat_provider_id.assert_not_awaited()
        ctx.get_provider_by_id.assert_called_once_with("system-image")

    async def test_configured_image_choice_matches_official_caption_route(self):
        # AstrBot's explicit caption route does not require modalities metadata.
        ctx = context(caption="system-image", providers={"system-image": provider(None)})
        self.assertEqual(await resolve_provider_id(ctx, self.event, vision=True), "system-image")

    async def test_unloaded_system_image_choice_does_not_silently_change_models(self):
        ctx = context(
            caption="unloaded-choice",
            providers={"session-chat": provider(["image"]), "fallback": provider(["image"])},
            fallbacks=["fallback"],
        )
        with self.assertRaises(RequestProblem) as caught:
            await resolve_provider_id(ctx, self.event, vision=True)
        self.assertEqual(caught.exception.code, "system_vision_unavailable")
        ctx.get_current_chat_provider_id.assert_not_awaited()
        ctx.get_provider_by_id.assert_called_once_with("unloaded-choice")

    async def test_non_chat_system_model_is_rejected_without_model_call(self):
        ctx = context(caption="speech-model", providers={"speech-model": object()})
        with self.assertRaises(RequestProblem):
            await resolve_provider_id(ctx, self.event, vision=True)

    async def test_empty_image_choice_uses_multimodal_session_model(self):
        ctx = context(providers={"session-chat": provider(["text", "image"])})
        self.assertEqual(await resolve_provider_id(ctx, self.event, vision=True), "session-chat")

    async def test_empty_modalities_keep_astrbot_migration_compatibility(self):
        ctx = context(providers={"session-chat": provider([])})
        self.assertEqual(await resolve_provider_id(ctx, self.event, vision=True), "session-chat")

    async def test_official_explicit_fallback_order_skips_missing_and_text_models(self):
        image = provider(["image"])
        ctx = context(
            providers={
                "session-chat": provider(["text"]),
                "text-fallback": provider(["text"]),
                "image-first": image,
                "image-later": provider(["image"]),
            },
            fallbacks=[None, "session-chat", "missing", "text-fallback", "image-first", "image-later"],
        )
        self.assertEqual(await resolve_provider_id(ctx, self.event, vision=True), "image-first")
        self.assertNotIn(unittest.mock.call("image-later"), ctx.get_provider_by_id.call_args_list)
        image.text_chat.assert_not_awaited()

    async def test_no_image_route_does_not_pick_text_override_or_group_context_model(self):
        ctx = context(
            providers={
                "session-chat": provider(["text"]),
                "plugin-override": provider(["image"]),
                "unrelated-group-model": provider(["image"]),
            }
        )
        with self.assertRaises(RequestProblem) as caught:
            await resolve_provider_id(ctx, self.event, vision=True, text_provider_id="plugin-override")
        self.assertEqual(caught.exception.code, "system_vision_unavailable")
        self.assertEqual(caught.exception.status, "needs_input")
        self.assertIn("尚未执行", caught.exception.facts["result"])
        ctx.get_provider_by_id.assert_called_once_with("session-chat")

    async def test_invalid_fallback_value_is_not_used_as_a_list(self):
        ctx = context(providers={"session-chat": provider(["text"])}, fallbacks="other")
        with self.assertRaises(RequestProblem):
            await resolve_provider_id(ctx, self.event, vision=True)
        ctx.get_provider_by_id.assert_called_once_with("session-chat")

    async def test_configuration_errors_do_not_reveal_raw_exceptions(self):
        ctx = context()
        ctx.get_config.side_effect = RuntimeError("secret-token-and-endpoint")
        with self.assertRaises(RequestProblem) as caught:
            await resolve_provider_id(ctx, self.event, vision=True)
        self.assertEqual(caught.exception.code, "system_vision_unavailable")
        self.assertNotIn("secret-token-and-endpoint", repr(caught.exception.facts))


if __name__ == "__main__":
    unittest.main()
