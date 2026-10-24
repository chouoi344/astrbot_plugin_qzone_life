"""Status captions use one coherent sentence; chat remains with its own model."""

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from astrbot_plugin_qzone_life.core.caption import generate_caption, single_sentence
from astrbot_plugin_qzone_life.core.dialogue import RequestProblem


class CaptionTests(unittest.IsolatedAsyncioTestCase):
    def test_fragmented_screenshot_and_multiline_are_rejected(self):
        self.assertFalse(single_sentence("顶层的风挺大。上来透口气，顺便喝杯冰的。"))
        self.assertFalse(single_sentence("顶层的风挺大\n上来透口气"))
        self.assertFalse(single_sentence(""))

    def test_connected_sentence_and_decimal_are_accepted(self):
        self.assertTrue(single_sentence("上顶层来吹吹风、透口气，顺便喝杯冰的。"))
        self.assertTrue(single_sentence("走了1.5公里，停下来喝杯咖啡。"))

    async def test_complete_sentence_with_decorative_emoji_is_preserved(self):
        text = "晚风把今天的疲惫吹散了。🍃"
        llm = AsyncMock(return_value=text)
        self.assertEqual(await generate_caption(llm, object(), "晚间露台"), text)
        llm.assert_awaited_once()
        self.assertTrue(single_sentence("「晚风很舒服。」🌙✨"))
        self.assertFalse(single_sentence("晚风很舒服。🌙 我想多待一会儿。"))

    def test_emoji_or_punctuation_alone_is_not_a_complete_sentence(self):
        for text in ("🍃", "✨🌙", "……", "！？", "1️⃣"):
            with self.subTest(text=text):
                self.assertFalse(single_sentence(text))

    def test_sentence_period_after_number_still_separates_sentences(self):
        self.assertFalse(single_sentence("I counted to 5. Then I stopped."))
        self.assertTrue(single_sentence("I walked 1.5 miles before stopping."))

    async def test_fragmented_output_is_regenerated_instead_of_punctuation_replaced(self):
        llm = AsyncMock(side_effect=["顶层的风挺大。上来透口气。", "上顶层吹吹风，顺便喝杯冰的。"])
        text = await generate_caption(llm, object(), "顶层露台，手中有冰饮", activity="露台休息")
        self.assertEqual(text, "上顶层吹吹风，顺便喝杯冰的。")
        self.assertEqual(llm.await_count, 2)
        self.assertIn("露台休息", llm.await_args.args[1])
        self.assertIn("聊天中的短句", llm.await_args.args[1])

    async def test_continued_bad_caption_leaves_publication_unattempted(self):
        llm = AsyncMock(return_value="风很大。喝杯冰的。")
        with self.assertRaises(RequestProblem) as caught:
            await generate_caption(llm, object(), "露台与饮料")
        self.assertEqual(caught.exception.code, "caption_not_one_sentence")
        self.assertEqual(llm.await_count, 2)

    async def test_provider_error_propagates_without_fixed_text_or_text_only_post(self):
        llm = AsyncMock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):
            await generate_caption(llm, object(), "照片摘要")
        llm.assert_awaited_once()
