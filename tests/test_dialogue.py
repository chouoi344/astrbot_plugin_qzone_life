"""Tool facts preserve context; the plugin never authors the chat reply."""

import ast
import copy
import inspect
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from astrbot_plugin_qzone_life.core.dialogue import (
    CHAT_RULE,
    Outcome,
    RequestProblem,
    exception_outcome,
)


class DialogueTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1] / "main.py"
        cls.tree = ast.parse(source.read_text(encoding="utf-8"))
        node = copy.deepcopy(next(n for n in cls.tree.body if isinstance(n, ast.ClassDef)))
        node.bases = []
        node.body = [
            n
            for n in node.body
            if isinstance(n, ast.AsyncFunctionDef) and n.name in ("_run", "space_dialogue_request")
        ]
        for method in node.body:
            method.decorator_list = []
        namespace = {
            "inspect": inspect,
            "Outcome": Outcome,
            "exception_outcome": exception_outcome,
            "CHAT_RULE": CHAT_RULE,
            "AstrMessageEvent": object,
            "logger": Mock(),
        }
        exec(
            compile(
                ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                str(source),
                "exec",
            ),
            namespace,
        )
        cls.plugin_class = namespace["QzoneLifePlugin"]

    def plugin(self):
        plugin = self.plugin_class()
        plugin.service = SimpleNamespace(llm=AsyncMock())
        return plugin

    async def test_tool_returns_facts_without_generating_or_sending_a_reply(self):
        plugin = self.plugin()
        event = SimpleNamespace(send=AsyncMock(), set_result=Mock(), set_extra=Mock())
        outcome = Outcome(
            "private diagnostic",
            action="publish",
            status="done",
            facts={"result": "照片已发布"},
            refs={"tid": "private-tid", "operation": "private-operation"},
        )
        data = json.loads(await plugin._run(event, lambda: outcome, action="publish"))
        self.assertEqual(data["facts"], outcome.facts)
        self.assertNotIn("private", json.dumps(data))
        plugin.service.llm.assert_not_awaited()
        event.send.assert_not_awaited()
        event.set_result.assert_not_called()
        event.set_extra.assert_not_called()

    async def test_original_persona_prompt_history_and_memory_remain_in_request(self):
        plugin = self.plugin()
        event = SimpleNamespace(get_platform_name=lambda: "aiocqhttp")
        contexts = [{"role": "user", "content": "关系前情"}]
        request = SimpleNamespace(
            system_prompt="原角色人设与本轮召回记忆", contexts=contexts, prompt="用户自己的话"
        )
        await plugin.space_dialogue_request(event, request)
        self.assertTrue(request.system_prompt.startswith("原角色人设与本轮召回记忆"))
        self.assertIs(request.contexts, contexts)
        self.assertEqual(request.prompt, "用户自己的话")
        self.assertNotIn("只回答结果", request.system_prompt)
        self.assertNotIn("简短", request.system_prompt)

    async def test_other_platform_prompt_untouched(self):
        req = SimpleNamespace(system_prompt="original")
        await self.plugin().space_dialogue_request(
            SimpleNamespace(get_platform_name=lambda: "webchat"), req
        )
        self.assertEqual(req.system_prompt, "original")

    async def test_raw_upstream_exception_stays_out_of_conversation(self):
        async def failed():
            raise RuntimeError("secret-token C:/private/config.json")

        result = json.loads(await self.plugin()._run(object(), failed, action="publish"))
        self.assertNotIn("secret", json.dumps(result))
        self.assertEqual(result["status"], "uncertain")
        self.assertNotIn("fallback", json.dumps(result))

    def test_safe_input_reasons_remain_specific(self):
        for code in ("photo_required", "caption_invalid", "binding_required", "binding_changed"):
            problem = RequestProblem(code, {"result": "受控的执行事实"}, status="needs_setup")
            result = exception_outcome(problem, action="publish")
            self.assertEqual(result.facts["reason"], code)
            self.assertEqual(result.status, "needs_setup")

    def test_read_failure_does_not_turn_known_counts_into_failed_writes(self):
        result = exception_outcome(RuntimeError("secret"), action="list")
        self.assertEqual(result.action, "list")
        self.assertEqual(result.facts["reason"], "read_unavailable")
        self.assertIn("先前已确认", result.facts["result"])

    def test_no_fixed_reply_rewriter_or_command_entry_points(self):
        commands = []
        handlers = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.AsyncFunctionDef):
                for d in node.decorator_list:
                    if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute):
                        if d.func.attr == "command":
                            commands.append(node.name)
                        if d.func.attr in ("on_decorating_result", "on_llm_response"):
                            handlers.append(node.name)
        self.assertEqual(commands, [])
        self.assertEqual(handlers, [])
        self.assertFalse(
            hasattr(Outcome("ok", action="publish", status="done", facts={}), "fallback")
        )

    def test_all_eight_natural_tools_remain_available(self):
        names = {
            d.keywords[0].value.value
            for node in ast.walk(self.tree)
            if isinstance(node, ast.AsyncFunctionDef)
            for d in node.decorator_list
            if isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "llm_tool"
        }
        self.assertEqual(
            names,
            {
                "qzone_publish_photo",
                "qzone_delete_post",
                "qzone_list_own_posts",
                "qzone_space_status",
                "qzone_prepare_space",
                "qzone_pause_or_resume",
                "qzone_retry_last_photo",
                "qzone_check_pending",
            },
        )


if __name__ == "__main__":
    unittest.main()
