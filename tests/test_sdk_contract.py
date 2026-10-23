"""Optional offline contract checks against pinned, unmodified AstrBot source.

Set ASTRBOT_COMMAND_SOURCE to a local copy of the upstream command.py. This
test AST-extracts the actual parsing methods, without importing AstrBot or
reimplementing its parser. The upstream source is not bundled in this plugin.

Source: AstrBotDevs/AstrBot, tag v4.26.8, AGPL-3.0
Commit: 60c9e68d50dc9b9ed58503f21a0b77a8d0bd2159
Path: astrbot/core/star/filter/command.py
https://github.com/AstrBotDevs/AstrBot/blob/60c9e68d50dc9b9ed58503f21a0b77a8d0bd2159/astrbot/core/star/filter/command.py
"""

import ast
import copy
import hashlib
import inspect
import os
from pathlib import Path
import types
import typing
import unittest


class SDKCommandContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_path = os.environ.get("ASTRBOT_COMMAND_SOURCE")
        if not source_path:
            raise unittest.SkipTest(
                "Set ASTRBOT_COMMAND_SOURCE for the optional upstream SDK contract checks"
            )
        source = Path(source_path).read_text(encoding="utf-8").rstrip("\n") + "\n"
        raw = source.encode("utf-8")
        blob = hashlib.sha1(b"blob " + str(len(raw)).encode("ascii") + b"\0" + raw).hexdigest()
        if blob != "abf339fda7f0740f542a91cd4adb859d5ef6ed5f":
            raise AssertionError("SDK reference differs from the audited v4.26.8 command.py")

        upstream = ast.parse(source)
        selected = [
            copy.deepcopy(node)
            for node in upstream.body
            if isinstance(node, (ast.ClassDef, ast.FunctionDef))
            and node.name in ("GreedyStr", "unwrap_optional")
        ]
        command = next(
            node
            for node in upstream.body
            if isinstance(node, ast.ClassDef) and node.name == "CommandFilter"
        )
        command = copy.deepcopy(command)
        command.bases = []
        command.body = [
            node
            for node in command.body
            if isinstance(node, ast.FunctionDef)
            and node.name in ("init_handler_md", "validate_and_convert_params", "print_types")
        ]
        selected.append(command)
        module = ast.Module(
            body=[
                ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
                *selected,
            ],
            type_ignores=[],
        )
        namespace = {"inspect": inspect, "types": types, "typing": typing, "Any": typing.Any}
        exec(compile(ast.fix_missing_locations(module), source_path, "exec"), namespace)
        cls.parser_type = namespace["CommandFilter"]
        cls.namespace = namespace

        main_path = Path(__file__).resolve().parents[1] / "main.py"
        plugin = next(
            node
            for node in ast.parse(main_path.read_text(encoding="utf-8")).body
            if isinstance(node, ast.ClassDef) and node.name == "QzoneLifePlugin"
        )
        cls.handlers = {
            node.name: node
            for node in plugin.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name in ("publish_space", "delete_space", "reconcile_space")
        }

    def parser(self, name, historical_empty_default=False):
        handler = copy.deepcopy(self.handlers[name])
        handler.decorator_list = []
        handler.body = [ast.Pass()]
        if historical_empty_default:
            handler.args.defaults = [ast.Constant(value="")]
        # Preserve the real entry point's signature, without loading plugin or SDK
        # services. Only inspect.signature is used on this function.
        namespace = {**self.namespace, "AstrMessageEvent": object}
        module = ast.Module(body=[handler], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), "main.py signature", "exec"), namespace)
        parser = self.parser_type()
        parser.init_handler_md(types.SimpleNamespace(handler=namespace[name]))
        return parser

    def parse(self, name, text):
        parser = self.parser(name)
        return parser.validate_and_convert_params(text.split(), parser.handler_params)

    def test_all_greedy_commands_accept_no_arguments(self):
        for name, parameter in (
            ("publish_space", "caption"),
            ("delete_space", "target"),
            ("reconcile_space", "arguments"),
        ):
            with self.subTest(handler=name):
                self.assertEqual({parameter: ""}, self.parse(name, ""))

    def test_caption_keeps_chinese_and_english_words(self):
        caption = "今天 在 coffee shop 喝咖啡"
        self.assertEqual({"caption": caption}, self.parse("publish_space", caption))

    def test_delete_target_keeps_words_separated_by_spaces(self):
        target = "今天 喝咖啡 的那条"
        self.assertEqual({"target": target}, self.parse("delete_space", target))

    def test_reconcile_receives_all_three_tokens(self):
        arguments = "publish:message-123 已成功 tid-456"
        parsed = self.parse("reconcile_space", arguments)["arguments"]
        self.assertEqual(["publish:message-123", "已成功", "tid-456"], parsed.split())

    def test_real_upstream_parser_exposes_the_previous_default_value_regression(self):
        parser = self.parser("reconcile_space", historical_empty_default=True)
        parsed = parser.validate_and_convert_params(
            ["publish:message-123", "已成功", "tid-456"], parser.handler_params
        )
        self.assertEqual({"arguments": "publish:message-123"}, parsed)


if __name__ == "__main__":
    unittest.main()
