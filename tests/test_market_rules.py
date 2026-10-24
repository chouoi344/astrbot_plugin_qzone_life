"""Market logging requirements apply to every shipped runtime module."""

import ast
import unittest
from pathlib import Path


def logging_violations(source):
    violations = []
    for node in ast.walk(ast.parse(source)):
        imports = []
        if isinstance(node, ast.Import):
            imports = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            imports = [node.module or ""]
            if any(alias.name == "logger" for alias in node.names):
                if node.module != "astrbot.api":
                    violations.append((node.lineno, "logger must come from astrbot.api"))
        for module in imports:
            if module.split(".", 1)[0] in {"logging", "loguru", "logbook", "structlog"}:
                violations.append((node.lineno, "use astrbot.api.logger"))
        if isinstance(node, ast.Call):
            name = node.func
            if (isinstance(name, ast.Attribute) and name.attr == "getLogger") or (
                isinstance(name, ast.Name) and name.id == "getLogger"
            ):
                violations.append((node.lineno, "do not create a separate logger"))
    return violations


class MarketLoggingRulesTests(unittest.TestCase):
    def test_all_runtime_modules_follow_host_logging_contract(self):
        root = Path(__file__).resolve().parents[1]
        paths = [root / "main.py", *(root / "core").rglob("*.py"),
                 *(root / "vendor").rglob("*.py")]
        violations = {
            str(path.relative_to(root)): found
            for path in paths
            if (found := logging_violations(path.read_text(encoding="utf-8")))
        }
        self.assertEqual(violations, {}, "All runtime logging must use astrbot.api.logger")

    def test_guard_detects_builtin_and_third_party_logging(self):
        for source in (
            "import logging as log",
            "from logging import warning",
            "from logging import getLogger\nlog = getLogger('astrbot')",
            "from loguru import logger",
            "import structlog",
        ):
            with self.subTest(source=source):
                self.assertTrue(logging_violations(source))

    def test_official_logger_and_its_alias_are_allowed(self):
        for source in (
            "from astrbot.api import logger\nlogger.warning('background failure')",
            "from astrbot.api import logger as log\nlog.warning('background failure')",
        ):
            self.assertEqual(logging_violations(source), [])
