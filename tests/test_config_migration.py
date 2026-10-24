"""Simplifying the form retires legacy behavior while preserving rollback data."""

import ast
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from astrbot_plugin_qzone_life.core.settings import normalize_config
from astrbot_plugin_qzone_life.core.configuration import migrate_legacy_config_file


class FakeStar:
    def __init__(self, context):
        self.context = context


class SavedConfig(dict):
    def __init__(self, values):
        super().__init__(values)
        self.save_config = Mock()


class ConfigMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data_dir = Path(self.temp.name) / "private-data"
        tree = ast.parse(
            (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        )
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
        cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "__init__"]
        self.service_factory = Mock()
        namespace = {
            "Star": FakeStar,
            "Context": object,
            "Path": Path,
            "json": json,
            "hashlib": hashlib,
            "normalize_config": normalize_config,
            "StarTools": SimpleNamespace(get_data_dir=lambda name: self.data_dir),
            "QzoneService": self.service_factory,
        }
        exec(
            compile(
                ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])),
                "actual plugin initialization",
                "exec",
            ),
            namespace,
        )
        self.plugin = namespace["QzoneLifePlugin"]

    def test_old_controls_are_removed_after_full_private_backup(self):
        original = {
            "auto_publish": False,
            "auto_reply": True,
            "daily_min": 1,
            "daily_max": 1,
            "active_start": 8,
            "active_end": 23,
            "selfie_timeout": 60,
            "persona_id": "old-role",
            "llm_provider_id": "old-provider",
        }
        config = SavedConfig(original)
        context = object()
        self.plugin(context, config)
        self.assertEqual(
            dict(config), {
                "auto_publish": False, "daily_post_limit": 1, "auto_reply": True,
                "llm_provider_id": "old-provider", "failure_policy": "稍后重试",
            }
        )
        config.save_config.assert_called_once()
        backups = list(self.data_dir.glob("settings-before-simplification-*.json"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(json.loads(backups[0].read_text(encoding="utf-8")), original)
        self.assertEqual(
            self.service_factory.call_args.args, (context, dict(config), self.data_dir)
        )

    def test_user_can_raise_the_new_slider_after_legacy_migration(self):
        config = SavedConfig({"daily_max": 1, "daily_post_limit": 2})
        self.plugin(object(), config)
        self.assertEqual(config["daily_post_limit"], 1)
        config["daily_post_limit"] = 2
        config.save_config.reset_mock()
        self.plugin(object(), config)
        self.assertEqual(config["daily_post_limit"], 2)
        config.save_config.assert_not_called()
        self.assertEqual(len(list(self.data_dir.glob("settings-before-simplification-*.json"))), 1)

    def test_already_simple_configuration_needs_no_migration_or_backup(self):
        config = SavedConfig({
            "auto_publish": True, "daily_post_limit": 2, "auto_reply": False,
            "llm_provider_id": "", "failure_policy": "稍后重试",
        })
        self.plugin(object(), config)
        config.save_config.assert_not_called()
        self.assertFalse(self.data_dir.exists())

    def test_backup_failure_leaves_original_configuration_intact(self):
        self.data_dir.write_text("occupied path", encoding="utf-8")
        original = {"daily_max": 1, "persona_id": "old-role"}
        config = SavedConfig(original)
        with self.assertRaises(FileExistsError):
            self.plugin(object(), config)
        self.assertEqual(dict(config), original)
        config.save_config.assert_not_called()
        self.service_factory.assert_not_called()

    def test_file_migration_preserves_legacy_maximum_before_host_loads_schema(self):
        path = Path(self.temp.name) / "plugin-config.json"
        original = b'\xef\xbb\xbf{"daily_min": 1, "daily_max": 1, "active_end": 23}'
        path.write_bytes(original)
        migrate_legacy_config_file(self.data_dir, config_path=path)
        self.assertEqual(
            json.loads(path.read_text(encoding="utf-8")),
            {
                "auto_publish": True, "daily_post_limit": 1, "auto_reply": True,
                "llm_provider_id": "", "failure_policy": "稍后重试",
            },
        )
        backup = next(self.data_dir.glob("settings-before-simplification-*.json"))
        self.assertEqual(backup.read_bytes(), original)
        migrate_legacy_config_file(self.data_dir, config_path=path)
        self.assertEqual(len(list(self.data_dir.glob("settings-before-simplification-*.json"))), 1)

    def test_file_migration_does_not_overwrite_original_when_backup_fails(self):
        self.data_dir.write_text("occupied", encoding="utf-8")
        path = Path(self.temp.name) / "plugin-config.json"
        path.write_bytes(b'{"daily_max": 1}')
        with self.assertRaises(FileExistsError):
            migrate_legacy_config_file(self.data_dir, config_path=path)
        self.assertEqual(path.read_bytes(), b'{"daily_max": 1}')

    def test_missing_text_choice_is_restored_from_private_backup_before_host_defaults(self):
        self.data_dir.mkdir()
        saved = self.data_dir / "settings-before-simplification-old.json"
        saved.write_text(json.dumps({"llm_provider_id": "my-space-model"}), encoding="utf-8")
        # Bad backups and recent three-choice backups carry no valid selection.
        (self.data_dir / "settings-before-simplification-bad.json").write_text("bad", encoding="utf-8")
        path = Path(self.temp.name) / "plugin-config.json"
        original = b'{"auto_publish": true, "daily_post_limit": 0, "auto_reply": true}'
        path.write_bytes(original)
        migrate_legacy_config_file(self.data_dir, config_path=path)
        migrated = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(migrated["llm_provider_id"], "my-space-model")
        self.assertEqual(migrated["daily_post_limit"], 0)
        self.assertTrue(migrated["auto_reply"])
        backups = list(self.data_dir.glob("settings-before-simplification-*.json"))
        self.assertTrue(any(p.read_bytes() == original for p in backups))
        migrated.update(llm_provider_id="", daily_post_limit=5, failure_policy="直接跳过")
        path.write_text(json.dumps(migrated), encoding="utf-8")
        migrate_legacy_config_file(self.data_dir, config_path=path)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), migrated)

    def test_explicit_text_choice_including_blank_takes_precedence_over_backups(self):
        self.data_dir.mkdir()
        (self.data_dir / "settings-before-simplification-old.json").write_text(
            '{"llm_provider_id": "old-model"}', encoding="utf-8",
        )
        path = Path(self.temp.name) / "plugin-config.json"
        for choice in ("", "new-model"):
            with self.subTest(choice=choice):
                path.write_text(json.dumps({"llm_provider_id": choice}), encoding="utf-8")
                migrate_legacy_config_file(self.data_dir, config_path=path)
                self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["llm_provider_id"], choice)

    def test_preload_migration_runs_before_plugin_class_is_registered(self):
        tree = ast.parse(
            (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        )
        migration = next(
            i
            for i, n in enumerate(tree.body)
            if isinstance(n, ast.Expr)
            and isinstance(n.value, ast.Call)
            and isinstance(n.value.func, ast.Name)
            and n.value.func.id == "migrate_legacy_config_file"
        )
        registration = next(i for i, n in enumerate(tree.body) if isinstance(n, ast.ClassDef))
        self.assertLess(migration, registration)
