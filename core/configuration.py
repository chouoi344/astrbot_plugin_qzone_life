"""Migrate before AstrBot's schema loader removes legacy configuration keys."""

import hashlib
import json
import os
import tempfile
from pathlib import Path

from .settings import normalize_config


def _restore_text_choice(original, data_dir):
    """Recover a choice removed by the earlier form, once, before host defaults.

    An explicitly present empty value means the user chose to follow chat.
    Only our own private migration backups are eligible for this recovery.
    """
    if "llm_provider_id" in original:
        return original
    candidates = []
    for backup in data_dir.glob("settings-before-simplification-*.json"):
        try:
            saved = json.loads(backup.read_text(encoding="utf-8-sig"))
            if isinstance(saved, dict) and isinstance(saved.get("llm_provider_id"), str):
                candidates.append((backup.stat().st_mtime_ns, backup.name, saved["llm_provider_id"]))
        except (OSError, UnicodeError, ValueError):
            continue
    if not candidates:
        return original
    return {**original, "llm_provider_id": max(candidates)[2]}


def migrate_legacy_config_file(data_dir, *, config_path=None):
    data_dir = Path(data_dir)
    path = (
        Path(config_path)
        if config_path is not None
        else (data_dir.parent.parent / "config" / "astrbot_plugin_qzone_life_config.json")
    )
    if not path.is_file():
        return
    original_bytes = path.read_bytes()
    original = json.loads(original_bytes.decode("utf-8-sig"))
    simple = normalize_config(_restore_text_choice(original, data_dir))
    if original == simple:
        return
    data_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(original_bytes).hexdigest()[:12]
    backup = data_dir / f"settings-before-simplification-{digest}.json"
    if not backup.exists():
        backup.write_bytes(original_bytes)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".qzone-life-config-",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(simple, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
