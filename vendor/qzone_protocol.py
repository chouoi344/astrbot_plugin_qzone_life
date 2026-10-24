"""Qzone protocol helpers adapted from Zhalslar/astrbot_plugin_qzone v3.1.3.

Copyright remains with the upstream contributors; GPL-3.0.
This derivative retains only the protocol required by Qzone Life and uses
plain dictionaries instead of AstrBot-dependent models. See the notices file.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class Credentials:
    uin: int
    skey: str
    p_skey: str

    @property
    def gtk(self) -> str:
        value = 5381
        for character in self.p_skey:
            value += (value << 5) + ord(character)
        return str(value & 0x7FFFFFFF)

    def cookies(self) -> dict[str, str]:
        return {"uin": f"o{self.uin}", "skey": self.skey, "p_skey": self.p_skey}

    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/138.0.0.0 Safari/537.36",
            "Referer": f"https://user.qzone.qq.com/{self.uin}",
            "Origin": "https://user.qzone.qq.com",
        }


def parse_response(text: str) -> dict[str, Any]:
    """Read JSON/JSONP without evaluating JavaScript or logging response secrets."""
    if not text or not text.strip():
        return {"code": -1, "message": "空间返回空响应", "_parse_error": True}
    # Delete CGI returns an iframe document whose try/catch braces precede the
    # JSON object. Decode only its single terminal callback, never the script.
    callbacks = list(re.finditer(r"\bframeElement\.callback\s*\(\s*", text))
    if callbacks and re.match(r"\s*<html\b", text, re.IGNORECASE):
        if len(callbacks) == 1:
            value = text[callbacks[0].end() :]
            try:
                parsed, end = json.JSONDecoder().raw_decode(value)
            except (ValueError, TypeError):
                pass
            else:
                if (
                    isinstance(parsed, dict)
                    and "code" in parsed
                    and re.fullmatch(
                        r"\s*\)\s*;\s*</script>\s*</body>\s*</html>\s*",
                        value[end:],
                        re.IGNORECASE,
                    )
                ):
                    return parsed
        return {"code": -1, "message": "空间响应无法解析", "_parse_error": True}
    value = text.strip().lstrip("\ufeff").strip()
    if not (value.startswith("{") and value.endswith("}")):
        callback = re.fullmatch(
            r"(?:[A-Za-z_$][\w$]*\.)*[A-Za-z_$][\w$]*\s*\(\s*(\{.*\})\s*\)\s*;?",
            value,
            re.DOTALL,
        )
        if callback is None:
            return {"code": -1, "message": "空间响应格式无法识别", "_parse_error": True}
        value = callback[1]
    if not value:
        return {"code": -1, "message": "空间响应格式无法识别", "_parse_error": True}
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        try:
            import json5

            # Upstream CGI occasionally returns the JavaScript undefined token.
            # Protect quoted user content before replacing bare JS values.
            value = re.sub(
                r'''("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')|(?<=[\[:,])\s*undefined(?=\s*[,}\]])''',
                lambda match: match[1] if match[1] is not None else "null",
                value,
                flags=re.DOTALL,
            )
            parsed = json5.loads(value)
        except (ImportError, ValueError, TypeError):
            return {"code": -1, "message": "空间响应无法解析", "_parse_error": True}
    if not isinstance(parsed, dict):
        return {"code": -1, "message": "空间响应不是对象", "_parse_error": True}
    return parsed


def number(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def result_code(raw: dict[str, Any]) -> int | None:
    value = raw.get("code", raw.get("ret"))
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def upload_fields(raw: dict[str, Any]) -> tuple[str, str]:
    data = raw.get("data")
    if not isinstance(data, dict):
        raise ValueError("图片上传响应缺少图片信息")
    url = str(data.get("url") or "")
    tokens = []
    for field in urlsplit(url).query.split("&"):
        key, separator, value = field.partition("=")
        if unquote(key) == "bo":
            tokens.append(unquote(value) if separator else "")
    # Qzone's token can contain a literal '+'. Form-style unquote_plus would
    # turn it into a space; only percent-decode this URL component.
    if len(tokens) != 1 or not tokens[0].strip():
        raise ValueError("图片上传响应缺少图片标识")
    required = ("albumid", "lloc", "sloc", "type", "height", "width")
    if any(key not in data for key in required):
        raise ValueError("图片上传响应不完整")
    richval = ",{},{},{},{},{},{},,{},{}".format(
        data["albumid"],
        data["lloc"],
        data["sloc"],
        data["type"],
        data["height"],
        data["width"],
        data["height"],
        data["width"],
    )
    return tokens[0], richval


def comment_from_raw(
    raw: dict[str, Any],
    parent_id: str | None = None,
    parent_uin: int | None = None,
) -> dict[str, Any]:
    identifier = str(raw.get("tid") or raw.get("id") or "")
    if identifier == "0":
        identifier = ""
    return {
        "id": identifier,
        "uin": number(raw.get("uin")),
        "nickname": str(raw.get("name") or raw.get("nickname") or ""),
        "content": str(raw.get("content") or ""),
        "created_at": number(raw.get("create_time", raw.get("created_time"))),
        "parent_id": parent_id,
        "parent_uin": parent_uin,
    }


def post_from_raw(raw: dict[str, Any], *, tid: str = "", uin: int = 0) -> dict[str, Any]:
    comments: list[dict[str, Any]] = []
    for main in raw.get("commentlist") or []:
        if not isinstance(main, dict):
            continue
        parent = comment_from_raw(main)
        comments.append(parent)
        for child in main.get("list_3") or []:
            if isinstance(child, dict):
                # A missing root id remains an empty string, never becomes a
                # top-level comment by accident. reply() refuses that route.
                comments.append(comment_from_raw(child, parent["id"], parent["uin"]))
    images: list[str] = []
    for picture in raw.get("pic") or []:
        if not isinstance(picture, dict):
            continue
        source = next(
            (picture[key] for key in ("url2", "url3", "url1", "smallurl") if picture.get(key)), None
        )
        if source:
            images.append(str(source))
    return {
        "tid": str(raw.get("tid") or tid),
        "uin": number(raw.get("uin"), uin),
        "text": str(raw.get("content") or ""),
        "created_at": number(raw.get("created_time", raw.get("create_time"))),
        "comments": comments,
        "images": images,
    }
