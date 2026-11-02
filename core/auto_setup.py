"""Discover the existing administrator/private session without chat activation.

Only login identity is read. No message, conversation, account or host setting
is created here; the caller validates persona, memory scope and space identity.
"""

from __future__ import annotations

import asyncio
import inspect

from .dialogue import RequestProblem


_LOGIN_TIMEOUT = 8


def _problem(code, message):
    return RequestProblem(code, {"result": message}, status="needs_setup")


def _qq(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    text = str(value).strip()
    if not text or not text.isascii() or not text.isdigit() or text.startswith("0"):
        return ""
    return text


def configured_admins(context):
    """Keep host order for the primary private chat, never grant new authority."""
    raw = context.get_config().get("admins_id", [])
    if not isinstance(raw, (list, tuple)):
        return []
    return list(dict.fromkeys(qq for value in raw if (qq := _qq(value))))


def _login_id(raw):
    if not isinstance(raw, dict):
        return ""
    if "status" in raw and raw["status"] != "ok":
        return ""
    if "retcode" in raw and (isinstance(raw["retcode"], bool) or raw["retcode"] not in (0, "0")):
        return ""
    data = raw.get("data", raw)
    if not isinstance(data, dict):
        return ""
    identities = [_qq(data[key]) for key in ("user_id", "uin", "self_id") if key in data]
    if not identities or not all(identities) or len(set(identities)) != 1:
        return ""
    return identities[0]


async def _await(value):
    return await value if inspect.isawaitable(value) else value


def _platforms(context):
    # AstrBot 4.29.0 exposes platform_manager/get_platform_inst, not a
    # get_platform_insts() method on Context.
    manager = getattr(context, "platform_manager", None)
    getter = getattr(manager, "get_insts", None)
    raw = getter() if callable(getter) else getattr(manager, "platform_insts", None)
    if not isinstance(raw, (list, tuple)):
        raise _problem("qq_connection_waiting", "QQ连接尚未就绪，连接恢复后会自动准备空间")
    result = {}
    for platform in raw:
        try:
            meta = platform.meta()
            platform_id = str(meta.id or "")
            if meta.name != "aiocqhttp" or not platform_id or ":" in platform_id:
                continue
            config = getattr(platform, "config", {})
            if isinstance(config, dict) and config.get("enable") is False:
                continue
            result[platform_id] = platform
        except (AttributeError, TypeError, ValueError):
            continue
    return result


def _connected_qq(client):
    """Read the unambiguous reverse-WS identity, if exposed by the adapter."""
    connections = getattr(client, "_wsr_api_clients", None)
    if not isinstance(connections, dict):
        return None
    if not connections:
        return ""
    if len(connections) != 1:
        raise _problem(
            "qq_account_ambiguous", "同一QQ连接中有多个机器人账号，请在后台为本插件保留一个QQ账号"
        )
    identity = _qq(next(iter(connections)))
    if not identity:
        raise _problem("qq_connection_waiting", "QQ连接身份尚未就绪，恢复后会自动准备空间")
    return identity


async def _identity(platform):
    try:
        client = platform.get_client()
        # CQHttp's reverse-WS client map is also used by AstrBot's adapter.
        # Our transport supports one bot per adapter; never select an arbitrary
        # account when its later cookie requests cannot carry a routing id.
        connected_before = _connected_qq(client)
        if connected_before == "":
            return ""
        getter = getattr(client, "get_login_info", None)
        if not callable(getter):
            return ""
        raw = await asyncio.wait_for(_await(getter()), timeout=_LOGIN_TIMEOUT)
        connected_after = _connected_qq(client)
        if connected_after == "":
            return ""
        login_id = _login_id(raw)
        if connected_before != connected_after or (
            connected_after is not None and login_id and connected_after != login_id
        ):
            raise _problem("account_changed", "QQ连接与登录账号暂未一致，恢复后会自动准备空间")
        return login_id
    except RequestProblem:
        raise
    except Exception:
        # SDK exceptions may contain access tokens/URLs. Keep them outside the
        # public preparation facts; a failed identity is simply not selected.
        return ""


async def discover_session(
    context, event_factory, *, previous=None, hint=None, allow_account_refresh=False
):
    """Return an observed session; the caller still verifies its space Cookie.

    Refreshing may follow the sole live account on the saved adapter. It never
    selects another adapter or changes stored posts, sessions or permissions.
    """
    admins = configured_admins(context)
    if not admins:
        raise _problem("admin_required", "请在AstrBot后台填写管理员QQ，保存后会自动准备空间")
    previous = previous if isinstance(previous, dict) else {}
    admin = str(previous.get("admin_id") or "")
    if admin not in admins:
        admin = admins[0]
    platform_id = ""
    hint_bot = ""
    expected_bot = _qq(previous.get("bot_id"))
    if previous:
        origin = str(previous.get("umo") or "").split(":", 2)
        if len(origin) != 3 or origin[1] != "FriendMessage" or not expected_bot:
            raise _problem("session_unavailable", "原空间会话信息不完整，需要在后台检查QQ连接设置")
        platform_id = origin[0]
    if hint is not None:
        if str(hint.get_sender_id()) not in admins:
            raise PermissionError("此操作仅限 AstrBot 管理员。")
        if hint.get_platform_name() != "aiocqhttp":
            raise _problem("qq_chat_required", "空间操作需要已连接的QQ会话")
        hint_origin = str(hint.unified_msg_origin).split(":", 2)
        hint_bot = _qq(hint.get_self_id())
        if len(hint_origin) != 3 or not hint_origin[0] or not hint_bot:
            raise _problem("qq_connection_waiting", "QQ会话信息尚未就绪，会在连接恢复后自动准备")
        if previous and (
            hint_origin[0] != platform_id
            or (hint_bot != expected_bot and not allow_account_refresh)
        ):
            raise _problem("account_changed", "当前QQ账号与原空间账号不同，已暂停空间操作")
        platform_id = platform_id or hint_origin[0]
        expected_bot = expected_bot or hint_bot

    platforms = _platforms(context)
    if platform_id:
        platforms = {platform_id: platforms[platform_id]} if platform_id in platforms else {}
    if not platforms:
        raise _problem("qq_connection_waiting", "QQ连接尚未就绪，连接恢复后会自动准备空间")
    items = list(platforms.items())
    identities = await asyncio.gather(
        *(_identity(platform) for _, platform in items), return_exceptions=True
    )
    for identity in identities:
        if isinstance(identity, BaseException):
            raise identity
    candidates = [(key, qq) for (key, _), qq in zip(items, identities) if qq]
    if not candidates:
        raise _problem("qq_connection_waiting", "QQ账号尚未连接或登录，恢复后会自动准备空间")
    if len(candidates) != 1:
        raise _problem("qq_account_ambiguous", "当前有多个可用QQ连接，请在后台为本插件保留一个QQ连接")
    selected, bot_id = candidates[0]
    if hint_bot and bot_id != hint_bot:
        raise _problem("account_changed", "当前消息与QQ登录账号不同，已暂停此次空间操作")
    if expected_bot and bot_id != expected_bot and not allow_account_refresh:
        raise _problem("account_changed", "QQ连接账号已改变，已暂停原空间操作")
    event = await _await(event_factory(context, {
        "umo": f"{selected}:FriendMessage:{admin}", "bot_id": bot_id,
        "admin_id": admin, "admin_name": "管理员",
    }))
    return event
