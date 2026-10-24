"""Rebuild a clean real-session SDK event for background work, never replay old images."""

import time


def build_background_event(context, binding):
    from astrbot.api.platform import AstrBotMessage, MessageMember, MessageType
    from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
        AiocqhttpMessageEvent,
    )

    platform_id, message_type, session_id = binding["umo"].split(":", 2)
    platform = context.get_platform_inst(platform_id)
    if platform is None or platform.meta().name != "aiocqhttp":
        raise RuntimeError("绑定的QQ连接尚未就绪，等待恢复连接或由管理员重新绑定")
    client = platform.get_client()
    message = AstrBotMessage()
    message.type = MessageType(message_type)
    message.self_id = str(binding["bot_id"])
    message.sender = MessageMember(
        user_id=str(binding["admin_id"]), nickname=str(binding.get("admin_name", "管理员"))
    )
    message.group_id = session_id if message_type == "GroupMessage" else ""
    message.session_id = session_id
    message.message_id = f"qzone-background-{time.time_ns()}"
    message.message = []
    message.message_str = ""
    message.raw_message = {}
    event = AiocqhttpMessageEvent("", message, platform.meta(), session_id, client)
    event.role = "admin"
    if event.unified_msg_origin != binding["umo"]:
        raise RuntimeError("后台会话归属与绑定不一致")
    return event
