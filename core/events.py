"""Rebuild a clean real-session SDK event for background work, never replay old images."""

import time


class BackgroundDeliveryBlocked(RuntimeError):
    """An autonomous image dependency tried to start a chat delivery."""


class _ReadOnlyBot:
    def __init__(self, bot):
        self._bot = bot

    async def call_action(self, action, **kwargs):
        if not str(action).startswith(("get_", "can_")):
            raise BackgroundDeliveryBlocked("自主生图不发送聊天消息")
        return await self._bot.call_action(action, **kwargs)

    def __getattr__(self, name):
        if name.startswith(("get_", "can_")):
            return getattr(self._bot, name)
        raise BackgroundDeliveryBlocked("自主生图不发送聊天消息")


class SilentGenerationEvent:
    """Scope the delivery guard to one image call, leaving normal chat intact.

    The supported image-only API reads event identity/reference data. It must
    not send progress, images, or create a new conversation completion event.
    Do not monkey-patch the shared SDK event or the plugin's global context.
    """

    def __init__(self, event):
        self._event = event
        self.bot = _ReadOnlyBot(getattr(event, "bot", None))
        self._extra = {"qzone_autonomous": True, "qzone_silent": True}

    def __getattr__(self, name):
        if name.startswith("send") or name in {"request_llm", "set_result"}:
            raise BackgroundDeliveryBlocked("自主生图不发送聊天消息")
        return getattr(self._event, name)

    async def send(self, *args, **kwargs):
        raise BackgroundDeliveryBlocked("自主生图不发送聊天消息")

    async def send_streaming(self, *args, **kwargs):
        raise BackgroundDeliveryBlocked("自主生图不发送聊天消息")

    def get_extra(self, key, default=None):
        return self._extra.get(key, default)

    def set_extra(self, key, value):
        if str(key).startswith("_gitee_bg_") or key == "provider_request":
            raise BackgroundDeliveryBlocked("自主生图不创建聊天通知")
        self._extra[key] = value


def build_background_event(context, binding):
    from astrbot.api.platform import AstrBotMessage, MessageMember, MessageType
    from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
        AiocqhttpMessageEvent,
    )

    platform_id, message_type, session_id = binding["umo"].split(":", 2)
    platform = context.get_platform_inst(platform_id)
    if platform is None or platform.meta().name != "aiocqhttp":
        raise RuntimeError("QQ连接尚未就绪，恢复后空间会自动继续")
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
