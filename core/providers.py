"""Resolve text choices and AstrBot's effective session image settings locally."""

from __future__ import annotations

from collections.abc import Mapping

from .dialogue import RequestProblem


def _provider_id(value):
    return value.strip() if isinstance(value, str) else ""


def _mapping(value):
    return value if isinstance(value, Mapping) else {}


def _chat_provider(context, provider_id):
    provider = context.get_provider_by_id(provider_id) if provider_id else None
    return provider if callable(getattr(provider, "text_chat", None)) else None


def _supports_image(provider):
    modalities = _mapping(getattr(provider, "provider_config", None)).get("modalities")
    # AstrBot 4.29 treats the migration value [] as unconfigured/compatible.
    return modalities == [] or isinstance(modalities, list) and "image" in modalities


def _vision_problem(detail):
    return RequestProblem(
        "system_vision_unavailable",
        {
            "result": detail,
            "needed": "在 AstrBot 设置中选择可用的图片转述模型，或为当前会话设置支持图片的聊天模型",
        },
    )


async def _current_chat_provider_id(context, event):
    try:
        provider_id = _provider_id(
            await context.get_current_chat_provider_id(event.unified_msg_origin)
        )
    except Exception:
        provider_id = ""
    if not provider_id:
        raise RequestProblem(
            "chat_model_unavailable",
            {"result": "当前会话尚未取得可用的对话模型，需要模型的操作尚未执行"},
        )
    return provider_id


async def resolve_provider_id(context, event, *, text_provider_id="", vision=False):
    """Use a selected text model or the system's image route for this session.

    Empty system image-caption settings follow AstrBot 4.29's image route:
    current chat model first, then only explicitly configured image-capable
    ``agent_runner.config.model.fallback_provider_ids``. No model is probed.
    """
    if not vision:
        return _provider_id(text_provider_id) or await _current_chat_provider_id(
            context, event
        )

    try:
        config = _mapping(context.get_config(umo=event.unified_msg_origin))
        settings = _mapping(config.get("provider_settings"))
        caption_id = _provider_id(settings.get("default_image_caption_provider_id"))
        if caption_id:
            # The official caption route uses its explicit choice regardless of
            # the chat model's modalities. An unavailable choice stays visible.
            if _chat_provider(context, caption_id) is not None:
                return caption_id
            raise _vision_problem("系统已选的看图模型当前未加载，需要读取图片的操作尚未执行")

        current_id = await _current_chat_provider_id(context, event)
        provider = _chat_provider(context, current_id)
        if provider is not None and _supports_image(provider):
            return current_id

        model_config = _mapping(
            _mapping(_mapping(config.get("agent_runner")).get("config")).get("model")
        )
        fallback_ids = model_config.get("fallback_provider_ids", [])
        if isinstance(fallback_ids, list):
            seen = {current_id}
            for raw_id in fallback_ids:
                fallback_id = _provider_id(raw_id)
                if not fallback_id or fallback_id in seen:
                    continue
                seen.add(fallback_id)
                provider = _chat_provider(context, fallback_id)
                if provider is not None and _supports_image(provider):
                    return fallback_id
    except RequestProblem:
        raise
    except Exception:
        raise _vision_problem("当前会话的系统看图配置暂时无法取得，需要读取图片的操作尚未执行") from None

    raise _vision_problem("当前会话尚未设置可用的看图模型，需要读取图片的操作尚未执行")
