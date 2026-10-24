"""QQ空间 — character-native dialogue over durable own-account actions."""

import inspect
import hashlib
import json
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

from .core.dialogue import CHAT_RULE, Outcome, exception_outcome
from .core.configuration import migrate_legacy_config_file
from .core.service import QzoneService
from .core.settings import normalize_config


# StarManager imports this module before constructing AstrBotConfig. Migrating
# here preserves old choices before the host drops keys and inserts defaults.
migrate_legacy_config_file(Path(StarTools.get_data_dir("astrbot_plugin_qzone_life")))


class QzoneLifePlugin(Star):
    def __init__(self, context: Context, config):
        super().__init__(context)
        data_dir = Path(StarTools.get_data_dir("astrbot_plugin_qzone_life"))
        original = dict(config)
        simple = normalize_config(original)
        if original != simple:
            # Retire hidden controls in both the form and runtime. Preserve the
            # complete old configuration privately for an actual rollback.
            data_dir.mkdir(parents=True, exist_ok=True)
            raw = json.dumps(original, ensure_ascii=False, sort_keys=True, indent=2)
            digest = hashlib.sha256(raw.encode()).hexdigest()[:12]
            backup = data_dir / f"settings-before-simplification-{digest}.json"
            if not backup.exists():
                backup.write_text(raw, encoding="utf-8")
            config.clear()
            config.update(simple)
            save = getattr(config, "save_config", None)
            if callable(save):
                save()
        self.service = QzoneService(context, simple, data_dir)

    async def initialize(self):
        self.service.start()

    async def terminate(self):
        await self.service.close()

    async def _run(self, event, operation, *, action="request"):
        # Return evidence to the existing conversation's tool loop. The original
        # character model keeps its history, memory and control of the reply.
        try:
            result = operation()
            result = await result if inspect.isawaitable(result) else result
            if not isinstance(result, Outcome):
                raise RuntimeError("空间操作缺少结构化结果")
        except Exception as exc:
            logger.warning("QQ空间操作未完成：%s", exc)
            result = exception_outcome(exc, action=action)
        return result.tool_text()

    @filter.on_llm_request()
    async def space_dialogue_request(self, event: AstrMessageEvent, req):
        if event.get_platform_name() == "aiocqhttp":
            req.system_prompt = (req.system_prompt or "") + "\n\n" + CHAT_RULE

    @filter.llm_tool(name="qzone_publish_photo")
    async def publish_photo_tool(self, event: AstrMessageEvent, caption: str = "") -> str:
        """用户明确要求把引用或当前消息的照片发到你的QQ空间时使用。仅看图聊天不调用。未明确指定发布文案时caption留空，插件按照片和当前人设自动配文。结果按facts用角色口吻回复，勿显示内部引用。结果不明先查询，勿再次调用发布。

        Args:
            caption(string): 用户明确指定的文案；未指定留空。
        """
        return await self._run(
            event, lambda: self.service.publish_from_chat(event, caption), action="publish"
        )

    @filter.llm_tool(name="qzone_delete_post")
    async def delete_post_tool(self, event: AstrMessageEvent, target: str) -> str:
        """用户明确要删除你自己空间的说说时使用。用文案片段、刚才那条、最新一条或刚查询候选中的第一条等定位。候选不唯一时询问用户，不猜。内部标识仅用于调用，不展示。

        Args:
            target(string): 用户指定的内容片段、先后顺序或内部引用。
        """
        return await self._run(
            event, lambda: self.service.delete_post(event, target), action="delete"
        )

    @filter.llm_tool(name="qzone_list_own_posts")
    async def list_posts_tool(self, event: AstrMessageEvent) -> str:
        """用户询问自己空间最近内容，或需要定位要删的说说时查看。用时间和内容描述候选，勿展示内部标识。"""
        return await self._run(event, lambda: self.service.list_posts(event), action="list")

    @filter.llm_tool(name="qzone_space_status")
    async def status_tool(self, event: AstrMessageEvent) -> str:
        """用户询问今天自主发了几条、是否暂停、日常分享与回评状态时查看。次数与开关信息用此工具就够了；只有用户明确询问具体说说内容时再读列表。用当前角色口吻解释。"""
        return await self._run(event, lambda: self.service.user_status(event), action="status")

    @filter.llm_tool(name="qzone_prepare_space")
    async def bind_tool(self, event: AstrMessageEvent) -> str:
        """管理员明确要启用或重新绑定当前角色的空间时调用，限私聊。不要无故更换绑定。"""
        return await self._run(event, lambda: self.service.bind(event), action="bind")

    @filter.llm_tool(name="qzone_pause_or_resume")
    async def control_tool(self, event: AstrMessageEvent, action: str) -> str:
        """管理员要求暂停或恢复日常自主发说说、自动回评与补赞时调用；这个开关一起控制三项。手动要求发说说仍可用。

        Args:
            action(string): pause表示暂停，resume表示恢复。
        """
        return await self._run(event, lambda: self.service.control(event, action), action="control")

    @filter.llm_tool(name="qzone_retry_last_photo")
    async def retry_tool(self, event: AstrMessageEvent) -> str:
        """用户要求重试刚才未成功的照片发布时使用。复用当前聊天里确定失败的最近一次原图原文，结果不明会先阻止重复发送。"""
        return await self._run(
            event, lambda: self.service.retry_last_publish(event), action="publish"
        )

    @filter.llm_tool(name="qzone_check_pending")
    async def check_tool(
        self, event: AstrMessageEvent, target: str = "", outcome: str = "", confirmation: str = ""
    ) -> str:
        """用户询问刚才发/删/回评是否成功，或明确核实后告知结果时调用。查询时outcome与confirmation留空。只有用户亲自看过空间、明确陈述结果时填写outcome，confirmation必须完整复制当前用户消息原文，不截取片段，问题或猜测不算确认。发布成功还会实际查询匹配说说。内部引用勿展示。

        Args:
            target(string): 刚才那次、内容片段、第一条或之前工具给的内部引用；只有一次时留空。
            outcome(string): 查询留空；用户明确核实已发生填已成功，明确未发生填未成功。
            confirmation(string): 完整复制对应确认结论的当前用户消息原文，不截取片段；查询留空。
        """
        return await self._run(
            event,
            lambda: self.service.check_pending(event, target, outcome, confirmation),
            action="check",
        )
