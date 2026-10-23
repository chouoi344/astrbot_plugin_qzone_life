"""QQ空间 — one fixed character, an own-account timeline and durable memories."""

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.core.star.filter.command import GreedyStr
from astrbot.api.star import Context, Star, StarTools

from .core.service import QzoneService


class QzoneLifePlugin(Star):
    def __init__(self, context: Context, config):
        super().__init__(context)
        self.service = QzoneService(
            context, config, StarTools.get_data_dir("astrbot_plugin_qzone_life")
        )

    async def initialize(self):
        self.service.start()

    async def terminate(self):
        await self.service.close()

    async def _run(self, operation):
        try:
            return await operation
        except Exception as exc:
            logger.warning("QQ空间操作未完成：%s", exc)
            return f"QQ空间操作未完成：{exc}"

    @filter.command("空间绑定")
    async def bind_space(self, event: AstrMessageEvent):
        """在管理员与固定角色的QQ私聊中绑定后台任务。"""
        yield event.plain_result(await self._run(self.service.bind(event)))

    @filter.command("空间状态")
    async def space_status(self, event: AstrMessageEvent):
        try:
            self.service.require_admin(event)
            yield event.plain_result(self.service.status())
        except Exception as exc:
            yield event.plain_result(str(exc))

    @filter.command("空间发布")
    async def publish_space(self, event: AstrMessageEvent, caption: GreedyStr):
        """引用照片发布说说，可在命令后附上文案。"""
        yield event.plain_result(
            await self._run(self.service.publish_from_chat(event, str(caption)))
        )

    @filter.command("空间列表")
    async def space_list(self, event: AstrMessageEvent):
        yield event.plain_result(await self._run(self.service.list_posts(event)))

    @filter.command("空间删除")
    async def delete_space(self, event: AstrMessageEvent, target: GreedyStr):
        yield event.plain_result(await self._run(self.service.delete_post(event, str(target))))

    @filter.command("空间暂停")
    async def pause_space(self, event: AstrMessageEvent):
        try:
            self.service.require_admin(event)
            self.service.store.set_value("paused", True)
            yield event.plain_result(
                "已暂停自主发帖与自动回评；已发生事件的记忆补写继续。用户明确要求发布仍可用。"
            )
        except Exception as exc:
            yield event.plain_result(str(exc))

    @filter.command("空间恢复")
    async def resume_space(self, event: AstrMessageEvent):
        try:
            self.service.require_admin(event)
            self.service.store.set_value("paused", False)
            yield event.plain_result("已恢复后台任务，按现有日程安排后续分享。")
        except Exception as exc:
            yield event.plain_result(str(exc))

    @filter.command("空间待核对")
    async def uncertain_space(self, event: AstrMessageEvent):
        try:
            self.service.require_admin(event)
            yield event.plain_result(self.service.unresolved())
        except Exception as exc:
            yield event.plain_result(str(exc))

    @filter.command("空间核对")
    async def reconcile_space(self, event: AstrMessageEvent, arguments: GreedyStr):
        parts = str(arguments).split()
        if len(parts) not in (2, 3):
            yield event.plain_result(
                "用法：/空间核对 操作标识 已成功 [说说标识]，或 /空间核对 操作标识 未成功。请先实际核对空间。"
            )
            return
        yield event.plain_result(
            await self._run(
                self.service.reconcile(
                    event, parts[0], parts[1], parts[2] if len(parts) == 3 else ""
                )
            )
        )

    @filter.llm_tool(name="qzone_publish_photo")
    async def publish_photo_tool(self, event: AstrMessageEvent, caption: str = "") -> str:
        """用户明确要求把引用或本条消息中的照片发到你的QQ空间时使用。直接发布；不能用于只是看图或聊照片。没有明确给出发布文案时caption留空，由插件按照片和人设配文。不接受任意图片路径。

        Args:
            caption(string): 用户明确指定的发布文案；未指定则留空。
        """
        return await self._run(self.service.publish_from_chat(event, caption))

    @filter.llm_tool(name="qzone_delete_post")
    async def delete_post_tool(self, event: AstrMessageEvent, target: str) -> str:
        """仅当用户明确要求删除自己QQ空间的说说时调用。目标不唯一时会返回候选，请让用户选择，不猜测。不删除好友的说说。

        Args:
            target(string): 用户指定的说说标识、完整链接、独有文案片段，或“刚才那条”。
        """
        return await self._run(self.service.delete_post(event, target))

    @filter.llm_tool(name="qzone_list_own_posts")
    async def list_posts_tool(self, event: AstrMessageEvent) -> str:
        """查看自己空间最近的说说和标识，用于用户明确询问或定位其要求删除的目标。"""
        return await self._run(self.service.list_posts(event))
