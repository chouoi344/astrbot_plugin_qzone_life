"""Execution facts for the original conversation; no character reply templates."""

from __future__ import annotations

import json


class Outcome(str):
    """Internal diagnostics remain available without becoming chat responses."""

    def __new__(cls, diagnostic, *, action, status, facts, refs=None):
        value = super().__new__(cls, diagnostic)
        value.action = action
        value.status = status
        value.facts = facts
        value.refs = refs or {}
        return value

    def tool_text(self):
        # Targets resolve by content/order. Raw keys and IDs stay in the service.
        return json.dumps(
            {
                "action": self.action,
                "status": self.status,
                "facts": self.facts,
                "context_rule": "这是本次操作的执行事实。继续结合原会话的人设、关系、历史和记忆自行回应，保留原本的交流方式；只把这些事实用于判断动作是否发生。需要选择时用时间、内容或顺序询问。",
            },
            ensure_ascii=False,
        )


class RequestProblem(ValueError):
    """A controlled safe reason for a blocked task, not a chat reply."""

    def __init__(self, code, facts, *, status="needs_input"):
        super().__init__(code)
        self.code, self.facts, self.status = code, facts, status


class ClarificationNeeded(RequestProblem):
    def __init__(self, facts):
        super().__init__("target_needs_confirmation", facts, status="needs_choice")


def exception_outcome(exc, *, action="request"):
    if isinstance(exc, RequestProblem):
        return Outcome(
            exc.code, action=action, status=exc.status, facts={"reason": exc.code, **exc.facts}
        )
    if isinstance(exc, PermissionError):
        reason, fact, status = (
            "permission_required",
            "当前用户没有操作空间的权限，此次操作未执行",
            "blocked",
        )
    elif action in ("list", "status", "check"):
        reason, fact, status = (
            "read_unavailable",
            "本次未取得所查询的最新结果；先前已确认的其他事实仍有效",
            "failed",
        )
    elif isinstance(exc, ValueError):
        reason, fact, status = (
            "input_unavailable",
            "请求中的输入未能完成校验，操作未执行",
            "needs_input",
        )
    else:
        reason, fact, status = (
            "operation_unavailable",
            "本次操作遇到问题，尚未取得可靠的完成结果",
            "uncertain",
        )
    return Outcome(reason, action=action, status=status, facts={"reason": reason, "result": fact})


CHAT_RULE = """涉及你的QQ空间时，按用户的自然表达调用空间工具，继续用当前会话的人设、关系、历史和记忆回应。工具提供本次动作的执行事实，不替代聊天上下文或角色表达；由你自行组织措辞。发布、删除、查看、启用、暂停恢复、重试与核对都通过自然聊天完成，使用内容、时间、先后顺序识别目标。有歧义再向对方确认。只有工具确认发生的动作才作为已完成事实；结果未明时先查询核对，保持防重复。把技术编号、原始异常和内部状态留在后台，日常聊天无需输入命令或复制标识。"""
