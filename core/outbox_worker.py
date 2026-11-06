"""Small durable follow-up batches with separate memory and QQ retry budgets."""

from __future__ import annotations

import asyncio
import time

from .request_policy import QzoneDeferred
from .comment_polling import post_reported_deleted


MEMORY_TIMEOUT = 30
MEMORY_BATCH = 3
LIKE_ATTEMPTS = 3
LIKE_RETRY_SECONDS = 600


def _old_owner(service, binding):
    current = service.store.get_value("binding")
    if not current:
        return False
    if any(str(current.get(key, "")) != str(binding.get(key, ""))
           for key in ("bot_id", "persona_id")):
        return True
    return current.get("umo") == binding.get("umo") and current.get("scope") != binding.get("scope")


async def _memory(service, task):
    key = task["event_key"]
    if not service.store.claim_outbox(key):
        return
    try:
        payload = task["payload"]
        event = service.event_factory(service.context, payload["binding"])
        result = await asyncio.wait_for(
            service.integrations.sync_memory(event, payload), timeout=MEMORY_TIMEOUT
        )
        service.store.finish_outbox(key, result)
    except asyncio.CancelledError:
        # The upstream memory call may already have written its event marker.
        service.store.retry_outbox(key, "记忆同步中断，恢复后核对", time.time() + 60,
                                   needs_reconcile=True)
        raise
    except Exception as exc:
        delay = min(3600, 30 * 2 ** min(int(task.get("attempts", 0)), 7))
        reason = f"记忆同步暂未完成（{type(exc).__name__}）"
        service.store.retry_outbox(key, reason, time.time() + delay, needs_reconcile=True)
        service.store.set_value("last_sync_error", reason)


async def _like(service, task):
    key, payload = task["event_key"], task["payload"]
    binding = payload["binding"]
    post = service.store.get_post(payload["tid"])
    if not post or post["status"] == "deleted":
        service.store.stop_outbox(key, "说说已删除，自赞结束")
        return
    if post_reported_deleted(service.store, post):
        service.store.stop_outbox(key, "空间已报告说说删除，自赞结束")
        return
    if _old_owner(service, binding):
        service.store.stop_outbox(key, "账号或角色已变化，旧自赞结束")
        return
    if int(task.get("attempts", 0)) >= LIKE_ATTEMPTS:
        service.store.stop_outbox(key, "自赞尝试次数已用完")
        return
    if service.store.get_value("paused", False):
        service.store.defer_outbox(key, "自主操作已暂停", time.time() + 60)
        return
    try:
        # This preflight happens before the durable attempt is claimed. Merely
        # waiting for a connection or account cooldown spends no like attempt.
        service.request_policy(binding["bot_id"]).check()
        event = service.event_factory(service.context, binding)
        await service.verify_binding(event, binding)
        transport = await service.transport(event)
    except asyncio.CancelledError:
        raise
    except QzoneDeferred as exc:
        service.store.defer_outbox(key, "空间访问正在等待恢复", max(time.time() + 1, exc.retry_at))
        return
    except Exception:
        service.store.defer_outbox(key, "账号或权限暂未就绪，稍后核对", time.time() + 60)
        return
    if service.store.get_value("paused", False):
        service.store.defer_outbox(key, "自主操作已暂停", time.time() + 60)
        return
    if not service.store.claim_outbox(key):
        return

    async def validate():
        # Pacing may have waited several seconds. Recheck the live role and
        # pause/deletion state after that wait, immediately before the HTTP write.
        await service.verify_binding(event, binding)
        latest = service.store.get_post(payload["tid"])
        if (not latest or latest["status"] == "deleted" or post_reported_deleted(service.store, latest)
                or _old_owner(service, binding)
                or service.store.get_value("paused", False)):
            raise ValueError("自赞条件已变化，本次尚未发送")

    try:
        result = await transport.like(payload["tid"], before_write=validate)
    except QzoneDeferred as exc:
        service.store.defer_outbox(
            key, "空间访问正在等待恢复", max(time.time() + 1, exc.retry_at), refund_attempt=True
        )
        return
    except asyncio.CancelledError:
        service.store.stop_outbox(key, "自赞执行中断，结果待核对", uncertain=True)
        raise
    except Exception:
        # An uncategorised exception after entering the transport has no proof
        # that the write stayed local. Preserve uncertainty instead of replaying.
        service.store.stop_outbox(key, "自赞结果未确认，自动重试已停止", uncertain=True)
        return
    if result.get("status") == "success":
        service.store.finish_outbox(key, result)
        return
    if result.get("status") != "rejected":
        service.store.stop_outbox(key, "自赞结果未确认，自动重试已停止", uncertain=True)
        return
    if result.get("stage") in ("credentials", "cooldown", "preflight"):
        service.store.defer_outbox(
            key, "自赞尚未发送，等待连接恢复",
            max(time.time() + 60, float(result.get("retry_at") or 0)), refund_attempt=True,
        )
        latest = service.store.get_post(payload["tid"])
        if (not latest or latest["status"] == "deleted" or post_reported_deleted(service.store, latest)
                or _old_owner(service, binding)):
            service.store.stop_outbox(key, "自赞条件已变化，本次自动尝试结束")
        return
    attempted = int(service.store.get_outbox(key)["attempts"])
    if attempted >= LIKE_ATTEMPTS:
        service.store.stop_outbox(key, "自赞尝试次数已用完")
    elif result.get("retryable") is True or result.get("stage") == "connect":
        service.store.retry_outbox(
            key, "自赞暂未完成，稍后再试",
            max(time.time() + LIKE_RETRY_SECONDS, float(result.get("retry_at") or 0)),
        )
    else:
        service.store.stop_outbox(key, "空间拒绝自赞，本次自动尝试结束")


async def process_outbox(service, *, allow_qq=True, include_memory=True):
    """Run at most three memory events and one self-like, without the UI lock.

    Memory is processed first and does not inspect QQ online/cooldown state.
    The task's original event binding is retained for every memory operation.
    A second concurrent caller simply leaves work to the current worker.
    """
    lock = getattr(service, "_outbox_lock", None)
    if lock is None:
        lock = service._outbox_lock = asyncio.Lock()
    if lock.locked():
        return
    async with lock:
        if include_memory:
            for task in service.store.due_outbox("memory", time.time(), limit=MEMORY_BATCH):
                await _memory(service, task)
        if allow_qq:
            for task in service.store.due_outbox("self_like", time.time(), limit=1):
                await _like(service, task)
