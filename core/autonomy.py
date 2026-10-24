"""Silent, bounded autonomous publishing with durable media reuse."""

from __future__ import annotations

import asyncio
from pathlib import Path

from .caption import single_sentence
from .planning import daily_target, schedule_hash


class _StopOpportunity(RuntimeError):
    """The opportunity changed or expired; keep a fixed, safe reason only."""


def _unresolved(service, binding, own_key: str = "") -> bool:
    for state in ("uncertain", "inflight"):
        for operation in service.store.list_operations(status=state, limit=100000):
            post = operation["payload"].get("post", {})
            bot = str(post.get("binding", {}).get("bot_id", ""))
            if (
                operation["key"] != own_key
                and operation["kind"] == "publish"
                and (not bot or bot == str(binding["bot_id"]))
            ):
                return True
    return False


def _quota_full(service, binding) -> bool:
    day = str(service.now().date())
    return service.store.count_autonomous(day) >= daily_target(
        day, binding["bot_id"], service.settings.daily_min, service.settings.daily_max
    )


def _too_close(service) -> bool:
    previous = [
        post
        for post in service.store.list_posts(limit=200, include_deleted=True)
        if post["source"] == "autonomous"
    ]
    return bool(
        previous
        and service.now().timestamp() - previous[0]["published_at"]
        < service.settings.min_gap_minutes * 60
    )


async def _check(service, event, binding, plan):
    if not service.settings.auto_publish or service.store.get_value("paused", False):
        raise _StopOpportunity("paused")
    if not service.plan_is_current(plan):
        raise _StopOpportunity("opportunity_expired")
    try:
        await service.verify_binding(event, binding)
        await service.integrations.check_life_persona(binding)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _StopOpportunity("binding_changed") from None
    life = await service.current_life(binding)
    if schedule_hash(life) != plan["schedule_hash"]:
        raise _StopOpportunity("schedule_changed")
    if not service.plan_matches_life(plan, life):
        raise _StopOpportunity("outside_schedule_activity")
    if _quota_full(service, binding):
        raise _StopOpportunity("daily_quota_reached")
    if _too_close(service):
        raise _StopOpportunity("minimum_gap")
    if _unresolved(service, binding, "auto:" + plan["plan_id"]):
        raise _StopOpportunity("other_publish_unknown")


def _terminal(service, plan, state, reason):
    service.store.update_plan(plan["plan_id"], stage=state, last_error_kind=reason)
    service.store.finish_plan(plan["plan_id"], state)
    service.store.set_value("last_autonomous_result", "本次自主分享已静默跳过")


async def run_autonomous(service, event, binding):
    """Execute at most one due attempt; never send a chat message.

    The runtime owns its service lock. SQLite claims prevent two workers from
    taking the same opportunity, while cached stages survive process restarts.
    """
    if not service.settings.auto_publish or service.store.get_value("paused", False):
        return
    service.store.expire_previous_plans(str(service.now().date()))
    await service.verify_binding(event, binding)
    await service.integrations.check_life_persona(binding)
    if _unresolved(service, binding):
        service.store.set_value("schedule_status", "自主分享结果待核对，后续自主分享已暂停")
        return
    life = await service.ensure_plan(event, binding)
    if not life:
        return
    day = str(service.now().date())
    if _quota_full(service, binding):
        return
    delays = [
        service.settings.auto_retry_first_minutes * 60,
        service.settings.auto_retry_second_minutes * 60,
    ][: service.settings.auto_retry_count]
    for plan in service.store.list_plans(day):
        now = service.now().timestamp()
        if plan["status"] != "pending" or plan["slot_at"] > now:
            continue
        if not service.plan_is_current(plan):
            _terminal(service, plan, "skipped", "opportunity_expired")
            continue
        if plan["schedule_hash"] != schedule_hash(life):
            _terminal(service, plan, "cancelled", "schedule_changed")
            continue
        # A newly selected skip policy also retires retries already waiting in
        # SQLite. Never claim one extra attempt using its old retry budget.
        if int(plan.get("attempt_count", 0)) > len(delays):
            _terminal(service, plan, "skipped", "retry_disabled" if not delays else "retry_exhausted")
            continue
        if float(plan.get("next_attempt_at") or plan["slot_at"]) > now or _too_close(service):
            continue
        if not service.store.claim_plan(plan["plan_id"], now=now, retry_delays=delays):
            continue
        plan = service.store.get_plan(plan["plan_id"])
        key = "auto:" + plan["plan_id"]
        stage = "selfie"
        stopped = None

        async def final_check():
            nonlocal stopped
            try:
                await _check(service, event, binding, plan)
            except _StopOpportunity as exc:
                stopped = str(exc)
                raise

        try:
            operation = service.store.get_operation(key)
            # Once an external write was attempted, its immutable payload is the
            # sole retry source. Never replace a missing original with a new image.
            if operation:
                if operation["status"] in ("uncertain", "inflight"):
                    service.store.finish_plan(plan["plan_id"], "uncertain")
                    break
                if operation["status"] == "done":
                    service.store.finish_plan(
                        plan["plan_id"], "done", (operation.get("result") or {}).get("tid")
                    )
                    break
                draft = operation["payload"]["post"]
                if not single_sentence(draft.get("content", "")):
                    # A write attempt fixes the payload. Do not rewrite a legacy
                    # multi-sentence draft and replay it as the same operation.
                    _terminal(service, plan, "skipped", "legacy_caption_format")
                    break
                original_images = draft.get("images") or []
                if len(original_images) != 1 or not Path(original_images[0]).is_file():
                    _terminal(service, plan, "skipped", "original_image_missing")
                    break
                plan = service.store.update_plan(
                    plan["plan_id"],
                    cached_image_path=str(original_images[0]),
                    cached_summary=str(draft.get("image_summary") or ""),
                    cached_caption=str(draft.get("content") or ""),
                )
            path = plan.get("cached_image_path")
            if not path or not Path(path).is_file():
                service.store.update_plan(
                    plan["plan_id"], stage="selfie", cached_summary="", cached_caption=""
                )
                prompt = (
                    f"拍摄你自己的单人日常自拍。当前活动：{plan['activity']}。"
                    f"今日穿搭：{life.get('outfit', '')}。画面只表现当前活动，"
                    "不混入今天其他时间的场景；保持已设置角色参考形象。"
                )
                path, _ = await asyncio.wait_for(
                    service.integrations.selfie(event, prompt), service.settings.selfie_timeout
                )
                if not Path(path).is_file():
                    raise ValueError("自拍文件缺失")
                plan = service.store.update_plan(plan["plan_id"], cached_image_path=str(path))
            await final_check()
            images = [str(path)]
            stage = "vision"
            summary = plan.get("cached_summary")
            if not summary:
                service.store.update_plan(plan["plan_id"], stage=stage)
                summary = await service.photo_summary(event, images)
                if not isinstance(summary, str) or not summary.strip():
                    raise ValueError("识图内容为空")
                plan = service.store.update_plan(plan["plan_id"], cached_summary=summary)
            await final_check()
            stage = "caption"
            text = plan.get("cached_caption")
            if text and not single_sentence(text):
                plan = service.store.update_plan(plan["plan_id"], cached_caption="")
                text = ""
            if not text:
                service.store.update_plan(plan["plan_id"], stage=stage)
                text = await service.generate_caption(event, summary, activity=plan["activity"])
                if not isinstance(text, str) or not text.strip() or len(text) > 2000:
                    raise ValueError("配文内容无效")
                plan = service.store.update_plan(plan["plan_id"], cached_caption=text)
            await final_check()
            stage = "publish"
            service.store.update_plan(plan["plan_id"], stage=stage)
            await service._publish(
                event, binding, text, images, summary, "autonomous", key, final_check=final_check
            )
            operation = service.store.get_operation(key)
            if operation and operation["status"] == "done":
                service.store.finish_plan(
                    plan["plan_id"], "done", (operation.get("result") or {}).get("tid")
                )
                service.store.set_value("last_autonomous_result", "自主分享已完成")
                service.store.set_value("last_autonomous_error", "")
            elif operation and operation["status"] in ("inflight", "uncertain"):
                service.store.finish_plan(plan["plan_id"], "uncertain")
                service.store.set_value("last_autonomous_result", "自主分享结果待核对")
            elif stopped:
                _terminal(service, plan, "cancelled", stopped)
            else:
                service.store.schedule_plan_retry(
                    plan["plan_id"], "publish_rejected", now=service.now().timestamp()
                )
        except _StopOpportunity as exc:
            _terminal(service, plan, "cancelled", str(exc))
        except asyncio.CancelledError:
            operation = service.store.get_operation(key)
            if operation and operation["status"] in ("inflight", "uncertain"):
                if operation["status"] == "inflight":
                    service.store.mark_operation_uncertain(key, "发布任务中断，等待核对")
                service.store.finish_plan(plan["plan_id"], "uncertain")
            else:
                service.store.schedule_plan_retry(
                    plan["plan_id"], "interrupted_before_publish", now=service.now().timestamp()
                )
            raise
        except Exception:
            operation = service.store.get_operation(key)
            if operation and operation["status"] in ("inflight", "uncertain"):
                if operation["status"] == "inflight":
                    service.store.mark_operation_uncertain(key, "发布结果未取得，等待核对")
                service.store.finish_plan(plan["plan_id"], "uncertain")
            elif operation and operation["status"] == "done":
                service.store.finish_plan(
                    plan["plan_id"], "done", (operation.get("result") or {}).get("tid")
                )
            else:
                if not service.plan_is_current(plan):
                    _terminal(service, plan, "cancelled", "opportunity_expired")
                else:
                    service.store.schedule_plan_retry(
                        plan["plan_id"], stage + "_failed", now=service.now().timestamp()
                    )
                service.store.set_value("last_autonomous_error", stage + "_failed")
        break
