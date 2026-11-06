"""Strict, single-attempt transport for the bot's own QQ space.

No method publishes to an account other than expected_uin. Mutating requests
are never retried internally: an ambiguous result must be reconciled by the
durable task layer before another write is considered.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import os
import re
import tempfile
import time
from http.cookies import CookieError, SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .request_policy import QzoneDeferred, RequestPolicy

from ..vendor.qzone_protocol import (
    Credentials,
    number,
    parse_response,
    post_from_raw,
    result_code,
    upload_fields,
)


class QzoneReadError(RuntimeError):
    """The remote read did not establish a trustworthy result."""


class QzonePostDeletedError(QzoneReadError):
    """The detail endpoint explicitly says this post has already been deleted."""


class AccountIdentityError(ValueError):
    """The selected connection and its space credentials are not one identity."""


class AccountUnavailableError(AccountIdentityError):
    """The current login cannot be checked yet; no different account was proven."""


class _CredentialsUnavailable(RuntimeError):
    """Credential acquisition failed before an HTTP request could be sent."""


class _PreflightRejected(RuntimeError):
    """The role, permission, or opportunity changed before the HTTP write."""


class QzoneTransport:
    BASE = "https://user.qzone.qq.com"
    UPLOAD_URL = "https://up.qzone.qq.com/cgi-bin/upload/cgi_upload_image"
    PUBLISH_URL = BASE + "/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_publish_v6"
    LIKE_URL = BASE + "/proxy/domain/w.qzone.qq.com/cgi-bin/likes/internal_dolike_app"
    LIST_URL = BASE + "/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6"
    DETAIL_URL = BASE + "/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msgdetail_v6"
    REPLY_URL = BASE + "/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_re_feeds"
    DELETE_URL = (
        "https://h5.qzone.qq.com/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_delete_v6"
    )
    MAX_IMAGE_BYTES = 20 * 1024 * 1024

    def __init__(
        self, client: Any, expected_uin: int | str, timeout: int = 30, cookie_ttl: int = 1800,
        *, policy: RequestPolicy | None = None, staging_dir: str | Path | None = None,
    ):
        value = str(expected_uin).strip()
        if not value.isdigit() or int(value) <= 0:
            raise ValueError("必须配置机器人 QQ 号")
        self.client = client
        self.expected_uin = int(value)
        self.timeout = max(1, int(timeout))
        self.cookie_ttl = max(0, int(cookie_ttl))
        self._credentials: Credentials | None = None
        self._refreshed_at = 0.0
        self._connection_snapshot = None
        self._nickname = value
        self._credential_lock = asyncio.Lock()
        self._http: Any = None
        self.policy = policy or RequestPolicy(account=value)
        self.staging_dir = Path(staging_dir) if staging_dir is not None else None

    @staticmethod
    def _account_id(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            return 0
        text = str(value).strip()
        return int(text) if len(text) <= 20 and text.isascii() and text.isdigit() and int(text) > 0 else 0

    def _connection_state(self):
        """Observe reconnects without selecting an arbitrary reverse-WS account."""
        if self.client is None:
            raise AccountUnavailableError("没有可用的 QQ 连接")
        connections = getattr(self.client, "_wsr_api_clients", None)
        if isinstance(connections, dict):
            if not connections:
                raise AccountUnavailableError("QQ 连接已断开，等待自动恢复")
            if len(connections) != 1:
                raise AccountIdentityError("同一 QQ 连接中有多个账号，空间操作等待账号明确")
            account, connection = next(iter(connections.items()))
            if self._account_id(account) != self.expected_uin:
                raise AccountIdentityError("QQ 连接账号已变化，已停止原账号的空间操作")
            return (str(account), id(connection))
        return None

    @staticmethod
    def _response_data(response: Any) -> dict[str, Any]:
        if not isinstance(response, dict):
            raise AccountIdentityError("QQ 连接返回的账号信息无效，等待自动恢复")
        if "status" in response and response["status"] != "ok":
            raise AccountIdentityError("QQ 连接尚未提供有效登录状态，等待自动恢复")
        code = response.get("retcode", 0)
        if isinstance(code, bool) or code not in (0, "0"):
            raise AccountIdentityError("QQ 连接尚未提供有效登录状态，等待自动恢复")
        data = response.get("data", response)
        if not isinstance(data, dict):
            raise AccountIdentityError("QQ 连接返回的账号信息无效，等待自动恢复")
        return data

    async def _online(self):
        """The websocket may remain connected after QQ itself has logged out."""
        getter = getattr(self.client, "get_status", None)
        if not callable(getter):
            raise AccountUnavailableError("暂时无法核对 QQ 在线状态，等待自动恢复")
        try:
            self.policy.count("online_checks")
            result = getter()
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, timeout=min(self.timeout, 8))
            data = self._response_data(result)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise AccountUnavailableError("暂时无法核对 QQ 在线状态，等待自动恢复") from None
        if data.get("online") is not True:
            raise AccountUnavailableError("QQ 尚未在线，空间操作等待登录恢复")

    async def _login_identity(self) -> dict[str, Any]:
        await self._online()
        getter = getattr(self.client, "get_login_info", None)
        if not callable(getter):
            raise AccountUnavailableError("QQ 连接尚未提供账号查询，等待自动恢复")
        try:
            self.policy.count("login_checks")
            result = getter()
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, timeout=min(self.timeout, 8))
        except asyncio.CancelledError:
            raise
        except Exception:
            # SDK exceptions can contain credentials and endpoint URLs.
            raise AccountUnavailableError("暂时无法核对 QQ 登录账号，等待自动恢复") from None
        info = self._response_data(result)
        accounts = [self._account_id(info[key]) for key in ("user_id", "uin", "self_id") if key in info]
        if not accounts or not all(accounts) or len(set(accounts)) != 1:
            raise AccountIdentityError("QQ 连接返回的账号信息无效或冲突，等待自动恢复")
        if accounts[0] != self.expected_uin:
            raise AccountIdentityError("QQ 连接账号已变化，已停止原账号的空间操作")
        return info

    def _clear_credentials(self):
        self._credentials = None
        self._refreshed_at = 0.0
        self._connection_snapshot = None

    async def _get_context(self, *, force=False, verify_login=False, allow_expired=False) -> Credentials:
        async with self._credential_lock:
            attempted_refresh = False
            try:
                connection = self._connection_state()
                if self._credentials is not None and connection != self._connection_snapshot:
                    self._clear_credentials()
                self.policy.check()
                cached = self._credentials is not None and connection == self._connection_snapshot and (
                    allow_expired or not self.cookie_ttl
                    or time.monotonic() - self._refreshed_at < self.cookie_ttl
                )
                if cached and not force:
                    if verify_login:
                        info = await self._login_identity()
                        if self._connection_state() != connection:
                            raise AccountUnavailableError("QQ 连接正在重连，等待重新核对空间账号")
                        self._nickname = str(info.get("nickname") or self.expected_uin)
                    else:
                        await self._online()
                    return self._credentials
                self._clear_credentials()
                await self._login_identity()
                try:
                    attempted_refresh = True
                    self.policy.count("credential_refreshes")
                    response = await asyncio.wait_for(
                        self.client.get_cookies(domain="user.qzone.qq.com"),
                        timeout=min(self.timeout, 8),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise AccountUnavailableError("暂时无法取得空间登录凭据，等待自动恢复") from None
                cookie_text = self._response_data(response).get("cookies")
                if not isinstance(cookie_text, str) or not cookie_text.strip():
                    raise AccountIdentityError("QQ 连接未提供空间登录凭据")
                try:
                    cookies = {key: morsel.value for key, morsel in SimpleCookie(cookie_text).items()}
                    uin_text = cookies.get("uin", "").removeprefix("o").removeprefix("O")
                    uin = self._account_id(uin_text)
                except (CookieError, ValueError, TypeError):
                    raise AccountIdentityError("空间登录凭据中的 QQ 号无效") from None
                if not uin:
                    raise AccountIdentityError("空间登录凭据中的 QQ 号无效")
                if uin != self.expected_uin:
                    raise AccountIdentityError("空间登录凭据与当前机器人 QQ 不一致，等待自动恢复")
                skey = cookies.get("skey", "")
                p_skey = cookies.get("p_skey", "") or skey
                if not p_skey:
                    raise AccountIdentityError("空间登录凭据缺少必要字段")
                # Both reads belong to one still-connected account. Do not keep
                # a valid-looking Cookie if the login changed during its RPC.
                info = await self._login_identity()
                if self._connection_state() != connection:
                    raise AccountUnavailableError("QQ 连接正在重连，等待重新核对空间账号")
                self._credentials = Credentials(uin, skey, p_skey)
                self._connection_snapshot = connection
                self._nickname = str(info.get("nickname") or self.expected_uin)
                self._refreshed_at = time.monotonic()
                return self._credentials
            except QzoneDeferred:
                # A network/service cooldown says nothing about a healthy
                # Cookie. Keep it for the next task; TTL and connection checks
                # still run before it can be used again.
                raise
            except BaseException as exc:
                if attempted_refresh and not isinstance(exc, asyncio.CancelledError):
                    self.policy.failure("credentials")
                self._clear_credentials()
                raise

    async def identity(self, *, force: bool = False) -> dict[str, Any]:
        # An idle identity check uses the already verified account association.
        # TTL renewal belongs to an actual space operation, not the ready loop.
        context = await self._get_context(force=force, verify_login=True, allow_expired=True)
        return {"uin": context.uin, "nickname": self._nickname}

    async def _request_context(self) -> Credentials:
        try:
            return await self._get_context(verify_login=True)
        except asyncio.CancelledError:
            raise
        except (QzoneDeferred, AccountIdentityError):
            raise
        except Exception:
            raise _CredentialsUnavailable() from None

    def _credential_rejection(self, tid: str = "", *, temporary=True) -> dict[str, Any]:
        return self._result(
            "rejected", tid=tid,
            message="暂时无法取得空间登录状态，本次操作尚未发送", stage="credentials",
            retryable=temporary, blocked=True, retry_at=self.policy.retry_at, access_wait=temporary,
        )

    def _deferred_rejection(self, exc, tid=""):
        return self._result(
            "rejected", tid=tid, message="空间访问正在等待恢复，本次操作尚未发送",
            stage="cooldown", retryable=True, blocked=True, retry_at=exc.retry_at, access_wait=True,
        )

    async def _http_session(self) -> Any:
        if self._http is None or self._http.closed:
            import aiohttp

            # Cookies are passed per request and never retained from redirects.
            self._http = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                cookie_jar=aiohttp.DummyCookieJar(),
            )
        return self._http

    async def _request(
        self, method: str, url: str, *, params=None, data=None, timeout=None, context=None,
        before_write=None,
    ) -> dict[str, Any]:
        import aiohttp

        async with self.policy.lock:
            await self.policy.wait_turn(write=method != "GET")
            if method != "GET":
                await self._before_write(before_write)
            current = await self._request_context()
            if context is not None and context != current:
                # Query/body tokens were built from context. Never mix them with
                # a newly acquired Cookie, or keep uploading after reconnect.
                raise _CredentialsUnavailable()
            context = current
            client = await self._http_session()
            try:
                self.policy.sent(write=method != "GET")
                async with client.request(
                    method, url, params=params, data=data,
                    headers=context.headers(), cookies=context.cookies(),
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=timeout or self.timeout),
                ) as response:
                    body = await response.text()
                    raw = parse_response(body)
                    raw["_http_status"] = response.status
            except asyncio.CancelledError:
                raise
            except Exception:
                self.policy.failure("network")
                raise
            reason = self._response_reason(raw)
            if reason:
                self.policy.failure(reason)
                if reason == "authentication":
                    self._clear_credentials()
            elif self._classify(raw)["status"] == "success":
                self.policy.success()
            return raw

    @staticmethod
    async def _before_write(callback):
        if callback is None:
            return
        try:
            await callback()
        except (asyncio.CancelledError, QzoneDeferred, AccountUnavailableError):
            raise
        except Exception:
            raise _PreflightRejected() from None

    @staticmethod
    def _response_reason(raw):
        http = number(raw.get("_http_status"), 200)
        nested = raw.get("data")
        if http == 401 or result_code(raw) == -3000 or (
            isinstance(nested, dict) and number(nested.get("ret")) == -100
        ):
            return "authentication"
        if http in (403, 429):
            return "restricted"
        if http >= 500 or raw.get("_parse_error"):
            return "service"
        # Qzone also reports account-wide throttling and service saturation in
        # ordinary HTTP 200 CGI errors. Keep unrelated per-post refusals local.
        if result_code(raw) not in (None, 0):
            message = str(raw.get("message") or raw.get("msg") or "")
            if any(text in message for text in ("操作过于频繁", "请求过于频繁", "访问过于频繁")):
                return "restricted"
            if "使用人数过多" in message:
                return "service"
        return ""

    @staticmethod
    def _result(
        status: str, *, tid: str = "", created_at: int = 0, message: str = "", **extra
    ) -> dict[str, Any]:
        return {
            "status": status,
            "tid": str(tid),
            "created_at": created_at,
            "message": message,
            **extra,
        }

    def _classify(self, raw: dict[str, Any], *, tid: str = "") -> dict[str, Any]:
        http = number(raw.get("_http_status"), 200)
        code = result_code(raw)
        message = str(raw.get("message") or raw.get("msg") or "")
        if raw.get("_parse_error") or http >= 500 or 300 <= http < 400 or code in (None, -1):
            return self._result(
                "unknown", tid=tid, message=message or "空间操作结果不明，需要核对", code=code
            )
        nested = raw.get("data")
        nested_ret = result_code({"ret": nested.get("ret")}) if isinstance(nested, dict) else None
        if code == 0 and (http >= 400 or nested_ret not in (None, 0)):
            return self._result(
                "unknown", tid=tid, message="空间响应相互矛盾，需要核对后再操作", code=code
            )
        if http >= 400 or code != 0:
            return self._result(
                "rejected", tid=tid, message=message or f"空间拒绝操作（{code}）", code=code
            )
        return self._result("success", tid=tid, created_at=number(raw.get("now")), code=code)

    async def _mutate(
        self, url: str, *, params=None, data=None, tid: str = "", timeout=None, context=None,
        before_write=None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        import aiohttp

        try:
            raw = await self._request(
                "POST", url, params=params, data=data, timeout=timeout, context=context,
                before_write=before_write,
            )
        except asyncio.CancelledError:
            raise
        except QzoneDeferred as exc:
            return self._deferred_rejection(exc, tid), {}
        except _PreflightRejected:
            return self._result(
                "rejected", tid=tid, message="发布条件已变化，本次操作尚未发送", stage="preflight",
                retryable=False,
            ), {}
        except (_CredentialsUnavailable, AccountUnavailableError):
            return self._credential_rejection(tid), {}
        except AccountIdentityError:
            return self._credential_rejection(tid, temporary=False), {}
        except aiohttp.ClientConnectorError:
            # No HTTP connection was established, so this write was not sent.
            # Drop the stale connector; only the durable caller may retry later.
            try:
                await self.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._http = None
            return self._result(
                "rejected", tid=tid, message="空间连接未建立，本次操作尚未发送", stage="connect",
                retryable=True, blocked=True, retry_at=self.policy.retry_at,
            ), {}
        except Exception:
            # Exception messages can contain signed URLs/cookies. Do not expose
            # them; the orchestrator already records which operation was active.
            return self._result(
                "unknown", tid=tid, message="请求未取得明确结果，请核对后再操作"
            ), {}
        result = self._classify(raw, tid=tid)
        reason = self._response_reason(raw)
        result["retryable"] = result["status"] == "rejected" and bool(reason)
        if reason:
            result.update(blocked=True, retry_at=self.policy.retry_at, reason=reason)
        return result, raw

    async def _read(self, url: str, *, params=None, context=None) -> dict[str, Any]:
        try:
            raw = await self._request("GET", url, params=params, context=context)
        except asyncio.CancelledError:
            raise
        except (AccountIdentityError, QzoneDeferred):
            raise
        except Exception:
            if self.policy.retry_at <= self.policy._clock():
                self.policy.failure("network")
            raise QzoneDeferred(self.policy.reason, self.policy.retry_at) from None
        reason = self._response_reason(raw)
        if reason:
            if self.policy.retry_at <= self.policy._clock():
                self.policy.failure(reason)
            raise QzoneDeferred(reason, self.policy.retry_at)
        result = self._classify(raw)
        if result["status"] != "success":
            if (
                url == self.DETAIL_URL and result_code(raw) not in (None, 0)
                and any(text in result["message"] for text in (
                    "该说说已删除", "该说说已被删除", "说说已被删除",
                ))
            ):
                raise QzonePostDeletedError("空间已明确告知这条说说被删除")
            raise QzoneReadError(result["message"] or "读取空间失败")
        return raw

    async def _load_image(self, source: Any) -> bytes:
        if isinstance(source, bytes):
            payload = source
        elif isinstance(source, (str, os.PathLike)):
            value = os.fspath(source).strip()
            parsed = urlparse(value)
            if parsed.scheme in ("http", "https"):
                client = await self._http_session()
                # Never send Qzone cookies to an image host.
                async with client.get(value, allow_redirects=True) as response:
                    response.raise_for_status()
                    chunks, size = [], 0
                    async for chunk in response.content.iter_chunked(65536):
                        size += len(chunk)
                        if size > self.MAX_IMAGE_BYTES:
                            raise ValueError("图片超过允许大小")
                        chunks.append(chunk)
                    payload = b"".join(chunks)
            else:
                if parsed.scheme == "file":
                    if parsed.netloc not in ("", "localhost"):
                        raise ValueError("不支持远程 file 图片地址")
                    value = unquote(parsed.path)
                    if re.match(r"^/[A-Za-z]:/", value):
                        value = value[1:]
                elif parsed.scheme and not re.match(r"^[A-Za-z]:[\\/]", value):
                    raise ValueError("不支持的图片地址")
                path = Path(value)
                if not path.is_file() or path.stat().st_size > self.MAX_IMAGE_BYTES:
                    raise ValueError("图片不存在或超过允许大小")
                payload = await asyncio.to_thread(path.read_bytes)
        else:
            raise ValueError("图片必须是文件路径、网址或字节数据")
        if not payload or len(payload) > self.MAX_IMAGE_BYTES:
            raise ValueError("图片为空或超过允许大小")
        return payload

    async def publish(self, text: str, images: Any, *, before_publish=None) -> dict[str, Any]:
        try:
            context = await self._request_context()
        except QzoneDeferred as exc:
            return self._deferred_rejection(exc)
        except (_CredentialsUnavailable, AccountUnavailableError):
            return self._credential_rejection()
        except AccountIdentityError:
            return self._credential_rejection(temporary=False)
        if isinstance(images, (str, bytes, os.PathLike)):
            images = [images]
        images = list(images or [])
        if not str(text or "").strip() and not images:
            return self._result("rejected", message="说说内容不能为空")
        if len(images) > 9:
            return self._result("rejected", message="单条说说最多使用九张照片")
        try:
            if self.staging_dir is not None:
                self.staging_dir.mkdir(parents=True, exist_ok=True)
            staging = tempfile.TemporaryDirectory(
                prefix="qzone-upload-", dir=self.staging_dir, ignore_cleanup_errors=True,
            )
        except Exception:
            return self._result("rejected", message="图片读取失败，尚未发布说说")
        with staging as directory:
            prepared = []
            try:
                # Preserve a validated immutable snapshot of every image before
                # uploading any. Only one decoded file's bytes stay in memory.
                for index, source in enumerate(images):
                    payload = await self._load_image(source)
                    path = Path(directory) / str(index)
                    await asyncio.to_thread(path.write_bytes, payload)
                    prepared.append(path)
                    del payload
            except asyncio.CancelledError:
                raise
            except Exception:
                return self._result("rejected", message="图片读取失败，尚未发布说说")
            return await self._publish_prepared(context, text, prepared, before_publish)

    async def _publish_prepared(self, context, text, prepared, before_publish):
        pic_bos, richvals = [], []
        for path in prepared:
            try:
                payload = await self._load_image(path)
                encoded = base64.b64encode(payload).decode("ascii")
                del payload
            except asyncio.CancelledError:
                raise
            except Exception:
                return self._result("rejected", message="图片读取失败，尚未发布说说", stage="upload")
            result, raw = await self._mutate(
                self.UPLOAD_URL,
                data={
                    "filename": "filename",
                    "uploadtype": "1",
                    "albumtype": "7",
                    "skey": context.skey,
                    "uin": context.uin,
                    "p_skey": context.p_skey,
                    "output_type": "json",
                    "base64": "1",
                    "picfile": encoded,
                },
                timeout=max(self.timeout, 60), context=context,
            )
            del encoded
            if result["status"] != "success":
                return self._result(
                    "rejected", message="图片上传未完成，尚未发布说说", stage="upload",
                    retryable=result.get("retryable", True),
                    blocked=result.get("blocked", False), retry_at=result.get("retry_at", 0),
                    access_wait=result.get("access_wait", False),
                )
            try:
                picbo, richval = upload_fields(raw)
            except ValueError:
                return self._result(
                    "rejected", message="图片上传响应不完整，尚未发布说说", stage="upload"
                )
            pic_bos.append(picbo)
            richvals.append(richval)
        data = {
            "syn_tweet_verson": "1",
            "paramstr": "1",
            "who": "1",
            "con": str(text or ""),
            "feedversion": "1",
            "ver": "1",
            "ugc_right": "1",
            "to_sign": "0",
            "hostuin": context.uin,
            "code_version": "1",
            "format": "json",
            "qzreferrer": f"{self.BASE}/{context.uin}",
        }
        if prepared:
            data.update(pic_bo=",".join(pic_bos), richtype="1", richval="\t".join(richvals))
        result, raw = await self._mutate(
            self.PUBLISH_URL, params={"g_tk": context.gtk, "uin": context.uin}, data=data,
            context=context, before_write=before_publish,
        )
        if result["status"] == "success":
            nested = raw.get("data") if isinstance(raw.get("data"), dict) else {}
            tid = str(raw.get("tid") or nested.get("tid") or "")
            if not tid or tid == "0":
                return self._result("unknown", message="空间响应未包含说说标识，需要核对")
            result.update(
                tid=tid, created_at=number(raw.get("now", nested.get("now")), int(time.time()))
            )
        return result

    @staticmethod
    def _tid(tid: str) -> str:
        result = str(tid or "").strip()
        if not result or result == "0" or not re.fullmatch(r"[A-Za-z0-9_-]+", result):
            raise ValueError("说说标识无效")
        return result

    async def list_posts(self, limit: int = 20) -> list[dict[str, Any]]:
        context = await self._get_context()
        limit = max(1, min(int(limit), 100))
        raw = await self._read(
            self.LIST_URL,
            context=context,
            params={
                "g_tk": context.gtk,
                "uin": context.uin,
                "ftype": 0,
                "sort": 0,
                "pos": 0,
                "num": limit,
                "replynum": 100,
                "callback": "_preloadCallback",
                "code_version": 1,
                "format": "json",
                "need_comment": 1,
                "need_private_comment": 1,
            },
        )
        source = (
            raw.get("data") if isinstance(raw.get("data"), dict) and "msglist" not in raw else raw
        )
        if "msglist" not in source:
            raise QzoneReadError("空间列表响应缺少列表字段")
        items = source.get("msglist") or []
        if not isinstance(items, list):
            raise QzoneReadError("空间列表响应格式异常")
        posts = [post_from_raw(item, uin=context.uin) for item in items if isinstance(item, dict)]
        if any(post["uin"] != context.uin for post in posts):
            raise QzoneReadError("空间列表包含非目标账号内容")
        return posts[:limit]

    async def get_post(self, tid: str) -> dict[str, Any]:
        tid = self._tid(tid)
        context = await self._get_context()
        raw = await self._read(
            self.DETAIL_URL,
            context=context,
            params={"uin": context.uin, "tid": tid, "format": "jsonp", "g_tk": context.gtk},
        )
        source = raw
        if isinstance(raw.get("data"), dict) and not any(
            key in raw for key in ("tid", "commentlist", "content")
        ):
            source = raw["data"]
        if not any(key in source for key in ("tid", "commentlist", "content")):
            raise QzoneReadError("空间详情响应缺少帖子内容")
        post = post_from_raw(source, tid=tid, uin=context.uin)
        if post["uin"] != context.uin or post["tid"] != tid:
            raise QzoneReadError("空间详情与请求的帖子不一致")
        return post

    async def like(self, tid: str, *, before_write=None) -> dict[str, Any]:
        tid = self._tid(tid)
        try:
            context = await self._request_context()
        except QzoneDeferred as exc:
            return self._deferred_rejection(exc, tid)
        except (_CredentialsUnavailable, AccountUnavailableError):
            return self._credential_rejection(tid)
        except AccountIdentityError:
            return self._credential_rejection(tid, temporary=False)
        mood = f"http://user.qzone.qq.com/{context.uin}/mood/{tid}"
        result, _ = await self._mutate(
            self.LIKE_URL,
            context=context,
            before_write=before_write,
            params={"g_tk": context.gtk},
            data={
                "qzreferrer": f"{self.BASE}/{context.uin}",
                "opuin": context.uin,
                "unikey": mood,
                "curkey": mood,
                "appid": 311,
                "from": 1,
                "typeid": 0,
                "abstime": int(time.time()),
                "fid": tid,
                "active": 0,
                "format": "json",
                "fupdate": 1,
            },
            tid=tid,
        )
        return result

    async def delete(self, tid: str, *, before_write=None) -> dict[str, Any]:
        tid = self._tid(tid)
        try:
            context = await self._request_context()
        except QzoneDeferred as exc:
            return self._deferred_rejection(exc, tid)
        except (_CredentialsUnavailable, AccountUnavailableError):
            return self._credential_rejection(tid)
        except AccountIdentityError:
            return self._credential_rejection(tid, temporary=False)
        result, _ = await self._mutate(
            self.DELETE_URL,
            context=context,
            before_write=before_write,
            params={"g_tk": context.gtk},
            data={
                "uin": context.uin,
                "topicId": f"{context.uin}_{tid}__1",
                "feedsType": 0,
                "feedsFlag": 0,
                "feedsKey": tid,
                "feedsAppid": 311,
                "feedsTime": int(time.time()),
                "fupdate": 1,
                "ref": "feeds",
                "qzreferrer": (
                    f"{self.BASE}/proxy/domain/ic2.qzone.qq.com/cgi-bin/feeds/feeds_html_module"
                    f"?g_iframeUser=1&i_uin={context.uin}&i_login_uin={context.uin}"
                    "&mode=4&previewV8=1&style=35&version=8&needDelOpr=true"
                ),
            },
            tid=tid,
        )
        return result

    async def reply(self, tid: str, comment: dict[str, Any], text: str, *, before_write=None) -> dict[str, Any]:
        tid = self._tid(tid)
        try:
            context = await self._request_context()
        except QzoneDeferred as exc:
            return self._deferred_rejection(exc, tid)
        except (_CredentialsUnavailable, AccountUnavailableError):
            return self._credential_rejection(tid)
        except AccountIdentityError:
            return self._credential_rejection(tid, temporary=False)
        target_uin = number(comment.get("uin"))
        if target_uin <= 0 or target_uin == context.uin:
            return self._result("rejected", tid=tid, message="回复对象无效或为机器人自身")
        target_id = str(comment.get("id") or "")
        parent_id = comment.get("parent_id")
        main_id = str(parent_id) if parent_id is not None else target_id
        if (
            not target_id.isdigit()
            or int(target_id) <= 0
            or not main_id.isdigit()
            or int(main_id) <= 0
        ):
            return self._result("rejected", tid=tid, message="评论缺少可靠的服务端楼层标识")
        root_uin = number(comment.get("parent_uin")) if parent_id is not None else target_uin
        if root_uin <= 0:
            return self._result("rejected", tid=tid, message="楼中楼缺少主评论作者")
        content = str(text or "").strip()
        if not content:
            return self._result("rejected", tid=tid, message="回复内容不能为空")
        # QQ's mention syntax uses punctuation as delimiters. Keep nickname text
        # from accidentally breaking the wire structure.
        nickname = re.sub(r"[{},\r\n]", " ", str(comment.get("nickname") or target_uin)).strip()
        content = f"@{{uin:{target_uin},nick:{nickname},auto:1}} {content}"
        result, raw = await self._mutate(
            self.REPLY_URL,
            context=context,
            before_write=before_write,
            params={"g_tk": context.gtk},
            data={
                "hostUin": context.uin,
                "topicId": f"{context.uin}_{tid}",
                "content": content,
                "format": "json",
                "qzreferrer": f"{self.BASE}/{context.uin}/main",
                "commentId": main_id,
                "replyUin": target_uin,
                "commentUin": root_uin,
            },
            tid=tid,
        )
        if result["status"] == "success":
            nested = raw.get("data") if isinstance(raw.get("data"), dict) else {}
            result["reply_id"] = str(
                raw.get("tid") or raw.get("commentid") or nested.get("tid") or ""
            )
            result["created_at"] = result["created_at"] or int(time.time())
        return result

    async def close(self) -> None:
        if self._http is not None and not self._http.closed:
            await self._http.close()
        self._http = None
        self._credentials = None
