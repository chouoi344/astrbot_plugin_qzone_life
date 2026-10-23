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
import time
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

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
        self, client: Any, expected_uin: int | str, timeout: int = 30, cookie_ttl: int = 1800
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
        self._credential_lock = asyncio.Lock()
        self._http: Any = None

    async def _get_context(self) -> Credentials:
        async with self._credential_lock:
            if self._credentials is not None and (
                not self.cookie_ttl or time.monotonic() - self._refreshed_at < self.cookie_ttl
            ):
                return self._credentials
            if self.client is None:
                raise ValueError("没有可用的 QQ 连接")
            response = await self.client.get_cookies(domain="user.qzone.qq.com")
            if isinstance(response, dict) and isinstance(response.get("data"), dict):
                response = response["data"]
            cookie_text = response.get("cookies") if isinstance(response, dict) else None
            if not isinstance(cookie_text, str) or not cookie_text.strip():
                raise ValueError("QQ 连接未提供空间登录凭证")
            try:
                cookies = {key: morsel.value for key, morsel in SimpleCookie(cookie_text).items()}
                uin_text = cookies.get("uin", "").removeprefix("o").removeprefix("O")
                uin = int(uin_text)
            except (ValueError, TypeError):
                raise ValueError("空间登录凭证中的 QQ 号无效") from None
            if uin != self.expected_uin:
                raise ValueError("登录 QQ 与配置的机器人 QQ 不一致，已停止空间操作")
            skey = cookies.get("skey", "")
            p_skey = cookies.get("p_skey", "") or skey
            if not p_skey:
                raise ValueError("空间登录凭证缺少必要字段")
            self._credentials = Credentials(uin, skey, p_skey)
            self._refreshed_at = time.monotonic()
            return self._credentials

    async def identity(self) -> dict[str, Any]:
        context = await self._get_context()
        nickname = str(context.uin)
        getter = getattr(self.client, "get_login_info", None)
        if getter is not None:
            result = getter()
            info = await result if inspect.isawaitable(result) else result
            if isinstance(info, dict) and isinstance(info.get("data"), dict):
                info = info["data"]
            if isinstance(info, dict):
                reported = number(info.get("user_id", info.get("uin")))
                if reported and reported != self.expected_uin:
                    self._credentials = None
                    raise ValueError("QQ 连接账号已变化，已停止空间操作")
                nickname = str(info.get("nickname") or nickname)
        return {"uin": context.uin, "nickname": nickname}

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
        self, method: str, url: str, *, params=None, data=None, timeout=None
    ) -> dict[str, Any]:
        import aiohttp

        context = await self._get_context()
        client = await self._http_session()
        async with client.request(
            method,
            url,
            params=params,
            data=data,
            headers=context.headers(),
            cookies=context.cookies(),
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=timeout or self.timeout),
        ) as response:
            body = await response.text()
            raw = parse_response(body)
            raw["_http_status"] = response.status
        # Refresh credentials on the NEXT task; never replay this write.
        nested = raw.get("data")
        if (
            response.status == 401
            or result_code(raw) == -3000
            or (isinstance(nested, dict) and number(nested.get("ret")) == -100)
        ):
            self._credentials = None
        return raw

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
        if http >= 400 or code != 0:
            return self._result(
                "rejected", tid=tid, message=message or f"空间拒绝操作（{code}）", code=code
            )
        return self._result("success", tid=tid, created_at=number(raw.get("now")), code=code)

    async def _mutate(
        self, url: str, *, params=None, data=None, tid: str = "", timeout=None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            raw = await self._request("POST", url, params=params, data=data, timeout=timeout)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Exception messages can contain signed URLs/cookies. Do not expose
            # them; the orchestrator already records which operation was active.
            return self._result(
                "unknown", tid=tid, message="请求未取得明确结果，请核对后再操作"
            ), {}
        return self._classify(raw, tid=tid), raw

    async def _read(self, url: str, *, params=None) -> dict[str, Any]:
        try:
            raw = await self._request("GET", url, params=params)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise QzoneReadError("读取空间失败，未取得可靠结果") from None
        result = self._classify(raw)
        if result["status"] != "success":
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

    async def publish(self, text: str, images: Any) -> dict[str, Any]:
        context = await self._get_context()
        if isinstance(images, (str, bytes, os.PathLike)):
            images = [images]
        images = list(images or [])
        if not str(text or "").strip() and not images:
            return self._result("rejected", message="说说内容不能为空")
        try:
            # Read every image before uploading anything. Never silently drop a
            # missing file and accidentally send a different or text-only post.
            payloads = [await self._load_image(source) for source in images]
        except asyncio.CancelledError:
            raise
        except Exception:
            return self._result("rejected", message="图片读取失败，尚未发布说说")
        pic_bos, richvals = [], []
        for payload in payloads:
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
                    "picfile": base64.b64encode(payload).decode("ascii"),
                },
                timeout=max(self.timeout, 60),
            )
            if result["status"] != "success":
                return self._result(
                    "rejected", message="图片上传未完成，尚未发布说说", stage="upload"
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
        if payloads:
            data.update(pic_bo=",".join(pic_bos), richtype="1", richval="\t".join(richvals))
        result, raw = await self._mutate(
            self.PUBLISH_URL, params={"g_tk": context.gtk, "uin": context.uin}, data=data
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

    async def like(self, tid: str) -> dict[str, Any]:
        tid = self._tid(tid)
        context = await self._get_context()
        mood = f"http://user.qzone.qq.com/{context.uin}/mood/{tid}"
        result, _ = await self._mutate(
            self.LIKE_URL,
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

    async def delete(self, tid: str) -> dict[str, Any]:
        tid = self._tid(tid)
        context = await self._get_context()
        result, _ = await self._mutate(
            self.DELETE_URL,
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

    async def reply(self, tid: str, comment: dict[str, Any], text: str) -> dict[str, Any]:
        tid = self._tid(tid)
        context = await self._get_context()
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
