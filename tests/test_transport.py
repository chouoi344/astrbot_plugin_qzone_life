from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core.transport import QzoneReadError, QzoneTransport
from astrbot_plugin_qzone_life.vendor.qzone_protocol import parse_response, upload_fields


class FakeQQ:
    def __init__(self, uin=10001):
        self.uin = uin

    async def get_cookies(self, domain):
        return {"cookies": f"uin=o{self.uin}; skey=test_skey; p_skey=test_p_skey"}

    async def get_login_info(self):
        return {"user_id": self.uin, "nickname": "测试角色"}


def upload_response():
    return {
        "ret": 0,
        "data": {
            "url": "https://image.example/pic?x=1&bo=picture_token",
            "albumid": "album",
            "lloc": "large",
            "sloc": "small",
            "type": 1,
            "height": 640,
            "width": 480,
        },
    }


class TransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.transport = QzoneTransport(FakeQQ(), "10001")
        self.transport._request = AsyncMock()

    async def asyncTearDown(self):
        await self.transport.close()

    async def test_identity_contains_no_credentials(self):
        self.assertEqual(await self.transport.identity(), {"uin": 10001, "nickname": "测试角色"})

    async def test_wrong_account_prevents_publish(self):
        self.transport.client = FakeQQ(20002)
        result = await self.transport.publish("photo", [b"bytes"])
        self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
        self.transport._request.assert_not_awaited()

    async def test_credential_failure_before_any_mutation_is_definitely_unsent(self):
        self.transport._get_context = AsyncMock(side_effect=OSError("private cookie RPC details"))
        for name, args in (
            ("publish", ("post", [])),
            ("delete", ("post1",)),
            ("like", ("post1",)),
            ("reply", ("post1", {"id": "1", "uin": 20002}, "reply")),
        ):
            with self.subTest(operation=name):
                result = await getattr(self.transport, name)(*args)
                self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
                self.assertNotIn("private", str(result))
        self.transport._request.assert_not_awaited()

    async def test_credential_refresh_failure_inside_request_is_also_unsent(self):
        context = await self.transport._get_context()
        self.transport._get_context = AsyncMock(side_effect=[context, OSError("private details")])
        self.transport._request = QzoneTransport._request.__get__(self.transport)
        self.transport._http_session = AsyncMock()
        result = await self.transport.publish("post", [])
        self.assertEqual((result["status"], result["stage"]), ("rejected", "credentials"))
        self.transport._http_session.assert_not_awaited()

    async def test_credential_stage_cancellation_keeps_shutdown_semantics(self):
        self.transport._get_context = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.transport.publish("post", [])
        self.transport._request.assert_not_awaited()

    async def test_disagreeing_login_info_is_refused(self):
        await self.transport.identity()
        self.transport.client.uin = 20002
        with self.assertRaisesRegex(ValueError, "账号已变化"):
            await self.transport.identity()
        self.assertIsNone(self.transport._credentials)

    async def test_read_all_images_before_any_upload(self):
        result = await self.transport.publish(
            "photo", [b"valid", "/definitely/not/present/photo.png"]
        )
        self.assertEqual(result["status"], "rejected")
        self.transport._request.assert_not_awaited()

    async def test_local_file_and_bytes_are_uploaded_before_publish(self):
        self.transport._request.side_effect = [
            upload_response(),
            upload_response(),
            {"code": 0, "tid": "real_tid", "now": 12345},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "photo.png"
            path.write_bytes(b"local_photo")
            result = await self.transport.publish("真正文案", [path, b"other_photo"])
        self.assertEqual(result["status"], "success")
        self.assertEqual((result["tid"], result["created_at"]), ("real_tid", 12345))
        requests = self.transport._request.await_args_list
        self.assertEqual(
            [call.args[1] for call in requests],
            [self.transport.UPLOAD_URL, self.transport.UPLOAD_URL, self.transport.PUBLISH_URL],
        )
        data = requests[-1].kwargs["data"]
        self.assertEqual(data["con"], "真正文案")
        self.assertEqual(data["hostuin"], 10001)
        self.assertEqual(data["pic_bo"], "picture_token,picture_token")
        self.assertIn("\t", data["richval"])

    async def test_file_uri_can_be_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "一 张.png"
            path.write_bytes(b"photo")
            self.assertEqual(await self.transport._load_image(path.as_uri()), b"photo")

    async def test_empty_image_stops_publish(self):
        result = await self.transport.publish("text", [b""])
        self.assertEqual(result["status"], "rejected")
        self.transport._request.assert_not_awaited()

    async def test_upload_timeout_does_not_publish(self):
        self.transport._request.side_effect = asyncio.TimeoutError()
        result = await self.transport.publish("photo", [b"image"])
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["stage"], "upload")
        self.transport._request.assert_awaited_once()
        self.assertEqual(self.transport._request.await_args.args[1], self.transport.UPLOAD_URL)

    async def test_publish_timeout_is_unknown_and_never_replayed(self):
        self.transport._request.side_effect = asyncio.TimeoutError()
        result = await self.transport.publish("post", [])
        self.assertEqual(result["status"], "unknown")
        self.transport._request.assert_awaited_once()

    async def test_connector_failure_is_definite_unsent_and_drops_old_session(self):
        old_session = SimpleNamespace(closed=False, close=AsyncMock())
        self.transport._http = old_session
        connection = SimpleNamespace(host="secret-signed-host", port=443, ssl=True)
        self.transport._request.side_effect = aiohttp.ClientConnectorError(
            connection, OSError("secret network details")
        )
        result = await self.transport.publish("post", [])
        self.assertEqual((result["status"], result["stage"]), ("rejected", "connect"))
        self.assertNotIn("secret", str(result))
        self.transport._request.assert_awaited_once()
        old_session.close.assert_awaited_once()
        self.assertIsNone(self.transport._http)

    async def test_timeout_and_disconnect_stay_unknown_without_replay(self):
        for error in (asyncio.TimeoutError(), aiohttp.ServerDisconnectedError("secret")):
            with self.subTest(error=type(error).__name__):
                self.transport._request.reset_mock(side_effect=True)
                self.transport._request.side_effect = error
                result = await self.transport.publish("post", [])
                self.assertEqual(result["status"], "unknown")
                self.assertNotIn("secret", str(result))
                self.transport._request.assert_awaited_once()

    async def test_upload_can_finish_but_failed_final_check_prevents_publish_post(self):
        self.transport._request.return_value = upload_response()
        check = AsyncMock(side_effect=RuntimeError("expired secret details"))
        result = await self.transport.publish("post", [b"image"], before_publish=check)
        self.assertEqual((result["status"], result["stage"]), ("rejected", "preflight"))
        self.assertNotIn("secret", str(result))
        check.assert_awaited_once()
        self.transport._request.assert_awaited_once()
        self.assertEqual(self.transport._request.await_args.args[1], self.transport.UPLOAD_URL)

    async def test_final_check_cancellation_propagates_without_publish_post(self):
        self.transport._request.return_value = upload_response()
        check = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.transport.publish("post", [b"image"], before_publish=check)
        self.transport._request.assert_awaited_once()
        self.assertEqual(self.transport._request.await_args.args[1], self.transport.UPLOAD_URL)

    async def test_success_without_tid_is_unknown(self):
        self.transport._request.return_value = {"code": 0, "now": 12345}
        result = await self.transport.publish("post", [])
        self.assertEqual(result["status"], "unknown")
        self.transport._request.assert_awaited_once()

    async def test_http_server_failure_cannot_be_success(self):
        self.transport._request.return_value = {"code": 0, "tid": "maybe", "_http_status": 503}
        self.assertEqual((await self.transport.publish("post", []))["status"], "unknown")
        self.transport._request.assert_awaited_once()

    async def test_conflicting_success_and_failure_signals_are_unknown(self):
        for raw in (
            {"code": 0, "data": {"ret": -100}},
            {"ret": 0, "data": {"ret": "-100"}},
            {"code": 0, "_http_status": 409},
            {"code": 0, "_http_status": 401},
        ):
            with self.subTest(raw=raw):
                self.transport._request.reset_mock()
                self.transport._request.return_value = raw
                result = await self.transport.delete("post1")
                self.assertEqual(result["status"], "unknown")
                self.transport._request.assert_awaited_once()

    def test_consistent_failures_and_successes_keep_their_classification(self):
        for raw, expected in (
            ({"code": 0, "data": {"ret": 0}}, "success"),
            ({"code": -403, "_http_status": 403}, "rejected"),
            ({"code": -3000, "data": {"ret": -100}}, "rejected"),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(self.transport._classify(raw)["status"], expected)

    async def test_get_post_flattens_two_levels_without_losing_parent(self):
        self.transport._request.return_value = {
            "code": 0,
            "tid": "post1",
            "uin": 10001,
            "content": "正文",
            "created_time": 123,
            "pic": [{"url1": "https://image.example/small", "url2": "https://image.example/large"}],
            "commentlist": [
                {
                    "tid": 10,
                    "uin": 20002,
                    "name": "主楼",
                    "content": "first",
                    "create_time": 124,
                    "list_3": [
                        {
                            "tid": 2,
                            "uin": 30003,
                            "name": "管理员",
                            "content": "child",
                            "create_time": 125,
                        }
                    ],
                }
            ],
        }
        post = await self.transport.get_post("post1")
        self.assertEqual(post["images"], ["https://image.example/large"])
        self.assertEqual(post["comments"][0]["parent_id"], None)
        child = post["comments"][1]
        self.assertEqual(
            (child["id"], child["parent_id"], child["parent_uin"], child["uin"]),
            ("2", "10", 20002, 30003),
        )
        self.assertEqual(child["created_at"], 125)

    async def test_root_comment_reply_uses_real_comment_id(self):
        self.transport._request.return_value = {"code": 0, "tid": 99}
        result = await self.transport.reply(
            "post1", {"id": "10", "uin": 20002, "nickname": "用户", "parent_id": None}, "收到"
        )
        self.assertEqual(result["status"], "success")
        data = self.transport._request.await_args.kwargs["data"]
        self.assertEqual(data["topicId"], "10001_post1")
        self.assertEqual(data["commentId"], "10")
        self.assertEqual(data["commentUin"], 20002)
        self.assertEqual(data["replyUin"], 20002)
        self.assertTrue(data["content"].startswith("@{uin:20002,"))

    async def test_nested_reply_routes_to_root_and_mentions_child_author(self):
        self.transport._request.return_value = {"ret": 0}
        result = await self.transport.reply(
            "post1",
            {
                "id": "2",
                "uin": 30003,
                "nickname": "管理员",
                "parent_id": "10",
                "parent_uin": 20002,
            },
            "回答管理员",
        )
        self.assertEqual(result["status"], "success")
        data = self.transport._request.await_args.kwargs["data"]
        self.assertEqual(
            (data["commentId"], data["commentUin"], data["replyUin"]), ("10", 20002, 30003)
        )

    async def test_missing_root_route_stops_reply(self):
        result = await self.transport.reply(
            "post1", {"id": "2", "uin": 30003, "parent_id": "10"}, "reply"
        )
        self.assertEqual(result["status"], "rejected")
        self.transport._request.assert_not_awaited()

    async def test_no_reply_to_self(self):
        result = await self.transport.reply("post1", {"id": "1", "uin": 10001}, "reply")
        self.assertEqual(result["status"], "rejected")
        self.transport._request.assert_not_awaited()

    async def test_unknown_reply_does_not_fallback(self):
        self.transport._request.return_value = parse_response("<html>bad gateway</html>")
        result = await self.transport.reply("post1", {"id": "10", "uin": 20002}, "reply")
        self.assertEqual(result["status"], "unknown")
        self.transport._request.assert_awaited_once()

    async def test_explicit_reply_rejection_does_not_fallback(self):
        self.transport._request.return_value = {"code": -403, "message": "无权限"}
        result = await self.transport.reply("post1", {"id": "10", "uin": 20002}, "reply")
        self.assertEqual(result["status"], "rejected")
        self.transport._request.assert_awaited_once()

    async def test_delete_checks_response(self):
        self.transport._request.return_value = {"code": 403, "message": "无权限"}
        result = await self.transport.delete("post1")
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["tid"], "post1")

    async def test_like_uses_http_mood_keys(self):
        self.transport._request.return_value = {"code": 0}
        self.assertEqual((await self.transport.like("post1"))["status"], "success")
        data = self.transport._request.await_args.kwargs["data"]
        self.assertEqual(data["unikey"], "http://user.qzone.qq.com/10001/mood/post1")
        self.assertEqual(data["curkey"], data["unikey"])

    async def test_read_failure_raises_instead_of_looking_like_deleted(self):
        self.transport._request.return_value = {"code": -403, "message": "无权限"}
        with self.assertRaises(QzoneReadError):
            await self.transport.get_post("post1")

    async def test_mismatched_read_is_refused(self):
        self.transport._request.return_value = {
            "code": 0,
            "tid": "post1",
            "uin": 20002,
            "content": "other account",
        }
        with self.assertRaises(QzoneReadError):
            await self.transport.get_post("post1")

    async def test_empty_list_is_distinguished_from_unrecognized_response(self):
        self.transport._request.return_value = {"code": 0, "msglist": None}
        self.assertEqual(await self.transport.list_posts(), [])
        self.transport._request.return_value = {"code": 0}
        with self.assertRaises(QzoneReadError):
            await self.transport.list_posts()

    async def test_cancellation_is_propagated_for_durable_task_recovery(self):
        self.transport._request.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.transport.publish("post", [])


class ParserTests(unittest.TestCase):
    IFRAME_DELETE = (
        '<html><head><meta http-equiv="Content-Type" '
        'content="text/html; charset=UTF-8" /></head><body>'
        '<script type="text/javascript"> var cb;'
        'try{document.domain="example.invalid";cb=frameElement.callback;}'
        'catch(e){try{document.domain="example.invalid";cb=frameElement.callback;}'
        'catch(e){document.domain="example.invalid";cb=frameElement.callback;}} '
        'frameElement.callback({"code":0,"data":{"feeds":""},"message":"",'
        '"needVerify":0,"subcode":0}); </script></body></html>'
    )

    def test_real_delete_iframe_response(self):
        result = parse_response(self.IFRAME_DELETE)
        self.assertEqual(
            result, {"code": 0, "data": {"feeds": ""}, "message": "", "needVerify": 0, "subcode": 0}
        )

    def test_iframe_rejection_is_not_success(self):
        result = parse_response(self.IFRAME_DELETE.replace('"code":0', '"code":-403'))
        transport = QzoneTransport(FakeQQ(), "10001")
        self.assertEqual(transport._classify(result)["status"], "rejected")

    def test_incomplete_or_ambiguous_iframe_is_unknown(self):
        for response in (
            self.IFRAME_DELETE.replace('"code":0,', ""),
            self.IFRAME_DELETE.replace("); </script>", "); alert(1); </script>"),
            self.IFRAME_DELETE.replace(
                "); </script>", '); frameElement.callback({"code":0}); </script>'
            ),
            self.IFRAME_DELETE.replace("</html>", ""),
            self.IFRAME_DELETE.replace('"code":0', '"code":undefined'),
        ):
            with self.subTest(response=response):
                self.assertTrue(parse_response(response)["_parse_error"])

    def test_jsonp_and_chinese_content(self):
        self.assertEqual(
            parse_response('_preloadCallback({"code":0,"content":"日常"});')["content"], "日常"
        )
        self.assertEqual(
            parse_response('{"code":0,"content":"frameElement.callback( belongs to text"}')["code"],
            0,
        )

    def test_bad_response_is_explicitly_unknown(self):
        for value in ("", "   ", "<html>login</html>", "callback({broken);", "[]"):
            with self.subTest(value=value):
                result = parse_response(value)
                self.assertEqual(result["code"], -1)
                self.assertTrue(result["_parse_error"])

    def test_embedded_success_object_or_trailing_content_is_not_a_valid_response(self):
        for value in (
            '<html><script>window.bootstrap={"code":0}</script><body>login required</body></html>',
            '_preloadCallback({"code":0}); incomplete_response',
            '{"code":0}this is not json',
            'var bootstrap = {"code":0};',
            '[{"code":0}]',
        ):
            with self.subTest(value=value):
                parsed = parse_response(value)
                self.assertTrue(parsed.get("_parse_error"), parsed)
                self.assertEqual(
                    QzoneTransport(FakeQQ(), "10001")._classify(parsed)["status"], "unknown"
                )

    def test_complete_jsonp_with_nonstandard_values_preserves_quoted_content(self):
        response = "_Callback({code:0,optional:undefined,content:'literal [undefined,]'});"
        self.assertEqual(
            parse_response(response),
            {"code": 0, "optional": None, "content": "literal [undefined,]"},
        )

    def test_upload_token_uses_only_its_query_value_and_preserves_literal_plus(self):
        for url, expected in (
            ("https://image.example/pic?bo=first", "first"),
            ("https://image.example/pic?x=1&bo=token&rf=viewer", "token"),
            ("https://image.example/pic?bo=a+b&rf=viewer", "a+b"),
            ("https://image.example/pic?bo=a%2Bb%2F%3D&rf=viewer", "a+b/="),
        ):
            with self.subTest(url=url):
                response = upload_response()
                response["data"]["url"] = url
                self.assertEqual(upload_fields(response)[0], expected)

    def test_upload_token_missing_empty_or_duplicated_is_rejected(self):
        for query in ("rf=viewer", "bo=", "bo=first&bo=second", "bo=first&bo="):
            with self.subTest(query=query):
                response = upload_response()
                response["data"]["url"] = "https://image.example/pic?" + query
                with self.assertRaises(ValueError):
                    upload_fields(response)


if __name__ == "__main__":
    unittest.main()
