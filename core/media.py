"""Collect quoted images first, then persist actual bytes for publication and vision."""

import asyncio
import base64
import hashlib
import io
from pathlib import Path

import aiohttp
from PIL import Image as PILImage, ImageOps
from .dialogue import RequestProblem

MAX_BYTES = 20 * 1024 * 1024


def component_type(segment):
    if isinstance(segment, dict):
        return str(segment.get("type", "")).lower()
    kind = getattr(segment, "type", type(segment).__name__)
    return str(getattr(kind, "value", kind)).lower()


async def collect_images(event, cache_dir: Path, limit=9) -> list[str]:
    chain = event.get_messages()
    quoted = []
    for segment in chain:
        if component_type(segment) != "reply":
            continue
        quoted = list(
            (segment.get("chain") if isinstance(segment, dict) else getattr(segment, "chain", None))
            or []
        )
        if not quoted:
            reply_id = (
                segment.get("data", {}).get("id")
                if isinstance(segment, dict)
                else getattr(segment, "id", None)
            )
            if reply_id and getattr(event, "bot", None):
                raw = await event.bot.get_msg(message_id=int(reply_id))
                quoted = raw.get("message", []) if isinstance(raw, dict) else []
        if any(component_type(item) == "image" for item in quoted):
            break
    selected = [seg for seg in quoted if component_type(seg) == "image"]
    if not selected:
        selected = [seg for seg in chain if component_type(seg) == "image"]
    if not selected:
        raise RequestProblem(
            "photo_required",
            {"result": "本条消息及引用内容中都没有照片", "next": "需要用户提供或引用要发布的照片"},
        )
    if len(selected) > limit:
        raise RequestProblem(
            "too_many_photos", {"result": "照片数量超过单条说说限制", "max_images": limit}
        )
    cache_dir.mkdir(parents=True, exist_ok=True)
    result = []
    for segment in selected:
        if isinstance(segment, dict):
            data = segment.get("data", {})
            reference = data.get("url") or data.get("file")
        else:
            reference = getattr(segment, "url", None) or getattr(segment, "file", None)
            if not reference and hasattr(segment, "convert_to_file_path"):
                reference = await segment.convert_to_file_path()
        if not reference:
            raise RequestProblem(
                "photo_unavailable", {"result": "引用的照片当前无法读取，需要用户重新提供"}
            )
        image = await read_image(str(reference))
        digest = hashlib.sha256(image).hexdigest()
        target = cache_dir / f"{digest}.png"
        if not target.exists():
            await asyncio.to_thread(target.write_bytes, image)
        result.append(str(target))
    return result


async def read_image(reference: str) -> bytes:
    if reference.startswith("base64://"):
        if len(reference) > MAX_BYTES * 2:
            raise ValueError("图片过大")
        data = base64.b64decode(reference[9:], validate=True)
    elif reference.startswith(("https://", "http://")):
        from urllib.parse import urlparse
        import ipaddress

        parsed = urlparse(reference)
        if parsed.hostname in ("localhost", None) or parsed.username or parsed.password:
            raise ValueError("不支持的图片地址")
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            address = None
        if address and not address.is_global:
            raise ValueError("不读取内网图片地址，请直接上传图片")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
            async with session.get(reference, allow_redirects=False) as response:
                response.raise_for_status()
                if response.status != 200:
                    raise ValueError("图片地址需要重定向，请重新发送图片")
                chunks, total = [], 0
                async for chunk in response.content.iter_chunked(65536):
                    total += len(chunk)
                    if total > MAX_BYTES:
                        raise ValueError("图片超过 20 MB")
                    chunks.append(chunk)
                data = b"".join(chunks)
    else:
        from urllib.parse import unquote, urlparse

        if reference.startswith("file://"):
            reference = unquote(urlparse(reference).path)
            if len(reference) > 3 and reference[0] == "/" and reference[2] == ":":
                reference = reference[1:]
        path = Path(reference)
        if not path.is_file() or path.stat().st_size > MAX_BYTES:
            raise ValueError("图片不存在或超过 20 MB")
        data = await asyncio.to_thread(path.read_bytes)
    if not data or len(data) > MAX_BYTES:
        raise ValueError("图片为空或超过 20 MB")

    def normalize():
        with PILImage.open(io.BytesIO(data)) as image:
            if image.width * image.height > 40_000_000:
                raise ValueError("图片像素过大")
            image.load()
            output = io.BytesIO()
            # Phone photos often store their displayed rotation only in EXIF.
            # Bake it into the pixels before conversion discards that metadata.
            with ImageOps.exif_transpose(image) as oriented:
                with oriented.convert("RGB") as normalized:
                    normalized.save(output, format="PNG")
            if output.tell() > MAX_BYTES:
                raise ValueError("图片转换后超过 20 MB")
            return output.getvalue()

    return await asyncio.to_thread(normalize)
