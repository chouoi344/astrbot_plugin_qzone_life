"""Generate a coherent status sentence without changing character chat style."""

import re

from .dialogue import RequestProblem


def single_sentence(text):
    """Validate visible boundaries, leaving style and wording to the model."""
    text = str(text or "").strip()
    if not text or len(text) > 2000 or "\n" in text or "\r" in text:
        return False
    # Keycap emoji contain digit codepoints but are decorations, not words.
    visible_words = re.sub(r"[#*0-9]\uFE0F?\u20E3", "", text)
    # A period is decimal punctuation only when both neighbours are digits.
    parts = re.split(r"[。！？!?]+|(?<!\d)\.|\.(?!\d)", visible_words)
    return sum(any(character.isalnum() for character in part) for part in parts) == 1


async def generate_caption(llm, event, summary, *, activity=""):
    facts = f"照片实际内容：{summary}"
    if activity:
        facts += f"\n当前日程活动：{activity}"
    prompt = (
        facts
        + "\n请为自己的QQ空间写配文，保留你的性格和语气，用连贯的一句完整的话自然表达照片中的场景与动作。"
        "这是公开说说配文，聊天中的短句、分段和逐句回复习惯不用于这里。合理使用句内逗号，避免零碎片语或几句短话拼接，不换行。"
        "只给这句配文，不加说明；内容依据照片与实际日程，避免编造未提供的经历。"
    )
    if not activity:
        prompt += "\n这是用户交来的照片，避免凭照片声称自己亲自拍摄或亲历，也不推断图片外发生的事。"
    for attempt in range(2):
        text = str(await llm(event, prompt)).strip()
        if single_sentence(text):
            return text
        prompt += "\n上一版有多句或分段。请重新生成连贯的一句完整配文，保留你自己的表达，不拼接或复述旧稿。"
    raise RequestProblem(
        "caption_not_one_sentence",
        {"result": "尚未得到连贯的一句话配文，照片尚未发布"},
        status="failed",
    )
