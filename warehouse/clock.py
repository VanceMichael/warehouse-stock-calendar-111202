"""日历边界与业务时区语义。

所有事实时刻以带偏移的 ISO-8601 文本登记（内部统一转 UTC 比较），
业务日按业务时区（IANA，默认 Asia/Shanghai）从时刻换算，而不是按
UTC 切日。日末快照对应业务时区自然日 24:00（即次日 00:00）那一刻
的持有状态；盘点截止时刻换算出的业务日即为盘点锚定的日末日。
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = 'Asia/Shanghai'


def zone(name):
    try:
        return ZoneInfo(name)
    except Exception as exc:
        raise ValueError(f'未知的 IANA 时区: {name}') from exc


def parse_instant(text):
    """解析带时区偏移的 ISO-8601 时刻，返回 UTC datetime。"""
    if not isinstance(text, str) or not text:
        raise ValueError('时刻必须是非空 ISO-8601 文本')
    normalized = text[:-1] + '+00:00' if text.endswith('Z') else text
    try:
        instant = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f'无法解析时刻: {text}') from exc
    if instant.tzinfo is None:
        raise ValueError(f'时刻必须带时区偏移: {text}')
    return instant.astimezone(timezone.utc)


def business_day(instant, tz):
    """时刻在业务时区下归属的自然日。"""
    return instant.astimezone(tz).date()


def parse_day(text):
    try:
        return date.fromisoformat(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'无法解析日期(YYYY-MM-DD): {text}') from exc


def iter_days(start, end):
    current = start
    step = timedelta(days=1)
    while current <= end:
        yield current
        current += step


def month_bounds(day):
    start = day.replace(day=1)
    if start.month == 12:
        next_start = date(start.year + 1, 1, 1)
    else:
        next_start = date(start.year, start.month + 1, 1)
    return start, next_start - timedelta(days=1)


def month_key(day):
    return f'{day.year:04d}-{day.month:02d}'
