import re
from typing import Tuple


def split_interval(interval: str) -> Tuple[str, str]:
    matched_groups = re.findall(r"(\d{2}:\d{2}:\d{1,2}\.*\d{0,3})\s(\d{2}:\d{2}:\d{1,2}\.*\d{0,3})",
                                interval.strip())
    start, end = matched_groups[0] if len(matched_groups) > 0 else ("", "")
    return start, end


def convert_duration_to_millisecond(duration: str) -> int:
    hours, minutes, seconds = duration.split(":")
    seconds, milliseconds = seconds.split(".")
    hours, minutes, seconds, milliseconds = map(int, (hours, minutes, seconds, milliseconds))
    return ((hours * 60 * 60) + (minutes * 60 + seconds)) * 1000 + milliseconds


def convert_interval_to_milliseconds(cls, interval: str) -> Tuple[int, int]:
    start, end = cls.split_interval(interval=interval)
    start_ms = cls.convert_duration_to_millisecond(duration=start)
    end_ms = cls.convert_duration_to_millisecond(duration=end)
    return start_ms, end_ms
