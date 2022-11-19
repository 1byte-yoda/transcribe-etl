import re
from pathlib import Path
from typing import List, Tuple, Union, Optional

from loguru import logger

from transcribe_etl.speech_annotator.base import Processor
from transcribe_etl.speech_annotator.model import TxData


def combine_and_measure_segments(segments: List[dict]) -> List[dict]:
    tx_data = []
    previous_segment = {}
    for segment in segments:
        new_segment = {}
        eol = segment["end_transcript"]

        if segment["speaker_tag"] == "<#no-speech>":
            new_segment["speaker_tag"] = ""

        elif "TRANSCRIPTION" in segment["speaker_tag"] and previous_segment:
            new_segment["speaker_tag"] = previous_segment["speaker_tag"]

        else:
            new_segment["speaker_tag"] = segment["speaker_tag"]

        if previous_segment:
            new_segment["text"] = previous_segment["text"].strip() + " " + segment["text"].strip()
            previous_segment = {}

        else:
            new_segment["text"] = segment["text"].strip() if segment["speaker_tag"] != "<#no-speech>" else "<#no-speech>"

        if eol == "\n":
            if segment["size"] > 1:
                new_segment["start"] = tx_data[-1]["end"]
            else:
                new_segment["start"] = segment["start_interval_ms"]
            new_segment["end"] = segment["end_interval_ms"]

        elif isinstance(eol, int):
            new_segment["start"] = tx_data[-1]["end"]
            new_segment["end"] = segment["start_interval_ms"] + eol

        else:
            previous_segment = segment
            continue

        tx_data.append(new_segment)

    return tx_data


class TextExtractAnnotator(Processor):
    def __init__(self, verbose: Optional[bool] = False):
        super().__init__(verbose=verbose)
        if not self.verbose:
            logger.disable("speech_annotator.processor.text_extract")

    def execute(self, file: Union[str, Path]) -> List[TxData]:
        text = self._get_text_to_process(file=file)
        timed_transcriptions = self.parse_timed_transcriptions(text=text)
        segments = self.aggregate_and_process_segments(timed_transcriptions=timed_transcriptions)
        tx_list = combine_and_measure_segments(segments)
        return [TxData.from_dict(x) for x in tx_list]

    @staticmethod
    def parse_timed_transcriptions(text: str) -> List[dict]:
        pattern = r"INTERVAL:(?P<interval>.+)\n(?P<transcription>TRANSCRIPTION: .+\n?)"
        return [x.groupdict() for x in re.finditer(pattern, text)]

    @staticmethod
    def split_interval(interval: str) -> Tuple[str, str]:
        matched_groups = re.findall(r"(\d{2}:\d{2}:\d{1,2}\.*\d{0,3})\s(\d{2}:\d{2}:\d{1,2}\.*\d{0,3})",
                                    interval.strip())
        start, end = matched_groups[0] if len(matched_groups) > 0 else ("", "")
        return start, end

    @staticmethod
    def convert_duration_to_millisecond(duration: str) -> int:
        hours, minutes, seconds = duration.split(":")
        seconds, milliseconds = seconds.split(".")
        hours, minutes, seconds, milliseconds = map(int, (hours, minutes, seconds, milliseconds))
        return ((hours * 60 * 60) + (minutes * 60 + seconds)) * 1000 + milliseconds

    @classmethod
    def convert_interval_to_milliseconds(cls, interval: str) -> Tuple[int, int]:
        start, end = cls.split_interval(interval=interval)
        start_ms = cls.convert_duration_to_millisecond(duration=start)
        end_ms = cls.convert_duration_to_millisecond(duration=end)
        return start_ms, end_ms

    @classmethod
    def aggregate_and_process_segments(cls, timed_transcriptions: List[dict]) -> List[dict]:
        tx_data = []
        for x in timed_transcriptions:
            interval, transcription = x["interval"], x["transcription"]
            start_ms, end_ms = cls.convert_interval_to_milliseconds(interval=interval)
            utterances = cls.parse_and_process_segments(text=transcription)
            utterances = cls.insert_interval_to_segments(start_ms=start_ms, end_ms=end_ms, segments=utterances)
            tx_data.extend(utterances)
        return tx_data

    @classmethod
    def parse_and_convert_duration(cls, duration: str) -> Union[int, str]:
        pattern = r"\[(\d+\.\d+)\]"
        match = re.match(pattern, duration)
        new_duration = int(float(match.groups()[0]) * 1000) if match else duration
        return new_duration

    @classmethod
    def process_segments(cls, segments: List[dict]) -> List[dict]:
        new_utterances = []
        segment_size = len(segments)
        for x in segments:
            duration = cls.parse_and_convert_duration(duration=x["end_transcript"])
            x["end_transcript"] = duration
            x["size"] = segment_size
            new_utterances.append(x)
        return new_utterances

    @classmethod
    def parse_and_process_segments(cls, text: str) -> List[dict]:
        pattern = r"(?P<speaker_tag>TRANSCRIPTION: (?!<\#.+>)|\<\#.+?\>)(?P<text>.+?)(?P<end_transcript>\[\d+\.\d+\]|~|\n)"
        segments = [x.groupdict() for x in re.finditer(pattern, text)]
        new_segments = cls.process_segments(segments=segments)
        return new_segments

    @classmethod
    def insert_interval_to_segments(cls, start_ms: int, end_ms: int, segments: List[dict]) -> List[dict]:
        new_segments = []
        for x in segments:
            x["start_interval_ms"] = start_ms
            x["end_interval_ms"] = end_ms
            new_segments.append(x)
        return new_segments

    @staticmethod
    def _get_text_to_process(file: Union[str, Path]) -> str:
        with open(file=file) as f:
            return f.read()

