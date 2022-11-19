import re
from pathlib import Path
from typing import List, Tuple, Union, Optional

from loguru import logger

from transcribe_etl.transform.base import Processor
from transcribe_etl.transform.helper import convert_interval_to_milliseconds
from transcribe_etl.transform.model import TxData


class TextExtractAnnotator(Processor):
    def __init__(self, verbose: Optional[bool] = False):
        super().__init__(verbose=verbose)
        if not self.verbose:
            logger.disable("transform.text_extract")

    def execute(self, file: Union[str, Path]) -> List[TxData]:
        text = self._get_text_to_process(file=file)
        timed_transcriptions = self.parse_timed_transcriptions(text=text)
        segments = self.aggregate_and_process_segments(timed_transcriptions=timed_transcriptions)
        tx_list = _combine_and_measure_segments(segments)
        return [TxData.from_dict(x) for x in tx_list]

    @staticmethod
    def parse_timed_transcriptions(text: str) -> List[dict]:
        pattern = r"INTERVAL:(?P<interval>.+)\n(?P<transcription>TRANSCRIPTION: .+\n?)"
        return [x.groupdict() for x in re.finditer(pattern, text)]

    @classmethod
    def aggregate_and_process_segments(cls, timed_transcriptions: List[dict]) -> List[dict]:
        tx_data = []
        for x in timed_transcriptions:
            interval, transcription = x["interval"], x["transcription"]
            start_ms, end_ms = convert_interval_to_milliseconds(interval=interval)
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
        for segment in segments:
            duration = cls.parse_and_convert_duration(duration=segment["end_transcript"])
            segment["end_transcript"], segment["size"] = duration, segment_size
            new_utterances.append(segment)
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
        for segment in segments:
            segment["start_interval_ms"] = start_ms
            segment["end_interval_ms"] = end_ms
            new_segments.append(segment)
        return new_segments

    @staticmethod
    def _get_text_to_process(file: Union[str, Path]) -> str:
        with open(file=file) as f:
            return f.read()


def _get_speaker_tag(segment: dict, previous_segment: dict) -> str:
    if segment["speaker_tag"] == "<#no-speech>":
        return ""

    elif "TRANSCRIPTION" in segment["speaker_tag"] and previous_segment:
        return previous_segment["speaker_tag"]

    else:
        return segment["speaker_tag"]


def _get_text(segment: dict, previous_segment: dict) -> Tuple[dict, str]:
    if previous_segment:
        text = previous_segment["text"].strip() + " " + segment["text"].strip()
        previous_segment = {}
        return previous_segment, text

    else:
        text = segment["text"].strip() if segment["speaker_tag"] != "<#no-speech>" else "<#no-speech>"
        return previous_segment, text


def _get_interval_ms(eol: str, segment: dict, tx_data: List[dict]) -> Tuple[int, int]:
    start, end = 0, 0
    if eol == "\n":
        end = segment["end_interval_ms"]

        if segment["size"] > 1:
            start = tx_data[-1]["end"]
        else:
            start = segment["start_interval_ms"]

        return start, end

    elif isinstance(eol, int):
        start = tx_data[-1]["end"]
        end = segment["start_interval_ms"] + eol
        return start, end

    else:
        return start, end


def _combine_and_measure_segments(segments: List[dict]) -> List[dict]:
    tx_data = []
    previous_segment = {}

    for segment in segments:
        new_segment = {}
        eol = segment["end_transcript"]

        new_segment["speaker_tag"] = _get_speaker_tag(segment=segment, previous_segment=previous_segment)
        previous_segment, new_segment["text"] = _get_text(segment=segment, previous_segment=previous_segment)

        start, end = _get_interval_ms(eol=eol, segment=segment, tx_data=tx_data)
        new_segment["start"], new_segment["end"] = start, end

        if start == end == 0:
            previous_segment = segment
            continue

        tx_data.append(new_segment)

    return tx_data