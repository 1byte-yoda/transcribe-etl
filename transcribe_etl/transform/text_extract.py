import re
from pathlib import Path
from typing import List, Tuple, Union, Optional

from loguru import logger

from transcribe_etl.transform.base import Processor
from transcribe_etl.transform.helper import convert_interval_to_milliseconds
from transcribe_etl.transform.model import TxData, ExtractedTranscription, Transcription, Segment, TxDataGroup


class TextExtractAnnotator(Processor):
    def __init__(self, verbose: Optional[bool] = False):
        super().__init__(verbose=verbose)
        if not self.verbose:
            logger.disable("transform.text_extract")

    def execute(self, file: Union[str, Path]) -> List[TxDataGroup]:
        text = self._get_text_to_process(file=file)
        timed_transcriptions = self.parse_timed_transcriptions(text=text)
        segments = self.convert_to_segments(extracted_transcriptions=timed_transcriptions)
        concatenated_segments = _combine_and_measure_segments(segments=segments)
        aggregated_tx_data = aggregate_segments(segments=concatenated_segments)
        return aggregated_tx_data

    @staticmethod
    def _get_text_to_process(file: Union[str, Path]) -> str:
        with open(file=file) as f:
            return f.read()

    @staticmethod
    def parse_timed_transcriptions(text: str) -> List[ExtractedTranscription]:
        pattern = r"FILE: (?P<file>.+)\nINTERVAL:(?P<interval>.+)\n(?P<transcription>TRANSCRIPTION: .+\n)(?P<hypothesis>HYPOTHESIS: .*\n)?LABELS: (?P<labels>.*)?\nUSER: (?P<user>.*)"  # noqa
        return [ExtractedTranscription.from_dict(x.groupdict()) for x in re.finditer(pattern, text)]

    @classmethod
    def convert_to_segments(cls, extracted_transcriptions: List[ExtractedTranscription]) -> List[Segment]:
        segments = []
        for x in extracted_transcriptions:
            transcriptions = cls.parse_and_process_transcriptions(text=x.transcription)
            new_segments = cls.create_segments(extracted_transcription=x, segments=transcriptions)
            segments.extend(new_segments)
        return segments

    @classmethod
    def parse_and_process_transcriptions(cls, text: str) -> List[Transcription]:
        pattern = r"(?P<speaker_tag>TRANSCRIPTION: (?!<\#.+>)|\<\#.+?\>)(?P<text>.+?)(?P<eol>\[\d+\.\d+\]|~|\n)"
        transcriptions = [x.groupdict() for x in re.finditer(pattern, text)]
        new_transcriptions = []
        for x in transcriptions:
            x["eol"] = cls.parse_and_convert_eol(duration=x["eol"])
            x["size"] = len(transcriptions)
            new_transcriptions.append(Transcription.from_dict(x))
        return new_transcriptions

    @classmethod
    def parse_and_convert_eol(cls, duration: str) -> Union[int, str]:
        pattern = r"\[(\d+\.\d+)\]"
        match = re.match(pattern, duration)
        new_duration = int(float(match.groups()[0]) * 1000) if match else duration
        return new_duration

    @classmethod
    def create_segments(cls, extracted_transcription: ExtractedTranscription, segments: List[Transcription]) -> List[Segment]:
        new_segments = []
        start_ms, end_ms = convert_interval_to_milliseconds(interval=extracted_transcription.interval)
        for s in segments:
            s = Segment(**s.to_dict(), start=start_ms, end=end_ms, file=extracted_transcription.file.strip())
            new_segments.append(s)
        return new_segments


def _get_speaker_tag(segment: Segment, previous_segment: Segment) -> str:
    if segment.speaker_tag == "<#no-speech>":
        return ""

    elif "TRANSCRIPTION" in segment.speaker_tag and previous_segment:
        return previous_segment.speaker_tag

    else:
        return segment.speaker_tag


def _get_text(segment: Segment, previous_segment: Segment) -> Tuple[Optional[Segment], str]:
    if previous_segment:
        text = previous_segment.text.strip() + " " + segment.text.strip()
        previous_segment = None
        return previous_segment, text

    else:
        text = segment.text.strip() if segment.speaker_tag != "<#no-speech>" else "<#no-speech>"
        return previous_segment, text


def _get_interval_ms(segment: Segment, tx_data: List[Segment]) -> Tuple[int, int]:
    eol = segment.eol
    is_within_same_audio = tx_data and tx_data[-1].file == segment.file

    if is_within_same_audio:
        if eol == "~":
            start, end = 0, 0
        elif eol == "\n":
            start = tx_data[-1].end
            end = segment.end
        else:
            start = tx_data[-1].end
            end = segment.start + eol
        return start, end

    else:
        start, end = segment.start, segment.end
        return start, end


def _combine_and_measure_segments(segments: List[Segment]) -> List[Segment]:
    previous_segment = None

    tx_data = []
    for segment in segments:
        speaker_tag = _get_speaker_tag(segment=segment, previous_segment=previous_segment)
        previous_segment, text = _get_text(segment=segment, previous_segment=previous_segment)

        start, end = _get_interval_ms(segment=segment, tx_data=tx_data)
        start, end = start, end

        if start == end == 0:
            previous_segment = segment
            continue

        new_segment = Segment(speaker_tag=speaker_tag, text=text, start=start, end=end, file=segment.file, size=segment.size, eol=segment.eol)
        tx_data.append(new_segment)

    return tx_data


def aggregate_segments(segments: List[Segment]) -> List[TxDataGroup]:
    tx_group = {}
    for s in segments:
        if s.file not in tx_group:
            tx_group[s.file] = {"tx_data": [], "duration": 0}

        tx_data = TxData(speaker_tag=s.speaker_tag, text=s.text, start=s.start, end=s.end)
        tx_group[s.file]["duration"] += s.end - s.start
        tx_group[s.file]["tx_data"].append(tx_data)

    return [TxDataGroup(file=filepath, segments=tx_group[filepath]["tx_data"]) for filepath in tx_group]
