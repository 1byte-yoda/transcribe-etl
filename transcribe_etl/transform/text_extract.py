import re
from pathlib import Path
from typing import List, Tuple, Union, Optional

from loguru import logger

from transcribe_etl.transform.base import Processor
from transcribe_etl.transform.helper import convert_interval_to_milliseconds
from transcribe_etl.transform.model import TxData, ExtractedTranscription, Transcription, Segment, TxDataGroup


class SegmentProcessor:
    @staticmethod
    def _get_speaker_tag_field(segment: Segment, previous_segment: Segment) -> str:
        if segment.speaker_tag == "<#no-speech>":
            return ""

        elif "TRANSCRIPTION" in segment.speaker_tag and previous_segment:
            return previous_segment.speaker_tag

        else:
            return segment.speaker_tag

    @staticmethod
    def _get_text_field(segment: Segment, previous_segment: Segment) -> Tuple[Optional[Segment], str]:
        if previous_segment and previous_segment.eol == "~":
            text = previous_segment.text.strip() + " " + segment.text.strip()
            previous_segment = None
            return previous_segment, text

        else:
            text = segment.text.strip() if segment.speaker_tag != "<#no-speech>" else "<#no-speech>"
            return previous_segment, text

    @staticmethod
    def _get_interval_field(segment: Segment, tx_data: List[Segment]) -> Tuple[int, int]:
        is_within_same_audio = tx_data and tx_data[-1].file == segment.file

        if segment.eol == "~":
            return 0, 0

        elif is_within_same_audio:
            if segment.eol == "\n":
                start = tx_data[-1].end
                end = segment.end
            else:
                start = tx_data[-1].end
                end = segment.start + segment.eol
            return start, end

        else:
            if isinstance(segment.eol, int):
                start = segment.start
                end = segment.start + segment.eol
            else:
                start, end = segment.start, segment.end
            return start, end

    def combine_and_measure_segments(self, segments: List[Segment]) -> List[Segment]:
        logger.info("Combining segments using tilde, and measuring the duration for each segments")
        previous_segment = None
        tx_data = []
        for segment in segments:
            previous_segment, new_segment = self._adjust_duration_and_speaker_text_tags(previous_segment, segment, tx_data=tx_data)

            if new_segment.start == new_segment.end == 0:
                previous_segment = segment
                continue

            logger.debug(f"Parsed: {new_segment}")
            tx_data.append(new_segment)

        logger.info(f"Finished combining {len(tx_data)} segments and measuring transcription duration")
        return tx_data

    def _adjust_duration_and_speaker_text_tags(self, previous_segment: Segment, segment: Segment, tx_data: List[Segment]) -> Tuple[Segment, Segment]:
        speaker_tag = self._get_speaker_tag_field(segment=segment, previous_segment=previous_segment)
        previous_segment, text = self._get_text_field(segment=segment, previous_segment=previous_segment)
        start, end = self._get_interval_field(segment=segment, tx_data=tx_data)
        new_segment = Segment(speaker_tag=speaker_tag, text=text, start=start, end=end, file=segment.file, size=segment.size, eol=segment.eol)
        return previous_segment, new_segment

    @staticmethod
    def aggregate_segments(segments: List[Segment]) -> List[TxDataGroup]:
        logger.info("Aggregating Segments based on filename.")
        tx_group = {}
        for s in segments:
            if s.file not in tx_group:
                tx_group[s.file] = {"tx_data": [], "duration": 0}

            tx_data = TxData(speaker_tag=s.speaker_tag, text=s.text, start=s.start, end=s.end)
            tx_group[s.file]["duration"] += s.end - s.start
            tx_group[s.file]["tx_data"].append(tx_data)
            logger.debug(f"Aggregated Segment: {tx_data}")
        return [TxDataGroup(file=filepath, tx_data=tx_group[filepath]["tx_data"]) for filepath in tx_group]


class TextExtractParser(Processor):
    def __init__(self, segment_processor: SegmentProcessor, verbose: Optional[bool] = False):
        super().__init__(verbose=verbose)
        self.segment_processor = segment_processor
        if not self.verbose:
            logger.disable("transform.text_extract")

    def execute(self, file: Union[str, Path]) -> List[TxDataGroup]:
        logger.info(f"Parsing transcriptions from {file}.")
        text = self._get_text_to_process(file=file)
        timed_transcriptions = self.parse_timed_transcriptions(text=text)
        segments = self.convert_to_segments(extracted_transcriptions=timed_transcriptions)
        concatenated_segments = self.segment_processor.combine_and_measure_segments(segments=segments)
        aggregated_tx_data = self.segment_processor.aggregate_segments(segments=concatenated_segments)
        return aggregated_tx_data

    @staticmethod
    def _get_text_to_process(file: Union[str, Path]) -> str:
        with open(file=file) as f:
            return f.read()

    @staticmethod
    def parse_timed_transcriptions(text: str) -> List[ExtractedTranscription]:
        pattern = r"FILE:\s?(?P<file>.+)\nINTERVAL:\s?(?P<interval>.+)\n(?P<transcription>TRANSCRIPTION:\s?.+\n)(?P<hypothesis>HYPOTHESIS:\s?.*\n)?LABELS:\s?(?P<labels>.*)?\nUSER:\s?(?P<user>.*)"  # noqa
        extracted_transcriptions = [ExtractedTranscription.from_dict(x.groupdict()) for x in re.finditer(pattern, text)]
        logger.debug(f"Extracted {len(extracted_transcriptions)} transcriptions.")
        return extracted_transcriptions

    @classmethod
    def convert_to_segments(cls, extracted_transcriptions: List[ExtractedTranscription]) -> List[Segment]:
        logger.info("Parsing TX Fields from the Extracted Transcriptions.")
        segments = []
        for x in extracted_transcriptions:
            transcriptions = cls.parse_and_process_transcriptions(text=x.transcription)
            new_segments = cls.create_segments(extracted_transcription=x, segments=transcriptions)
            segments.extend(new_segments)
        logger.debug(f"Parsing {len(segments)} segments finished.")
        return segments

    @classmethod
    def parse_and_process_transcriptions(cls, text: str) -> List[Transcription]:
        pattern = r"(?P<speaker_tag>TRANSCRIPTION: (?!<\#.+>)|\<\#.+?\>)(?P<text>.+?)(?P<eol>\[\d+\.\d+\]|~|\n)"
        transcriptions = [x.groupdict() for x in re.finditer(pattern, text)]
        new_transcriptions = []
        for x in transcriptions:
            x["eol"] = cls.parse_and_convert_eol(duration=x["eol"])
            x["size"] = len(transcriptions)
            transcription = Transcription.from_dict(x)
            new_transcriptions.append(transcription)
            logger.debug(f"Parsed {transcription}...")
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
            logger.debug(f"Segment created: {s}")
            new_segments.append(s)
        return new_segments
