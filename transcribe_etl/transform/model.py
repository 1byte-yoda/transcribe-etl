from dataclasses import dataclass
from typing import Optional, List, Union

from dataclasses_json import DataClassJsonMixin


@dataclass(frozen=True)
class TxData(DataClassJsonMixin):
    speaker_tag: str
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class Speaker:
    email: str
    gender: str
    native_language: str


@dataclass(frozen=True)
class Metadata(DataClassJsonMixin):
    audio_file_name: str
    audio_duration: float
    corpus_code: str
    speaker_id: Speaker


@dataclass(frozen=True)
class ExtractedTranscription(DataClassJsonMixin):
    file: str
    interval: str
    transcription: str
    user: str
    hypothesis: Optional[str] = None
    labels: Optional[str] = None


@dataclass(frozen=True)
class Transcription(DataClassJsonMixin):
    speaker_tag: str
    text: str
    eol: str
    size: int


@dataclass(frozen=True)
class Segment(DataClassJsonMixin):
    file: str
    speaker_tag: str
    text: str
    eol: Union[str, int]
    size: int
    start: int
    end: int


@dataclass(frozen=True)
class TxDataGroup(DataClassJsonMixin):
    file: str
    segments: List[Segment]
