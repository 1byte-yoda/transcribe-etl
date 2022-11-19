from dataclasses import dataclass
from typing import Optional, List

from dataclasses_json import DataClassJsonMixin


@dataclass(frozen=True)
class TxData(DataClassJsonMixin):
    speaker_tag: str
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class Speaker:
    user_id: str
    email: str
    gender: str
    native_language: str


@dataclass(frozen=True)
class MetaData(DataClassJsonMixin):
    audio_file_name: str
    audio_duration: float
    corpus_code: Optional[str]
    speaker_id: Speaker


@dataclass(frozen=True)
class TranscriptionPackage:
    tx_data: List[TxData]
    metadata: MetaData
