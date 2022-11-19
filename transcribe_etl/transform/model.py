from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin


@dataclass(frozen=True)
class TxData(DataClassJsonMixin):
    speaker_tag: str
    text: str
    start: int
    end: int
