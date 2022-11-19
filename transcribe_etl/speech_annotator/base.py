from abc import ABC
from pathlib import Path
from typing import List, Optional, Union

from transcribe_etl.speech_annotator.model import TxData


class Processor(ABC):
    def __init__(self, verbose: Optional[bool] = False):
        self.verbose = verbose

    def execute(self, file: Union[str, Path]) -> List[TxData]:
        raise NotImplementedError
