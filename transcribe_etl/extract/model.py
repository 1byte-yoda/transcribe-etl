from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class StageFolder:
    extract_files: List[Path]
