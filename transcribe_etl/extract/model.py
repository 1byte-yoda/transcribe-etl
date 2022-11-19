from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class StageFolder:
    company_xyz: List[Path]
    extract_files: List[Path]
    qa_report: List[Path]

