import shutil
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, List

import pandas as pd

_IMAGINARY_STAGING_URI = Path(__file__).parent.parent.parent / "stage"


class DataSynchronizer:
    def __init__(self, execution_id: Optional[uuid.UUID] = uuid.uuid4()):
        self.execution_id = execution_id
        _now = datetime.now()
        self._package_hierarchy = f"{_now.year}{_now.month}{_now.day}{_now.hour}-{execution_id}"

    def sync_files_from_blob(self, uri: Union[str, Path], container_name: str, file_type: str) -> List[Path]:
        files = Path(Path(uri) / container_name).glob(f"*.{file_type}")
        destination_folder = _IMAGINARY_STAGING_URI / f"{self._package_hierarchy}" / container_name
        destination_folder.mkdir(parents=True, exist_ok=True)
        sync_files = []

        for f in files:
            file_destination = destination_folder / f.name
            sync_files.append(file_destination)
            shutil.copy(f, file_destination)

        return sync_files

    def sync_table_from_sql(self, uri: str, container_name: str, table_name: str):
        con = sqlite3.connect(uri)
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=con)
        destination_folder = _IMAGINARY_STAGING_URI / self._package_hierarchy / container_name
        destination_folder.mkdir(parents=True, exist_ok=True)
        file_destination = f"{destination_folder / table_name}.csv"
        df.to_csv(file_destination, index=False)
        return file_destination
