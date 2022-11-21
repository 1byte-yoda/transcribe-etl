import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, List

from loguru import logger

_IMAGINARY_STAGING_URI = Path(__file__).parent.parent.parent / "stage"


class DataSynchronizer:
    def __init__(self, execution_id: Optional[uuid.UUID] = uuid.uuid4()):
        self.execution_id = execution_id
        _now = datetime.now()
        self._package_hierarchy = f"{_now.year}{_now.month}{_now.day}{_now.hour}-{execution_id}"

    def sync_files_from_blob(self, uri: Union[str, Path], container_name: str, file_type: str) -> List[Path]:
        logger.info(f"Synchronizing {file_type} files from {uri}/{container_name} store.")
        files = Path(Path(uri) / container_name).glob(f"*.{file_type}")
        destination_folder = _IMAGINARY_STAGING_URI / f"{self._package_hierarchy}" / container_name
        destination_folder.mkdir(parents=True, exist_ok=True)
        sync_files = []

        for f in files:
            file_destination = destination_folder / f.name
            logger.debug(f"Copying {f.name} to {file_destination}")

            sync_files.append(file_destination)
            shutil.copy(f, file_destination)

        logger.success(f"Synchronization of {len(sync_files)} files Finished.")

        return sync_files
