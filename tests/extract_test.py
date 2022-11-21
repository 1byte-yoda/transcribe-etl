import os
import uuid
from unittest import mock
from pathlib import Path

import pytest

from transcribe_etl.runner import extract_data


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
@pytest.mark.parametrize("file_type,exist,total_count", [("txt", True, 1), ("csv", False, 0), ("parquet", True, 1)])
def test_extract_will_find_file_based_on_extension(file_type: str, exist: bool, execution_id: uuid.UUID, total_count: int):
    staging_folder = extract_data(container_name="extract_files", file_type=file_type, execution_id=execution_id)
    assert len(staging_folder.extract_files) == total_count
    assert all([f.exists() == exist for f in staging_folder.extract_files])


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
def test_extract_will_sync_data_from_cloud_container_to_staging(execution_id: uuid.UUID):
    staging_folder = extract_data(container_name="extract_files", file_type="txt", execution_id=execution_id)
    assert all([f.exists() for f in staging_folder.extract_files])
