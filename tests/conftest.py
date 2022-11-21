import uuid
from pathlib import Path
from unittest import mock

import pytest


@pytest.fixture(scope="function", autouse=True)
def default_session_fixture(request, tmp_path):
    staging_dir_patch = mock.patch("transcribe_etl.extract.datasynchronizer._IMAGINARY_STAGING_URI", Path(tmp_path / "stage"))
    load_root_dir_patch = mock.patch("transcribe_etl.load.s3_bucket._ROOT_FOLDER", tmp_path)
    runner_root_dir_patch = mock.patch("transcribe_etl.runner._ROOT_FOLDER", tmp_path)

    staging_dir_patch.start()
    load_root_dir_patch.start()
    runner_root_dir_patch.start()

    def unpatch():
        staging_dir_patch.stop()
        load_root_dir_patch.stop()
        runner_root_dir_patch.stop()

    request.addfinalizer(unpatch)


@pytest.fixture(scope="session", autouse=True)
def execution_id():
    return uuid.UUID("f0c23045-978f-4cc6-a1c1-160586e56e8e")
