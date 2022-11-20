import os
from pathlib import Path
from typing import List, Any

import pandas as pd
from dotenv import load_dotenv

from transcribe_etl.extract.datasynchronizer import DataSynchronizer
from transcribe_etl.extract.model import StageFolder
from transcribe_etl.load.s3_bucket import load_data_to_s3_bucket, lookup_transcript_metadata
from transcribe_etl.transform.model import TxDataGroup
from transcribe_etl.transform.text_extract import TextExtractAnnotator, SegmentProcessor

load_dotenv()

_ROOT_FOLDER = Path(__file__).parent
_CLOUD_URI = Path(os.environ.get("CLOUD_URI"))
_QA_REPORT_DB_URI = os.environ.get("QA_REPORT_DB_URI")
_S3_BUCKET_URI = os.environ.get("S3_BUCKET_URI")


def extract_data() -> StageFolder:
    data_syncer = DataSynchronizer()
    extract_files = data_syncer.sync_files_from_blob(uri=_CLOUD_URI, container_name="extract_files", file_type="txt")
    return StageFolder(extract_files=extract_files)


def transcribe(stage_folder: StageFolder) -> List[TxDataGroup]:
    text_annotator = TextExtractAnnotator(segment_processor=SegmentProcessor())
    tx_data_groups = []
    for file in stage_folder.extract_files:
        tx_data_groups.extend(text_annotator.execute(file=file))
    return tx_data_groups


def load(data: List[TxDataGroup]):
    s3_bucket_uri = _ROOT_FOLDER / _S3_BUCKET_URI
    transcription_lookup_df = lookup_transcript_metadata(extract_files=data)
    for _df in transcription_lookup_df.itertuples():  # type: Any
        package_date = s3_bucket_uri / Path(_df.package_date)
        save_path = package_date / "no-pin" if pd.isna(_df.pin) else package_date / str(_df.pin)
        load_data_to_s3_bucket(save_folder=save_path, file_name=_df.file, data=_df.segments)


if __name__ == '__main__':
    stg_folder: StageFolder = extract_data()
    tx_data: List[TxDataGroup] = transcribe(stage_folder=stg_folder)
    load(data=tx_data)
