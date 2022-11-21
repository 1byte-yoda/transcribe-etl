import os
import uuid
from pathlib import Path
from typing import List, Any

import pandas as pd
from dotenv import load_dotenv

from transcribe_etl.extract.datasynchronizer import DataSynchronizer
from transcribe_etl.extract.model import StageFolder
from transcribe_etl.load.s3_bucket import load_data_to_s3_bucket, lookup_transcript_metadata, generate_tx_metadata
from transcribe_etl.transform.model import TxDataGroup
from transcribe_etl.transform.text_extract import TextExtractParser, SegmentProcessor

load_dotenv()

_ROOT_FOLDER = Path(__file__).parent.parent


def extract_data(execution_id: uuid.UUID, container_name: str, file_type: str) -> StageFolder:
    cloud_uri = os.environ.get("CLOUD_URI")
    data_syncer = DataSynchronizer(execution_id=execution_id)
    extract_files = data_syncer.sync_files_from_blob(uri=cloud_uri, container_name=container_name, file_type=file_type)
    return StageFolder(extract_files=extract_files)


def transcribe_from_txt(stage_folder: StageFolder) -> List[TxDataGroup]:
    text_annotator = TextExtractParser(segment_processor=SegmentProcessor())
    tx_data_groups = []
    for file in stage_folder.extract_files:
        tx_data_groups.extend(text_annotator.execute(file=file))
    return tx_data_groups


def load_data(data: List[TxDataGroup]):
    s3_bucket_uri = _ROOT_FOLDER / os.environ.get("S3_BUCKET_URI")
    transcription_lookup_df = lookup_transcript_metadata(extract_files=data)
    for _df in transcription_lookup_df.itertuples():  # type: Any
        package_date = s3_bucket_uri / Path(_df.package_date)
        save_path = package_date / "no-pin" if pd.isna(_df.pin) else package_date / str(_df.pin)
        filename = _df.file.strip("/audio-efs/")
        tx_metadata = generate_tx_metadata(df_row=_df).to_dict()
        load_data_to_s3_bucket(save_folder=save_path, file_name=filename.replace(".wav", "_tx.json"), data=_df.tx_data)
        load_data_to_s3_bucket(save_folder=save_path, file_name=filename.replace(".wav", "_meta.json"), data=tx_metadata)


def data_pipeline():
    stg_folder: StageFolder = extract_data(container_name="extract_files", file_type="txt", execution_id=uuid.uuid4())
    tx_data: List[TxDataGroup] = transcribe_from_txt(stage_folder=stg_folder)
    load_data(data=tx_data)
