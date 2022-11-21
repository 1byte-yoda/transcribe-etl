import json
import os
import re
import typing
from datetime import datetime
from pathlib import Path
from typing import List
from dotenv import load_dotenv
import pandas as pd

from transcribe_etl.extract.helper import get_transcription_metadata
from transcribe_etl.transform.model import TxDataGroup, Metadata, Speaker

load_dotenv()

_ROOT_FOLDER = Path(__file__).parent


def load_data_to_s3_bucket(save_folder: Path, file_name: str, data: typing.Union[List[dict], dict]):
    save_folder.mkdir(parents=True, exist_ok=True)

    with open(f"{save_folder}/{file_name}", "w") as f:
        f.seek(0)
        json.dump(data, fp=f)
        f.truncate()


def parse_package_date(filename: str) -> str:
    package_date = re.search(r"(\d{8})", filename).group()
    return datetime.strptime(package_date, "%Y%m%d").strftime("%Y-%m-%d")


def get_metadata_df() -> pd.DataFrame:
    _CLOUD_URI = Path(os.environ.get("CLOUD_URI"))
    _QA_REPORT_DB_URI = os.environ.get("QA_REPORT_DB_URI")
    input_metadata_uri = _CLOUD_URI / "input_metadata" / "input_file.csv"
    metadata_df = get_transcription_metadata(qa_report_db_uri=_QA_REPORT_DB_URI, input_metadata_uri=input_metadata_uri)
    return metadata_df


def lookup_transcript_metadata(extract_files: List[TxDataGroup]) -> pd.DataFrame:
    transcription_df = pd.DataFrame(data=extract_files)
    metadata_df = get_metadata_df()
    transcription_df["package_date"] = transcription_df["file"].apply(parse_package_date)
    transcription_lookup_df = pd.merge(left=transcription_df, right=metadata_df, left_on="file", right_on="file_path", how="left")
    return transcription_lookup_df


def generate_tx_metadata(df_row: typing.Any) -> Metadata:
    return Metadata(
        audio_file_name=df_row.file,
        audio_duration=df_row.audio_duration,
        corpus_code=df_row.corpus_code,
        speaker_id=Speaker(email=df_row.email, gender=df_row.gender, native_language=df_row.native_language),
    )
