import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List
from dotenv import load_dotenv
import pandas as pd

from transcribe_etl.extract.helper import get_transcription_metadata
from transcribe_etl.transform.model import TxDataGroup

load_dotenv()

_ROOT_FOLDER = Path(__file__).parent
_CLOUD_URI = Path(os.environ.get("CLOUD_URI"))
_QA_REPORT_DB_URI = os.environ.get("QA_REPORT_DB_URI")


def load_data_to_s3_bucket(save_folder: Path, file_name: str, data: List[dict]):
    save_folder.mkdir(parents=True, exist_ok=True)
    filename = file_name.strip('/audio-efs/').replace('.wav', '.json')

    with open(f"{save_folder}/{filename}", "w") as f:
        f.seek(0)
        json.dump(data, fp=f)
        f.truncate()


def parse_package_date(filename: str) -> str:
    package_date = re.search(r"(\d{8})", filename).group()
    return datetime.strptime(package_date, "%Y%m%d").strftime("%Y-%m-%d")


def get_metadata_df() -> pd.DataFrame:
    input_metadata_uri = _CLOUD_URI / "input_metadata" / "input_file.csv"
    metadata_df = get_transcription_metadata(qa_report_db_uri=_QA_REPORT_DB_URI, input_metadata_uri=input_metadata_uri)
    return metadata_df


def lookup_transcript_metadata(extract_files: List[TxDataGroup]) -> pd.DataFrame:
    transcription_df = pd.DataFrame(data=extract_files)
    metadata_df = get_metadata_df()
    transcription_df["package_date"] = transcription_df["file"].apply(parse_package_date)
    transcription_lookup_df = pd.merge(
        left=transcription_df, right=metadata_df, left_on="file", right_on="file_path", how="left"
    )
    return transcription_lookup_df
