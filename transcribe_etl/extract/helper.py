import sqlite3
from pathlib import Path
from typing import Union

import pandas as pd


def get_transcription_metadata(qa_report_db_uri: Union[Path, str], input_metadata_uri: Union[Path, str]) -> pd.DataFrame:
    qa_report_df = get_qa_report_metadata(qa_report_db_uri=qa_report_db_uri)
    input_metadata_df = get_input_metadata(input_metadata_uri=input_metadata_uri)
    metadata_df = pd.merge(left=qa_report_df, right=input_metadata_df, on="directory_name", how="left")
    metadata_df["pin"] = metadata_df["pin"].fillna("unmapped-pin")
    return metadata_df


def get_input_metadata(input_metadata_uri: Union[Path, str]) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer=input_metadata_uri)
    return df


def get_qa_report_metadata(qa_report_db_uri: Union[Path, str]) -> pd.DataFrame:
    con = sqlite3.connect(qa_report_db_uri)
    df = pd.read_sql(
        "SELECT directory_name, corpus_code, file_path, audio_duration, email, user_id, gender, native_language FROM qa_report",
        con=con,
    )
    return df
