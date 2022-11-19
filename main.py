from pathlib import Path
import os

import pandas as pd
from loguru import logger
from dotenv import load_dotenv

from transcribe_etl.data_synchronizer.datasynchronizer import DataSynchronizer
from transcribe_etl.data_synchronizer.model import StageFolder
from transcribe_etl.speech_annotator.audio import AudioAnnotator
from transcribe_etl.speech_annotator.text_extract import TextExtractAnnotator


def extract_data() -> StageFolder:
    simulated_cloud_uri = os.environ.get("CLOUD_URI")
    qa_report_db_uri = os.environ.get("QA_REPORT_DB_URI")
    data_synchronizer = DataSynchronizer()
    company_xyz_files = data_synchronizer.sync_files_from_blob(
        uri=simulated_cloud_uri,
        container_name="company_xyz",
        file_type="csv"
    )
    extract_files = data_synchronizer.sync_files_from_blob(
        uri=simulated_cloud_uri,
        container_name="extract_files",
        file_type="txt"
    )
    qa_report_file = data_synchronizer.sync_table_from_sql(
        uri=qa_report_db_uri,
        container_name="qa_report",
        table_name="qa_report"
    )
    return StageFolder(company_xyz=company_xyz_files, extract_files=extract_files, qa_report=[qa_report_file])


def transcribe(stage_folder: StageFolder):
    token = os.environ.get("HUGGING_FACE_TOKEN")
    audio_annotator = AudioAnnotator(token=token)
    text_annotator = TextExtractAnnotator()
    for file in stage_folder.qa_report:
        df = pd.read_csv(file)
        path = df.directory_name + df.file_path
        try:
            tx_data = path.apply(audio_annotator.execute)
        except ValueError as err:
            logger.error(err)

    for file in stage_folder.company_xyz:
        df = pd.read_csv(file)
        audio_dir = df.directory_name.apply(lambda x: Path(x).glob("*.wav"))
        for audio_files in audio_dir:
            for audio_file in audio_files:
                print(audio_annotator.execute(file=audio_file))

    for file in stage_folder.extract_files:
        print(text_annotator.execute(file=file))


if __name__ == '__main__':
    load_dotenv()
    stg_folder = extract_data()
    transcribe(stage_folder=stg_folder)