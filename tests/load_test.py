import json
import os
from pathlib import Path
from unittest import mock

from transcribe_etl.runner import load_data
from transcribe_etl.transform.model import TxDataGroup, TxData


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
@mock.patch.dict(os.environ, {"S3_BUCKET_URI": "s3_bucket_test"})
@mock.patch.dict(os.environ, {"QA_REPORT_DB_URI": str(Path(__file__).parent / "data" / "qa_report_test.db")})
def test_load_data_will_store_both_metadata_and_tx_json_into_s3_bucket(tmp_path):
    tx_data = [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav", tx_data=[TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045)]
        )
    ]
    tx_data_folder = tmp_path / "s3_bucket_test" / "2022-06-05" / "P998123"
    tx_json = tx_data_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1_tx.json"
    metadata_json = tx_data_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1_meta.json"

    assert not tx_json.exists()
    assert not metadata_json.exists()

    load_data(data=tx_data)

    assert tx_json.exists()
    assert metadata_json.exists()


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
@mock.patch.dict(os.environ, {"S3_BUCKET_URI": "s3_bucket_test"})
@mock.patch.dict(os.environ, {"QA_REPORT_DB_URI": str(Path(__file__).parent / "data" / "qa_report_test.db")})
def test_load_data_will_generate_tx_json_from_tx_data_groups(tmp_path):
    tx_data = [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav", tx_data=[TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045)]
        )
    ]

    tx_data_folder = tmp_path / "s3_bucket_test" / "2022-06-05" / "P998123"
    tx_json = tx_data_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1_tx.json"

    load_data(data=tx_data)

    expected_tx_data = [{"speaker_tag": "<#spk_2>", "text": "hello, how are you", "start": 45, "end": 5045}]
    with open(tx_json, "r") as f:
        assert json.loads(f.read()) == expected_tx_data


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
@mock.patch.dict(os.environ, {"S3_BUCKET_URI": "s3_bucket_test"})
@mock.patch.dict(os.environ, {"QA_REPORT_DB_URI": str(Path(__file__).parent / "data" / "qa_report_test.db")})
def test_load_data_will_generate_metadata_from_sql_database_and_input_csv_file(tmp_path):
    tx_data = [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav", tx_data=[TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045)]
        )
    ]

    tx_package_folder = tmp_path / "s3_bucket_test" / "2022-06-05" / "P998123"
    meta_json = tx_package_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1_meta.json"

    load_data(data=tx_data)

    expected_tx_data = {
        "audio_duration": 19.12,
        "audio_file_name": "/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav",
        "corpus_code": "solo2-17-A-1",
        "speaker_id": {"email": "xxx123xxx@hotmail.com", "gender": "FEMALE", "native_language": "Franch"},
    }
    with open(meta_json, "r") as f:
        assert json.loads(f.read()) == expected_tx_data


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
@mock.patch.dict(os.environ, {"S3_BUCKET_URI": "s3_bucket_test"})
@mock.patch.dict(os.environ, {"QA_REPORT_DB_URI": str(Path(__file__).parent / "data" / "qa_report_test.db")})
def test_load_data_will_store_all_json_into_no_pin_folder_if_pin_not_found_in_metadata_storage(tmp_path):
    tx_data = [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_TEST_NOT_IN_DB.wav", tx_data=[TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045)]
        )
    ]

    tx_package_folder = tmp_path / "s3_bucket_test" / "2022-06-05" / "no-pin"
    tx_json = tx_package_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_TEST_NOT_IN_DB_tx.json"
    meta_json = tx_package_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_TEST_NOT_IN_DB_meta.json"

    assert not tx_json.exists()
    assert not meta_json.exists()

    load_data(data=tx_data)

    assert tx_json.exists()
    assert meta_json.exists()


@mock.patch.dict(os.environ, {"CLOUD_URI": str(Path(__file__).parent / "data")})
@mock.patch.dict(os.environ, {"S3_BUCKET_URI": "s3_bucket_test"})
@mock.patch.dict(os.environ, {"QA_REPORT_DB_URI": str(Path(__file__).parent / "data" / "qa_report_test.db")})
def test_load_data_will_use_the_audio_filename_to_store_it_into_json(tmp_path):
    tx_data = [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav", tx_data=[TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045)]
        )
    ]

    tx_package_folder = tmp_path / "s3_bucket_test" / "2022-06-05" / "P998123"
    incorrect_audio_filename = tx_package_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_TEST_INCORRECT_AUDIO_FILENAME_tx.json"
    correct_audio_filename = tx_package_folder / "Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1_tx.json"

    assert not incorrect_audio_filename.exists()
    assert not correct_audio_filename.exists()

    load_data(data=tx_data)

    assert not incorrect_audio_filename.exists()
    assert correct_audio_filename.exists()
