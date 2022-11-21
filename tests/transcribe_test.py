from pathlib import Path

from transcribe_etl.extract.model import StageFolder
from transcribe_etl.runner import transcribe_from_txt
from transcribe_etl.transform.model import TxDataGroup, TxData

_TEST_DATA_DIR = Path(__file__).parent / "data" / "scenario_txt_files"


def test_transcribe_will_parse_single_segment_transcription():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "single_segment_transcription.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav",
            tx_data=[TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045)],
        )
    ]


def test_transcribe_will_parse_multiple_segment_transcription():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "multiple_segment_transaction.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0006_solo2-17-V-3.wav",
            tx_data=[
                TxData(
                    speaker_tag="<#spk_1>",
                    text="I will now describe the personal interest .",
                    start=1040,
                    end=4575,
                ),
                TxData(
                    speaker_tag="<#spk_2>",
                    text="on the thin side <pause> she as long middle of the back link .",
                    start=4575,
                    end=19020,
                ),
                TxData(
                    speaker_tag="<#spk_1>",
                    text="brunette white hair it is parted on the left side .",
                    start=19020,
                    end=24650,
                ),
            ],
        )
    ]


def test_transcribe_will_not_parse_noise_tags():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_with_noise_tags.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-14.wav",
            tx_data=[
                TxData(
                    speaker_tag="<#spk_1>",
                    text="no I am not busy with office work I am just <unk> <unk> on LinkedIn . I am just looking for some .",
                    start=11130,
                    end=16983,
                ),
                TxData(speaker_tag="<#spk_3>", text="new .", start=16983, end=18395),
                TxData(speaker_tag="<#spk_3>", text="job openings .", start=18395, end=19707),
                TxData(speaker_tag="<#spk_2>", text="<interjection>ohh</interjection> .", start=19707, end=21386),
                TxData(speaker_tag="<#spk_3>", text="yeah .", start=21386, end=22095),
            ],
        )
    ]


def test_transcribe_will_include_pause_tags_in_text_field():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_with_pause_tags.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-D-14.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045),
                TxData(
                    speaker_tag="<#spk_2>",
                    text="<ah> you're s- you seem to be so busy with your office work . ? <pause> <um> are you aware ?",
                    start=5045,
                    end=7105,
                ),
                TxData(
                    speaker_tag="<#spk_2>",
                    text="on the thin side <pause> she as long middle of the back link .",
                    start=7105,
                    end=19020,
                ),
            ],
        )
    ]


def test_transcribe_will_include_continue_tags_in_text_field():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_with_continued_tag.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-17.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="how about you ?", start=36050, end=38966),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="I am there only on LinkedIn and of course WhatsApp <continued>",
                    start=38966,
                    end=42895,
                ),
                TxData(speaker_tag="<#spk_3>", text="which belongs to Facebook again .", start=42895, end=45151),
                TxData(speaker_tag="<#spk_2>", text="yeah yeah .", start=45151, end=46440),
            ],
        )
    ]


def test_transcribe_will_parse_and_combine_sequential_segments_with_tilde():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_with_tilde_at_eol.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_3>", text="<um> not good.", start=5045, end=6446),
                TxData(speaker_tag="", text="<#no-speech>", start=6446, end=8678),
                TxData(
                    speaker_tag="<#spk_2>",
                    text="what happened? I see you get injured.",
                    start=8678,
                    end=11553,
                ),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="I was hit by a truck fifteen days ago .",
                    start=11553,
                    end=14219,
                ),
                TxData(
                    speaker_tag="<#spk_2>",
                    text="oh, it's really terrible. I hope you'll get well soon",
                    start=14219,
                    end=19575,
                ),
            ],
        ),
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0011_solo2-17-V-2.wav",
            tx_data=[
                TxData(
                    speaker_tag="<#spk_1>",
                    text="with what seemed to be a blue pattern skirt . <hm> she was carrying a tan purse .",
                    start=20560,
                    end=24700,
                )
            ],
        ),
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-16.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="hello, how are you", start=45, end=5045),
                TxData(speaker_tag="<#spk_3>", text="<um> not good.", start=5045, end=6446),
                TxData(speaker_tag="", text="<#no-speech>", start=6446, end=8678),
            ],
        ),
    ]


def test_transcribe_will_parse_interval_and_add_each_speaker_duration_and_use_end_interval_for_last_speaker():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segment_that_will_add_each_speaker_duration_to_interval.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-18.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="correct yeah .", start=13345, end=14660),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="people seem to have forgotten about Signal .",
                    start=14660,
                    end=16569,
                ),
                TxData(speaker_tag="<#spk_3>", text="Signal .", start=16569, end=17190),
                TxData(speaker_tag="<#spk_2>", text="yeah but I <er> maybe things have", start=17190, end=20695),
            ],
        )
    ]


def test_transcribe_will_parse_no_speech_tags_and_use_blank_speaker_tag_for_it():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segment_with_the_no_speech_tags.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0002_20220605-192230_0038_solo2-17-A-1.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_3>", text="<um> not good.", start=5045, end=6446),
                TxData(speaker_tag="", text="<#no-speech>", start=6446, end=8678),
                TxData(speaker_tag="<#spk_2>", text="what happened? I see", start=8678, end=9035),
                TxData(speaker_tag="<#spk_3>", text="uhmmm", start=9035, end=10436),
                TxData(speaker_tag="", text="<#no-speech>", start=10436, end=12668),
            ],
        )
    ]


def test_transcribe_is_aggregated_based_on_filename():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_aggregated_based_on_filename.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    group_size = len(tx_data_groups)
    assert group_size == 3
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-15.wav",
            tx_data=[
                TxData(
                    speaker_tag="<#spk_1>",
                    text="no I am not busy with office work . I am just <unk> <unk> on LinkedIn .",
                    start=0,
                    end=2100,
                ),
                TxData(speaker_tag="<#spk_2>", text="I am just looking for some .", start=2100, end=16050),
                TxData(speaker_tag="<#spk_2>", text="no I am not busy with office work", start=16050, end=17171),
                TxData(speaker_tag="<#spk_1>", text="I am just <unk> <unk> on LinkedIn .", start=17171, end=18150),
                TxData(speaker_tag="<#spk_2>", text="I am just looking for some .", start=18150, end=21315),
            ],
        ),
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-17.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="how about you ?", start=36050, end=38966),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="I am there only on LinkedIn and of course WhatsApp <continued>",
                    start=38966,
                    end=42895,
                ),
                TxData(speaker_tag="<#spk_3>", text="which belongs to Facebook again .", start=42895, end=45151),
                TxData(speaker_tag="<#spk_2>", text="yeah yeah .", start=45151, end=46440),
            ],
        ),
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-18.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="you know migrate to Signal .", start=4530, end=6069),
                TxData(speaker_tag="<#spk_2>", text="at one .", start=6069, end=6624),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="point of time that thing spiked and now I think it mi- it looks like as though it died down .",
                    start=6624,
                    end=13345,
                ),
                TxData(speaker_tag="<#spk_2>", text="correct yeah .", start=13345, end=14660),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="people seem to have forgotten about Signal .",
                    start=14660,
                    end=16569,
                ),
                TxData(speaker_tag="<#spk_3>", text="Signal .", start=16569, end=17190),
                TxData(speaker_tag="<#spk_2>", text="yeah but I <er> maybe things have", start=17190, end=20695),
            ],
        ),
    ]


def test_transcribe_can_parse_text_with_optional_hyperparameter_field():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_with_missing_hyperparameter_field.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-18.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="correct yeah .", start=13345, end=14660),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="people seem to have forgotten about Signal .",
                    start=14660,
                    end=16569,
                ),
                TxData(speaker_tag="<#spk_3>", text="Signal .", start=16569, end=17190),
                TxData(speaker_tag="<#spk_2>", text="yeah but I <er> maybe things have", start=17190, end=20695),
            ],
        )
    ]


def test_transcribe_can_parse_text_with_optional_labels_field():
    stage_folder = StageFolder(extract_files=[Path(_TEST_DATA_DIR) / "segments_with_missing_labels_field.txt"])
    tx_data_groups = transcribe_from_txt(stage_folder=stage_folder)
    assert tx_data_groups == [
        TxDataGroup(
            file="/audio-efs/Test_04803_MUL_MUL_0006_20220605-181850_0019_solo2-D-18.wav",
            tx_data=[
                TxData(speaker_tag="<#spk_2>", text="correct yeah .", start=13345, end=14660),
                TxData(
                    speaker_tag="<#spk_3>",
                    text="people seem to have forgotten about Signal .",
                    start=14660,
                    end=16569,
                ),
                TxData(speaker_tag="<#spk_3>", text="Signal .", start=16569, end=17190),
                TxData(speaker_tag="<#spk_2>", text="yeah but I <er> maybe things have", start=17190, end=20695),
            ],
        )
    ]
