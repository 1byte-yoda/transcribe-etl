import os.path
from pathlib import Path
from typing import Union, Optional, List, Tuple

import whisper
from loguru import logger
from pyannote.audio import Audio, Pipeline
from pyannote.core import Segment
from whisper import Whisper

from transcribe_etl.transform.helper import split_interval, convert_duration_to_millisecond
from transcribe_etl.transform.base import Processor
from transcribe_etl.transform.model import TxData


class AudioAnnotator(Processor):
    def __init__(self, token: str, verbose: Optional[bool] = False):
        # TODO: NEED TO GET A TOKEN and access to speaker-diarization and segmentation
        #  https://huggingface.co/pyannote/speaker-diarization
        #  https://huggingface.co/pyannote/segmentation
        super().__init__(verbose=verbose)
        self._token = token
        if not self.verbose:
            logger.disable("transform.audio")

    def execute(self, file: Union[str, Path]) -> List[TxData]:
        logger.debug("Sample only AudioAnnotator")

        if not os.path.exists(file):
            logger.error(f"File {file} does not exists")
            return []

        audio = Audio(sample_rate=16000, mono=True)
        speech_model = self._prepare_whisper_speech_recognition()
        diarization = self._prepare_pyannote_diarization(file=file)

        annotated_audios = []

        for segment, _, speaker in diarization.itertracks(yield_label=True):
            waveform, sample_rate = audio.crop(file=file, segment=segment)
            text = speech_model.transcribe(waveform.squeeze().numpy())["text"]
            start, end = self._parse_interval_ms(segment=segment)
            tx_data = TxData(speaker_tag=f"<#{speaker}>", text=text, start=start, end=end)
            annotated_audios.append(tx_data)

        return annotated_audios

    @staticmethod
    def _prepare_whisper_speech_recognition(name: Optional[str] = "small") -> Whisper:
        return whisper.load_model(name=name)

    def _prepare_pyannote_diarization(self, file: Union[str, Path]) -> Pipeline:
        if self._token is None:
            raise Exception(
                "Token not found! You need to provide the token to use the diarization model. " "Don't have one yet? Create a new one here: https://huggingface.co/settings/tokens"
            )
        pipeline = Pipeline.from_pretrained(checkpoint_path="pyannote/speaker-diarization", use_auth_token=self._token)
        return pipeline(file)

    @staticmethod
    def _parse_interval_ms(segment: Segment) -> Tuple[int, int]:
        interval = str(segment).replace(r" --> ", "").strip(r"(\[\] )")
        duration = split_interval(interval=interval)
        start = convert_duration_to_millisecond(duration=duration[0])
        end = convert_duration_to_millisecond(duration=duration[1])
        return start, end


def annotate_multiple_audio_files(audio_files: List[Path], audio_annotator: AudioAnnotator):
    return [tx for file in audio_files for tx in audio_annotator.execute(file=file)]
