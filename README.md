# PE Preliminary Examination

## Transcription Package

## 1. Brief

>  It's a real package in the recent stuff. Hope you can make it in 3 hours. 

We need to generate a package which meets such below requirements. And each source could be updated in any reason in any time. And it should be made by Python3 script.

## 2. Sources

1. Table in the sqlite (qa_report.db)
   1. Table Name: qa_report
   2. Headers: directory_name, corpus_code, file_path, audio_duration, email, user_id, gender, native_language
   3. Primary Key: directory_name
2. Extract file (extract.txt)
   1. Each file has several utterances
   2. Each utterance has the same format
   3. You have to parse extract file by yourself. 
3. Input file (input_file.csv)
   1. file_name: input_file.csv
   2. Headers: directory_name, pin
   3. Primary Key: directory_name



## 3. Package Folder Structure

```
====== structure =========
-- {package_date}
        |-- {pin}
        			|-- <file_name>_meta.json    >>>>> meta data files
        			|-- <file_name>_tx.json    >>>>> transcription data files

======  example  =========
-- 2022-01-01
        |-- 777777_01
               |-- Exam_04803_MUL_MUL_0001_20220101-192230_0036_s1_meta.json
               |-- Exam_04803_MUL_MUL_0001_20220101-192230_0036_s1_tx.json
               |-- Exam_04803_MUL_MUL_0001_20220101-192230_0038_s2_meta.json
               |-- Exam_04803_MUL_MUL_0001_20220101-192230_0038_s2_tx.json
        |-- 888888_01
               |-- Exam_04803_MUL_MUL_0002_20220101-145630_0036_s1_meta.json
               |-- Exam_04803_MUL_MUL_0002_20220101-145630_0036_s1_tx.json
               |-- Exam_04803_MUL_MUL_0002_20220101-145630_0038_s2_meta.json
               |-- Exam_04803_MUL_MUL_0002_20220101-145630_0038_s2_tx.json
```

### 3.1 Meta Data Json files

```json
--- json ---
{
	"audio_file_name": str,
	"audio_duration": float,
	"corpus_code": str,
    'speaker_id': {
  		"email": str,
  		"gender": str,
  		"native_language": str,
  }
}
```

- "speaker_id": user_id from input fie, column "user_id"
- "gender": gender from qareport, column "gender"
- "collection_language": native_language  from qa_report, column "native_language"
- "audio_duration": from qa_report,  column "audio_duration"
- "audio_file_name": full audio filename from column "file_path" (e.g.: Exam_04803_MUL_MUL_0001_20220101-192230_0038_s2.wav)



### 3.2 TX Data Json files

```json
--- json ---
[
	{
		"speaker_tag": str,
		"text": str,
		"start": int, 
		"end": int
	}
]
```

- "speaker_tag": the speaker tag in extract transcription content. (e.g.: spk_2)
-  "text": the whole extract transcription content.
- "start": the start time of current segment. (millisecond)
- "end": the end time of current segment. (millisecond)

#### TimeStamps

>  Some important things:
>
>  - Manual timestamps will now only appear in [square brackets] only,  no other information (e.g [4.444] , second)
>  - Script must segment an utterance at each time there is a timestamp to create a new segment
>  - Intervals (second) also form part of time marking. 
>  - Speaker tags only appear in the begin of the segment.
>  - If the text only contains <#no-speech> please keep empty speaker_tag.
>
>  Definition of Segment:
>
>  - The start of a segment will always be defined by: 
>    - The beginning of the utterance (unless the previous one does not end with the ~ marker)
>    - A timestamp
>  - Merging utterances
>    - if the preceding utterance ends with the ~ marker, merge the two utterances
>    - The end of a segment will always be defined by:
>   - The end of an utterance (unless it ends with the ~ marker)
>     - A timestamp



**You have to handle Tilde. **

**The Tilde indicates joining utterances, this will affect manual timestamps and interval timestamps.**

**You have to handle TimeStamp.**

**The TimeStamp means we need to split current utterance into two different segments in TX data.** 



#### Example

> ```
> FILE: /audio-efs/0001.WAV
> INTERVAL: 00:00:00.045 00:00:5.045
> TRANSCRIPTION: <#spk_2> hello, how are you
> HYPOTHESIS: 
> LABELS: 
> USER: User41
> 
> FILE: /audio-efs/0001.WAV
> INTERVAL: 00:00:5.045 00:00:9.035
> TRANSCRIPTION: <#spk_3> <um> not good. [1.401] <#no-speech> [3.633] <#spk_2> what happened? I see ~
> HYPOTHESIS: 
> LABELS: 
> USER: User41
> 
> FILE: /audio-efs/0001.WAV
> INTERVAL: 00:00:9.035 00:00:19.575
> TRANSCRIPTION: you get injured. [2.518] <#spk_3> I was hit by a truck fifteen days ago . [5.184] <#spk_2> oh, it's really terrible. I hope you'll get well soon
> HYPOTHESIS: 
> LABELS: 
> USER: User41
> ```

output:

```json
[
  {
    "speaker_tag": "spk_2",
		"text": "hello, how are you",
		"start": 45, 
		"end": 5045
  },
  {
    "speaker_tag": "spk_3",
		"text": "<um> not good.",
		"start": 5045, 
		"end": 6446
  },
  {
    "speaker_tag": "",
		"text": "<#no-speech>",
		"start": 6446, 
		"end": 8678
  },
  {
    "speaker_tag": "spk_2",
		"text": "what happened? I see you get injured.",
		"start": 8678, 
		"end": 11553
  },
  {
    "speaker_tag": "spk_3",
		"text": "I was hit by a truck fifteen days ago .",
		"start": 11553, 
		"end": 14219
  },
  {
    "speaker_tag": "spk_2",
		"text": "oh, it's really terrible. I hope you'll get well soon",
		"start": 14219, 
		"end": 19575
  },
]
```

