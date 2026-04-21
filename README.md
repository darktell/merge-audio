# merge-audio

A simple Python tool for batch merging external audio tracks into video files using `ffmpeg`.

It scans a folder, finds matching video/audio pairs, and muxes them without re-encoding the video.  
The script can match files by:

- exact file name
- detected episode number patterns such as `S01E01`, `E01`, `- 01`, or `[01]`

## Features

- Batch merge video and audio files in one folder
- No video re-encoding
- Supports common video and audio formats
- Automatic pairing by file name or episode number
- Optional dry-run mode to preview matches
- Serial or parallel processing
- Optional GUI folder picker
- Windows launcher included

## Requirements

- Python 3
- `ffmpeg`
- `ffprobe`  
- `tkinter` for the folder picker GUI, if you want to use it

## Installation

1. Install Python 3.
2. Install `ffmpeg` and make sure it is available on your `PATH`.
3. Clone this repository.

## Usage

### Run with the folder picker
- `bash python merge_audio.py`
### Run with a specific folder
- `bash python merge_audio.py /path/to/folder`
### Use multiple parallel jobs
- `bash python merge_audio.py /path/to/folder -j 4`
### Preview matches without writing files
- `bash python merge_audio.py /path/to/folder --dry-run`

## Output

By default, merged files are saved into a `merged` subfolder inside the selected directory.
Example output file:
- `video_name.mkv`

## Command-line options

- `-o, --output` — set a custom output directory
- `-j, --jobs` — number of parallel merges
- `--keep-original` — keep the original audio track(s) from the video
- `--audio-codec` — set the audio codec, for example `copy` or `aac`
- `--ext` — set the output container extension, default is `mkv`
- `--dry-run` — show detected files and pairings without running `ffmpeg`

## Windows launcher

If you are on Windows, you can use `run.bat` to start the script quickly.

## Notes

- For best performance, use `-j 1` on HDD drives.
- On SSD drives, `-j 2` to `-j 4` may work well.
- If a folder contains ambiguous matches, the script will skip them and show a warning.

## License
The MIT License (MIT)