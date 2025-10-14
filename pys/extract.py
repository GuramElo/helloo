import sys
import subprocess
import json
from pathlib import Path

def extract_subtitles(mkv_file_path: Path):
    """
    Extracts all subtitle tracks from a given MKV file and saves them as
    separate WebVTT (.vtt) files.

    Args:
        mkv_file_path (Path): The path to the input MKV file.
    """
    if not mkv_file_path.is_file():
        print(f"Error: File not found at '{mkv_file_path}'")
        return

    print(f"Processing file: {mkv_file_path.name}")

    # 1. Use ffprobe to get subtitle stream information in JSON format
    ffprobe_command = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "s",
        "-show_entries", "stream=index,codec_name:stream_tags=language",
        "-of", "json",
        str(mkv_file_path)
    ]

    try:
        result = subprocess.run(ffprobe_command, capture_output=True, text=True, check=True)
        stream_data = json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running ffprobe: {e.stderr}")
        return
    except json.JSONDecodeError:
        print("Error: Could not parse ffprobe output. No subtitle streams found or file is invalid.")
        return

    subtitle_streams = stream_data.get("streams", [])

    if not subtitle_streams:
        print("No subtitle streams found in this file.")
        return

    print(f"Found {len(subtitle_streams)} subtitle stream(s).")

    # 2. Loop through each subtitle stream and extract it with ffmpeg
    for stream in subtitle_streams:
        stream_index = stream['index']
        codec_name = stream.get('codec_name', 'unknown')
        # Get language tag, default to 'und' (undetermined) if not present
        lang_code = stream.get('tags', {}).get('language', 'und')

        # Create a descriptive output filename
        base_filename = mkv_file_path.stem
        output_filename = mkv_file_path.parent / f"{base_filename}_sub_{lang_code}_{stream_index}.vtt"

        print(f"  -> Extracting track {stream_index} ({lang_code}, {codec_name}) -> {output_filename.name}")

        ffmpeg_command = [
            "ffmpeg",
            "-i", str(mkv_file_path),
            "-map", f"0:{stream_index}",
            "-c:s", "webvtt",
            "-y",  # Overwrite output file if it exists
            str(output_filename)
        ]

        # Execute the ffmpeg command
        try:
            # Use DEVNULL to hide ffmpeg's verbose output for a cleaner experience
            subprocess.run(ffmpeg_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            print(f"     Error extracting track {stream_index}: {e}")

    print("\nExtraction complete.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_subs.py <path_to_mkv_file>")
        sys.exit(1)

    # Allow processing multiple files
    for file_arg in sys.argv[1:]:
        input_file = Path(file_arg)
        extract_subtitles(input_file)
        print("-" * 20)