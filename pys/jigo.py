#!/usr/bin/env python3
"""
HLS Video Converter with Proper Multi-Audio and Subtitle Support
Converts MKV/MP4 files to HLS format with multiple quality variants,
separate audio tracks, and subtitle support.

CORRECTED VERSION 2: Uses the 'segment' muxer for robust subtitle conversion.
"""

import os
import sys
import json
import subprocess
import argparse
from pathlib import Path
from typing import Dict, List, Tuple
import re

class HLSConverter:
    def __init__(self, input_file: str, output_dir: str):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Quality profiles for video
        self.quality_profiles = {
            'high': {
                'name': 'high',
                'height': 1080,
                'video_bitrate': '5000k',
                'maxrate': '5350k',
                'bufsize': '7500k',
                'crf': '19',
            },
            # 'medium': {
            #     'name': 'medium',
            #     'height': 720,
            #     'video_bitrate': '2800k',
            #     'maxrate': '2996k',
            #     'bufsize': '4200k',
            #     'crf': '21',
            # },
            # 'low': {
            #     'name': 'low',
            #     'height': 480,
            #     'video_bitrate': '1400k',
            #     'maxrate': '1498k',
            #     'bufsize': '2100k',
            #     'crf': '23',
            # }
        }

        # Audio profiles
        self.audio_profiles = {
            'high': {'bitrate': '192k', 'sample_rate': '48000'},
            # 'medium': {'bitrate': '128k', 'sample_rate': '48000'},
            # 'low': {'bitrate': '96k', 'sample_rate': '48000'},
        }

        self.video_info = None
        self.audio_streams = []
        self.subtitle_streams = []

    def check_ffmpeg(self) -> bool:
        """Check if ffmpeg and ffprobe are available"""
        try:
            subprocess.run(['ffmpeg', '-version'],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     check=True)
            subprocess.run(['ffprobe', '-version'],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("ERROR: ffmpeg and ffprobe must be installed and in PATH")
            return False

    def probe_file(self) -> bool:
        """Probe input file to get stream information"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                self.input_file
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)

            if 'streams' not in data:
                print("ERROR: No streams found in input file")
                return False

            print(f"\n{'='*70}")
            print(f"🔍 Analyzing file: {os.path.basename(self.input_file)}")
            print(f"{'='*70}")
            print(f"Total streams found: {len(data['streams'])}")

            # Extract stream information
            for stream in data['streams']:
                codec_type = stream.get('codec_type', '').lower()
                stream_index = stream.get('index', -1)
                codec_name = stream.get('codec_name', 'unknown')

                print(f"\nStream #{stream_index}: type={codec_type}, codec={codec_name}")

                if codec_type == 'video' and not self.video_info:
                    self.video_info = {
                        'index': stream['index'],
                        'codec': stream.get('codec_name'),
                        'width': stream.get('width'),
                        'height': stream.get('height'),
                        'fps': self._parse_fps(stream.get('r_frame_rate', '25/1')),
                        'bitrate': stream.get('bit_rate', 'N/A'),
                        'pix_fmt': stream.get('pix_fmt', 'yuv420p')
                    }
                    print(f"   ✓ Video stream detected")

                elif codec_type == 'audio':
                    lang = stream.get('tags', {}).get('language', 'und')
                    title = stream.get('tags', {}).get('title', '')

                    # Create a better default title
                    if not title:
                        if lang != 'und':
                            title = f"{lang.upper()}"
                        else:
                            title = f"Audio {len(self.audio_streams) + 1}"

                    self.audio_streams.append({
                        'index': stream['index'],
                        'codec': stream.get('codec_name'),
                        'channels': stream.get('channels', 2),
                        'sample_rate': stream.get('sample_rate', '48000'),
                        'language': lang,
                        'title': title,
                        'bitrate': stream.get('bit_rate', 'N/A')
                    })
                    print(f"   ✓ Audio stream detected: {title} ({lang})")

                elif codec_type == 'subtitle':
                    lang = stream.get('tags', {}).get('language', 'und')
                    title = stream.get('tags', {}).get('title', '')

                    # Create a better default title
                    if not title:
                        if lang != 'und':
                            title = f"{lang.upper()}"
                        else:
                            title = f"Subtitle {len(self.subtitle_streams) + 1}"

                    self.subtitle_streams.append({
                        'index': stream['index'],
                        'codec': stream.get('codec_name'),
                        'language': lang,
                        'title': title
                    })
                    print(f"   ✓ Subtitle stream detected: {title} ({lang}) - codec: {codec_name}")

            if not self.video_info:
                print("\n❌ ERROR: No video stream found in input file")
                return False

            print(f"\n{'='*70}")
            print(f"📹 Video: {self.video_info['codec']} "
                  f"{self.video_info['width']}x{self.video_info['height']} "
                  f"@ {self.video_info['fps']} fps")
            print(f"{'='*70}")

            if self.audio_streams:
                print(f"\n🔊 Found {len(self.audio_streams)} audio stream(s):")
                for i, audio in enumerate(self.audio_streams):
                    print(f"   [{i}] {audio['title']:30} | Lang: {audio['language']:5} | "
                          f"Codec: {audio['codec']:8} | Channels: {audio['channels']}")
            else:
                print(f"\n⚠️  No audio streams found")

            if self.subtitle_streams:
                print(f"\n💬 Found {len(self.subtitle_streams)} subtitle stream(s):")
                for i, sub in enumerate(self.subtitle_streams):
                    print(f"   [{i}] {sub['title']:30} | Lang: {sub['language']:5} | "
                          f"Codec: {sub['codec']}")
            else:
                print(f"\n⚠️  No subtitle streams found")

            print(f"\n{'='*70}\n")

            return True

        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to probe file: {e}")
            return False
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse ffprobe output: {e}")
            return False

    def _parse_fps(self, fps_string: str) -> float:
        """Parse fps from fraction string"""
        try:
            if '/' in fps_string:
                num, den = fps_string.split('/')
                return round(float(num) / float(den), 3)
            return float(fps_string)
        except:
            return 25.0

    def _calculate_scale(self, target_height: int) -> Tuple[str, int, int]:
        """Calculate proper scaling maintaining aspect ratio"""
        source_height = self.video_info['height']
        source_width = self.video_info['width']

        # Don't upscale
        if target_height >= source_height:
            target_height = source_height
            target_width = source_width
        else:
            # Calculate width maintaining aspect ratio
            aspect_ratio = source_width / source_height
            target_width = int(target_height * aspect_ratio)

        # Make sure dimensions are even (required for h264)
        target_width = target_width if target_width % 2 == 0 else target_width - 1
        target_height = target_height if target_height % 2 == 0 else target_height - 1

        return f"{target_width}:{target_height}", target_width, target_height

    def convert_subtitles(self):
        """Convert subtitles to segmented WebVTT format using the segment muxer."""
        print("\n📝 Converting subtitles to HLS (Segmented WebVTT)...")

        if not self.subtitle_streams:
            print("   ⚠️  No subtitle streams detected to convert")
            return

        image_based_codecs = ['hdmv_pgs_subtitle', 'dvd_subtitle', 'dvdsub', 'pgssub', 'pgs']
        converted_count = 0
        skipped_count = 0

        for i, subtitle in enumerate(self.subtitle_streams):
            safe_lang = re.sub(r'[^\w\-]', '_', subtitle['language'])
            
            output_playlist = self.output_dir / f"subtitle_{i}_{safe_lang}.m3u8"
            segment_filename_pattern = self.output_dir / f"subtitle_{i}_{safe_lang}_%04d.vtt"
            codec = subtitle['codec'].lower()

            print(f"\n   [{i}] Processing: {subtitle['title']} ({subtitle['language']})")
            print(f"       Codec: {codec}")

            if codec in image_based_codecs:
                print(f"       ⚠️  Image-based subtitle format detected. Skipping.")
                skipped_count += 1
                continue

            # This command uses the 'segment' muxer which is the correct tool for this job.
            cmd = [
                'ffmpeg',
                '-v', 'warning',
                '-i', self.input_file,
                '-map', f"0:{subtitle['index']}",
                '-c:s', 'webvtt',                  # Ensure output codec is WebVTT
                '-f', 'segment',                   # Use the segment muxer
                '-segment_time', '10',             # Create 10-second segments
                '-segment_list', str(output_playlist), # The output M3U8 playlist
                '-segment_list_type', 'm3u8',      # The type of playlist to generate
                '-y',
                str(segment_filename_pattern)      # The pattern for the segment files
            ]

            try:
                subprocess.run(cmd,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               check=True,
                               timeout=180)

                if output_playlist.exists() and output_playlist.stat().st_size > 50:
                    print(f"       ✅ Successfully converted to HLS format.")
                    converted_count += 1
                else:
                    print(f"       ❌ Conversion failed: output playlist is empty.")
                    skipped_count += 1

            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                error_msg = e.stderr.decode('utf-8', errors='ignore') if hasattr(e, 'stderr') else str(e)
                print(f"       ❌ Conversion failed for subtitle stream #{i}.")
                print(f"          Error: {error_msg[:250]}")
                skipped_count += 1
        
        print(f"\n   {'='*66}")
        print(f"   📊 Subtitle Conversion Summary:")
        print(f"      ✅ Converted: {converted_count}")
        print(f"      ❌ Skipped:   {skipped_count}")
        print(f"   {'='*66}")


    def convert_audio_track(self, audio_index: int, audio_stream: Dict, quality: str) -> bool:
        """Convert a single audio track to AAC for a specific quality"""
        profile = self.audio_profiles[quality]
        safe_lang = re.sub(r'[^\w\-]', '_', audio_stream['language'])
        output_name = f"audio_{audio_index}_{safe_lang}_{quality}"

        print(f"   Converting: Audio #{audio_index} ({audio_stream['title']}) - {quality} quality...")

        cmd = [
            'ffmpeg',
            '-i', self.input_file,
            '-map', f"0:{audio_stream['index']}",
            '-c:a', 'aac',
            '-b:a', profile['bitrate'],
            '-ar', profile['sample_rate'],
            '-ac', '2',  # Stereo
            '-f', 'hls',
            '-hls_time', '6',
            '-hls_playlist_type', 'vod',
            '-hls_segment_type', 'mpegts',
            '-hls_segment_filename', str(self.output_dir / f"{output_name}_%03d.ts"),
            '-v', 'warning',
            '-y',
            str(self.output_dir / f"{output_name}.m3u8")
        ]

        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            print(f"         ✓ Completed")
            return True
        except subprocess.CalledProcessError as e:
            print(f"         ✗ Failed: {e.stderr.decode()[:200]}")
            return False

    def convert_video_quality_variant(self, profile_name: str, profile: Dict) -> bool:
        """Convert video-only stream for a specific quality"""
        print(f"\n🎬 Converting {profile_name} quality video...")

        scale, width, height = self._calculate_scale(profile['height'])
        output_name = f"video_{profile_name}"

        # Video-only encoding
        cmd = [
            'ffmpeg',
            '-i', self.input_file,
            '-map', f"0:{self.video_info['index']}",
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-profile:v', 'high',
            '-level', '4.1',
            '-crf', profile['crf'],
            '-maxrate', profile['maxrate'],
            '-bufsize', profile['bufsize'],
            '-vf', f"scale={scale}:flags=lanczos",
            '-g', str(int(self.video_info['fps'] * 2)),
            '-keyint_min', str(int(self.video_info['fps'])),
            '-sc_threshold', '0',
            '-pix_fmt', 'yuv420p',
            '-an',  # No audio in video-only stream
            '-f', 'hls',
            '-hls_time', '6',
            '-hls_playlist_type', 'vod',
            '-hls_segment_type', 'mpegts',
            '-hls_segment_filename', str(self.output_dir / f"{output_name}_%03d.ts"),
            '-v', 'warning',
            '-stats',
            '-y',
            str(self.output_dir / f"{output_name}.m3u8")
        ]

        try:
            print(f"   Settings: {width}x{height}, bitrate={profile['video_bitrate']}, CRF={profile['crf']}")
            print(f"   Encoding (this may take a while)...")

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Monitor progress
            for line in process.stderr:
                if 'frame=' in line:
                    # Extract and display progress
                    print(f"   {line.strip()}\r", end='', flush=True)

            process.wait()

            if process.returncode == 0:
                print(f"\n   ✅ {profile_name} video completed successfully")
                return True
            else:
                print(f"\n   ❌ {profile_name} video failed")
                return False

        except Exception as e:
            print(f"\n   ❌ Error: {e}")
            return False

    def convert_all_audio_tracks(self):
        """Convert all audio tracks for all quality levels"""
        if not self.audio_streams:
            print("\n⚠️  No audio streams found - creating video-only streams")
            return True

        print(f"\n{'='*70}")
        print(f"🔊 Converting {len(self.audio_streams)} audio track(s)")
        print(f"{'='*70}")

        success = True
        for i, audio in enumerate(self.audio_streams):
            print(f"\n📻 Audio Track #{i}: {audio['title']} ({audio['language']})")
            for quality in ['high']: #, 'medium', 'low'
                if not self.convert_audio_track(i, audio, quality):
                    success = False
                    print(f"   ⚠️  Warning: Audio {i} ({quality}) conversion failed")

        return success

    def create_master_playlist(self):
        """Create master playlist with all variants, audio tracks, and subtitles"""
        print("\n📋 Creating master playlist...")

        master_file = self.output_dir / "master.m3u8"

        with open(master_file, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write("#EXT-X-VERSION:6\n\n")

            # Audio track declarations
            if self.audio_streams:
                f.write("# Audio tracks\n")
                for i, audio in enumerate(self.audio_streams):
                    safe_lang = re.sub(r'[^\w\-]', '_', audio['language'])

                    # Check if audio files exist before adding
                    for quality_level in ['high']: #, 'medium', 'low'
                        audio_file = self.output_dir / f"audio_{i}_{safe_lang}_{quality_level}.m3u8"
                        if audio_file.exists():
                            f.write(f'#EXT-X-MEDIA:TYPE=AUDIO,')
                            f.write(f'GROUP-ID="audio-{quality_level}",')
                            f.write(f'NAME="{audio["title"]}",')
                            f.write(f'LANGUAGE="{audio["language"]}",')
                            f.write(f'DEFAULT={"YES" if i == 0 else "NO"},')
                            f.write(f'AUTOSELECT={"YES" if i == 0 else "NO"},')
                            f.write(f'CHANNELS="{audio["channels"]}",')
                            f.write(f'URI="audio_{i}_{safe_lang}_{quality_level}.m3u8"\n')

                f.write("\n")

            # Subtitle declarations - only include playlists that exist
            has_subtitles = False
            if self.subtitle_streams:
                f.write("# Subtitles\n")
                subtitle_index = 0
                for i, subtitle in enumerate(self.subtitle_streams):
                    safe_lang = re.sub(r'[^\w\-]', '_', subtitle['language'])
                    subtitle_playlist_file = f"subtitle_{i}_{safe_lang}.m3u8"
                    subtitle_path = self.output_dir / subtitle_playlist_file

                    # Only add subtitle if its M3U8 playlist exists and has content
                    if subtitle_path.exists() and subtitle_path.stat().st_size > 50:
                        f.write(f'#EXT-X-MEDIA:TYPE=SUBTITLES,')
                        f.write(f'GROUP-ID="subs",')
                        f.write(f'NAME="{subtitle["title"]}",')
                        f.write(f'LANGUAGE="{subtitle["language"]}",')
                        f.write(f'DEFAULT={"YES" if subtitle_index == 0 else "NO"},')
                        f.write(f'AUTOSELECT={"YES" if subtitle_index == 0 else "NO"},')
                        f.write(f'FORCED=NO,')
                        f.write(f'URI="{subtitle_playlist_file}"\n')
                        has_subtitles = True
                        subtitle_index += 1

                if has_subtitles:
                    f.write("\n")

            # Video variants
            f.write("# Video variants\n")
            for profile_name in ['high']: #, 'medium', 'low'
                profile = self.quality_profiles[profile_name]
                audio_profile = self.audio_profiles[profile_name]
                scale, width, height = self._calculate_scale(profile['height'])

                video_playlist = f"video_{profile_name}.m3u8"

                if (self.output_dir / video_playlist).exists():
                    # Calculate bandwidth
                    video_bw = int(profile['video_bitrate'].replace('k', '000'))
                    audio_bw = int(audio_profile['bitrate'].replace('k', '000'))
                    total_bandwidth = video_bw + audio_bw

                    f.write(f'#EXT-X-STREAM-INF:')
                    f.write(f'BANDWIDTH={total_bandwidth},')
                    f.write(f'AVERAGE-BANDWIDTH={int(total_bandwidth * 0.9)},')
                    f.write(f'RESOLUTION={width}x{height},')
                    f.write(f'CODECS="avc1.640029,mp4a.40.2",')
                    f.write(f'FRAME-RATE={self.video_info["fps"]:.3f}')

                    if self.audio_streams:
                        f.write(f',AUDIO="audio-{profile_name}"')

                    if has_subtitles:
                        f.write(',SUBTITLES="subs"')

                    f.write(f'\n{video_playlist}\n')

        print(f"   ✓ Master playlist created: {master_file}")

        # Verify subtitle files
        if self.subtitle_streams:
            print("\n🔍 Verifying subtitle files...")
            subtitle_count = 0
            for i, subtitle in enumerate(self.subtitle_streams):
                safe_lang = re.sub(r'[^\w\-]', '_', subtitle['language'])
                subtitle_file = self.output_dir / f"subtitle_{i}_{safe_lang}.m3u8"
                if subtitle_file.exists() and subtitle_file.stat().st_size > 50:
                    size_kb = subtitle_file.stat().st_size / 1024
                    print(f"   ✓ {subtitle_file.name} (Playlist, {size_kb:.1f} KB)")
                    subtitle_count += 1
                else:
                    # Don't print "not created" for image-based subs that were intentionally skipped
                    codec = subtitle['codec'].lower()
                    image_based_codecs = ['hdmv_pgs_subtitle', 'dvd_subtitle', 'dvdsub', 'pgssub', 'pgs']
                    if codec not in image_based_codecs:
                        print(f"   ✗ {subtitle_file.name} (not created)")

            if subtitle_count == 0 and len(self.subtitle_streams) > 0:
                print(f"   ⚠️  No subtitle files were successfully created")

        # Verify other files
        print("\n🔍 Verifying video and audio files...")
        missing_files = []

        # Check video files
        for profile_name in ['high']: #, 'medium', 'low'
            video_file = self.output_dir / f"video_{profile_name}.m3u8"
            if not video_file.exists():
                missing_files.append(str(video_file))
            else:
                print(f"   ✓ video_{profile_name}.m3u8")

        # Check audio files
        if self.audio_streams:
            for i, audio in enumerate(self.audio_streams):
                safe_lang = re.sub(r'[^\w\-]', '_', audio['language'])
                for quality in ['high']: #, 'medium', 'low'
                    audio_file = self.output_dir / f"audio_{i}_{safe_lang}_{quality}.m3u8"
                    if not audio_file.exists():
                        missing_files.append(str(audio_file))

        if missing_files:
            print("\n⚠️  Warning: Some files are missing:")
            for f in missing_files[:5]:
                print(f"   - {f}")
            if len(missing_files) > 5:
                print(f"   ... and {len(missing_files) - 5} more")
        else:
            print("   ✓ All required files verified")

        return str(master_file)

    def convert(self) -> bool:
        """Main conversion process"""
        print("\n" + "="*70)
        print(" "*20 + "🎥 HLS VIDEO CONVERTER")
        print("="*70)
        print(f"📁 Input:  {self.input_file}")
        print(f"📁 Output: {self.output_dir}")
        print("="*70)

        # Check ffmpeg
        if not self.check_ffmpeg():
            return False

        # Probe file
        if not self.probe_file():
            return False

        # Convert subtitles first (fast)
        if self.subtitle_streams:
            self.convert_subtitles()
        else:
            print("\n⚠️  No subtitles detected in source file")

        # Convert all video quality variants
        print(f"\n{'='*70}")
        print("PHASE 1: Converting Video Streams")
        print(f"{'='*70}")

        video_success = True
        for profile_name in ['high']: #, 'medium', 'low'
            if not self.convert_video_quality_variant(profile_name, self.quality_profiles[profile_name]):
                video_success = False
                print(f"⚠️  Warning: {profile_name} video conversion failed")

        # Convert all audio tracks
        print(f"\n{'='*70}")
        print("PHASE 2: Converting Audio Streams")
        print(f"{'='*70}")

        audio_success = self.convert_all_audio_tracks()

        if not video_success or not audio_success:
            print("\n⚠️  Some conversions failed, but continuing...")

        # Create master playlist
        master_playlist = self.create_master_playlist()

        # Summary
        subtitle_count = len([f for f in self.output_dir.glob("subtitle_*.m3u8")
                              if f.stat().st_size > 50])

        print("\n" + "="*70)
        print(" "*20 + "✅ CONVERSION COMPLETED!")
        print("="*70)
        print(f"📁 Output directory:    {self.output_dir}")
        print(f"🎬 Master playlist:     {master_playlist}")
        print(f"📺 Video qualities:     1 (high: 1080p)")
        print(f"🔊 Audio tracks:        {len(self.audio_streams)}")
        print(f"💬 Subtitle tracks:     {subtitle_count}/{len(self.subtitle_streams)} converted")
        print("="*70)
        print("\n💡 Next Steps:")
        print(f"   1. Serve files from: {self.output_dir}")
        print(f"   2. Point player to:  master.m3u8")
        print("="*70 + "\n")

        return True


def main():
    parser = argparse.ArgumentParser(
        description='Convert MKV/MP4 to HLS with multiple audio tracks and subtitles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.mkv output_dir
  %(prog)s /path/to/movie.mp4 /var/www/html/streams/movie

Output structure:
  - master.m3u8 (main playlist)
  - video_high.m3u8, video_medium.m3u8, etc. (video stream playlists)
  - audio_0_eng_high.m3u8, etc. (audio stream playlists)
  - subtitle_0_eng.m3u8, etc. (subtitle playlists)
  - various .ts and .vtt segment files

Features:
  ✓ Preserves all audio tracks with separate streams
  ✓ Converts all text-based subtitles to WebVTT for HLS
  ✓ Creates multiple quality levels (configurable in script)
  ✓ Proper HLS structure for browser compatibility
  ✓ Detailed progress monitoring and error reporting
        """
    )

    parser.add_argument('input', help='Input video file (MKV or MP4)')
    parser.add_argument('output', help='Output directory for HLS files')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    # Validate input file
    if not os.path.isfile(args.input):
        print(f"❌ ERROR: Input file does not exist: {args.input}")
        sys.exit(1)

    # Validate file extension
    valid_extensions = ['.mkv', '.mp4', 'avi', '.mov', '.m4v', '.webm']
    file_ext = os.path.splitext(args.input)[1].lower()
    if file_ext not in valid_extensions:
        print(f"⚠️  WARNING: File extension '{file_ext}' may not be supported")
        print(f"   Supported: {', '.join(valid_extensions)}")
        response = input("   Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

    # Create converter and run
    converter = HLSConverter(args.input, args.output)

    try:
        if converter.convert():
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Conversion interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()