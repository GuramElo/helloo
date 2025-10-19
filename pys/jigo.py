#!/usr/bin/env python3
"""
HLS Video Converter with Stable Subtitle Support
Converts MKV/MP4 files to HLS format with multiple quality variants,
separate audio tracks, and native browser-compatible subtitles.

VERSION 4: Subtitles are completely separate from HLS (most stable approach)
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
        }

        # Audio profiles
        self.audio_profiles = {
            'high': {'bitrate': '192k', 'sample_rate': '48000'},
        }

        self.video_info = None
        self.audio_streams = []
        self.subtitle_streams = []
        self.converted_subtitles = []  # Track successfully converted subtitles

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

        if target_height >= source_height:
            target_height = source_height
            target_width = source_width
        else:
            aspect_ratio = source_width / source_height
            target_width = int(target_height * aspect_ratio)

        target_width = target_width if target_width % 2 == 0 else target_width - 1
        target_height = target_height if target_height % 2 == 0 else target_height - 1

        return f"{target_width}:{target_height}", target_width, target_height

    def convert_subtitles(self):
        """Convert subtitles to standalone WebVTT files (NOT in HLS manifest)"""
        print("\n📝 Converting subtitles to standalone WebVTT files...")

        if not self.subtitle_streams:
            print("   ⚠️  No subtitle streams detected to convert")
            return

        image_based_codecs = ['hdmv_pgs_subtitle', 'dvd_subtitle', 'dvdsub', 'pgssub', 'pgs']
        converted_count = 0
        skipped_count = 0

        for i, subtitle in enumerate(self.subtitle_streams):
            safe_lang = re.sub(r'[^\w\-]', '_', subtitle['language'])
            safe_title = re.sub(r'[^\w\-\s]', '_', subtitle['title'])

            output_vtt = self.output_dir / f"subtitle_{i}_{safe_lang}.vtt"
            codec = subtitle['codec'].lower()

            print(f"\n   [{i}] Processing: {subtitle['title']} ({subtitle['language']})")
            print(f"       Codec: {codec}")

            if codec in image_based_codecs:
                print(f"       ⚠️  Image-based subtitle format detected. Skipping.")
                skipped_count += 1
                continue

            cmd = [
                'ffmpeg',
                '-v', 'warning',
                '-i', self.input_file,
                '-map', f"0:{subtitle['index']}",
                '-c:s', 'webvtt',
                '-y',
                str(output_vtt)
            ]

            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                    timeout=180
                )

                if output_vtt.exists() and output_vtt.stat().st_size > 10:
                    size_kb = output_vtt.stat().st_size / 1024
                    print(f"       ✅ Successfully converted ({size_kb:.1f} KB)")
                    converted_count += 1

                    # Store info about successfully converted subtitle
                    self.converted_subtitles.append({
                        'file': output_vtt.name,
                        'language': subtitle['language'],
                        'title': subtitle['title'],
                        'index': i
                    })
                else:
                    print(f"       ❌ Conversion failed: output file is empty or missing")
                    skipped_count += 1

            except subprocess.TimeoutExpired:
                print(f"       ❌ Conversion timed out (>180 seconds)")
                skipped_count += 1
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
                print(f"       ❌ Conversion failed.")
                print(f"          Error: {error_msg[:200]}")
                skipped_count += 1

        print(f"\n   {'='*66}")
        print(f"   📊 Subtitle Conversion Summary:")
        print(f"      ✅ Converted: {converted_count}")
        print(f"      ❌ Skipped:   {skipped_count}")
        print(f"   {'='*66}")

    def create_subtitle_manifest(self):
        """Create a JSON manifest file with subtitle information"""
        if not self.converted_subtitles:
            return

        manifest_file = self.output_dir / "subtitles.json"

        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump({
                'subtitles': self.converted_subtitles
            }, f, indent=2, ensure_ascii=False)

        print(f"\n   ✓ Subtitle manifest created: {manifest_file}")
        print(f"      Contains {len(self.converted_subtitles)} subtitle track(s)")

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
            '-ac', '2',
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
            '-an',
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

            for line in process.stderr:
                if 'frame=' in line:
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
            for quality in ['high']:
                if not self.convert_audio_track(i, audio, quality):
                    success = False
                    print(f"   ⚠️  Warning: Audio {i} ({quality}) conversion failed")

        return success

    def create_master_playlist(self):
        """Create master playlist (WITHOUT subtitle references - subtitles are separate)"""
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

                    for quality_level in ['high']:
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

            # NOTE: NO subtitle references in HLS manifest
            # Subtitles are handled separately via subtitles.json

            # Video variants
            f.write("# Video variants\n")
            for profile_name in ['high']:
                profile = self.quality_profiles[profile_name]
                audio_profile = self.audio_profiles[profile_name]
                scale, width, height = self._calculate_scale(profile['height'])

                video_playlist = f"video_{profile_name}.m3u8"

                if (self.output_dir / video_playlist).exists():
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

                    # NO subtitle reference here
                    f.write(f'\n{video_playlist}\n')

        print(f"   ✓ Master playlist created: {master_file}")
        return str(master_file)

    def convert(self) -> bool:
        """Main conversion process"""
        print("\n" + "="*70)
        print(" "*15 + "🎥 HLS VIDEO CONVERTER (Stable Subtitles)")
        print("="*70)
        print(f"📁 Input:  {self.input_file}")
        print(f"📁 Output: {self.output_dir}")
        print("="*70)

        if not self.check_ffmpeg():
            return False

        if not self.probe_file():
            return False

        # Convert subtitles first
        if self.subtitle_streams:
            self.convert_subtitles()
            self.create_subtitle_manifest()
        else:
            print("\n⚠️  No subtitles detected in source file")

        # Convert video
        print(f"\n{'='*70}")
        print("PHASE 1: Converting Video Streams")
        print(f"{'='*70}")

        video_success = True
        for profile_name in ['high']:
            if not self.convert_video_quality_variant(profile_name, self.quality_profiles[profile_name]):
                video_success = False

        # Convert audio
        print(f"\n{'='*70}")
        print("PHASE 2: Converting Audio Streams")
        print(f"{'='*70}")

        audio_success = self.convert_all_audio_tracks()

        # Create master playlist
        master_playlist = self.create_master_playlist()

        # Summary
        print("\n" + "="*70)
        print(" "*20 + "✅ CONVERSION COMPLETED!")
        print("="*70)
        print(f"📁 Output directory:    {self.output_dir}")
        print(f"🎬 Master playlist:     {master_playlist}")
        print(f"📺 Video qualities:     1 (high: 1080p)")
        print(f"🔊 Audio tracks:        {len(self.audio_streams)}")
        print(f"💬 Subtitle tracks:     {len(self.converted_subtitles)} converted")
        if self.converted_subtitles:
            print(f"📄 Subtitle manifest:   subtitles.json")
            print(f"   Subtitles are SEPARATE from HLS (native browser rendering)")
        print("="*70)
        print("\n💡 Implementation Notes:")
        print("   • Subtitles are NOT in HLS manifest (better compatibility)")
        print("   • Use subtitles.json to load subtitle tracks in HTML")
        print("   • Subtitles render via native HTML5 <track> elements")
        print("="*70 + "\n")

        return True


def main():
    parser = argparse.ArgumentParser(
        description='Convert MKV/MP4 to HLS with stable subtitle support',
    )

    parser.add_argument('input', help='Input video file (MKV or MP4)')
    parser.add_argument('output', help='Output directory for HLS files')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"❌ ERROR: Input file does not exist: {args.input}")
        sys.exit(1)

    valid_extensions = ['.mkv', '.mp4', '.avi', '.mov', '.m4v', '.webm']
    file_ext = os.path.splitext(args.input)[1].lower()
    if file_ext not in valid_extensions:
        print(f"⚠️  WARNING: File extension '{file_ext}' may not be supported")
        print(f"   Supported: {', '.join(valid_extensions)}")
        response = input("   Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

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