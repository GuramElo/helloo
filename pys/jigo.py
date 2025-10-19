#!/usr/bin/env python3
"""
HLS Video Converter with Stable Subtitle Support
Converts MKV/MP4 files to HLS format with multiple quality variants,
separate audio tracks, and native browser-compatible subtitles.

VERSION 4.5: Optional explicit quality selection via --explicit-qualities flag
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
    def __init__(self, input_file: str, output_dir: str, best_quality: bool = False, explicit_qualities: List[str] = None):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.best_quality = best_quality

        # Determine which qualities to generate
        if explicit_qualities:
            self.enabled_qualities = explicit_qualities
        else:
            self.enabled_qualities = ['high', 'medium', 'low']  # Default: all qualities

        # Quality profiles will be determined after probing the video
        self.quality_profiles = {}

        # Audio profiles - depend on quality mode
        if best_quality:
            self.audio_profiles = {
                'high': {'bitrate': '256k', 'sample_rate': '48000'},
                'medium': {'bitrate': '192k', 'sample_rate': '48000'},
                'low': {'bitrate': '128k', 'sample_rate': '48000'},
            }
        else:
            self.audio_profiles = {
                'high': {'bitrate': '192k', 'sample_rate': '48000'},
                'medium': {'bitrate': '128k', 'sample_rate': '48000'},
                'low': {'bitrate': '96k', 'sample_rate': '48000'},
            }

        self.video_info = None
        self.audio_streams = []
        self.subtitle_streams = []
        self.converted_subtitles = []

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

    def _determine_quality_ladder(self):
        """
        Determine quality ladder based on source resolution:

        4K (≥2160p):     high=4K,     medium=1080p, low=480p
        Under 4K:        high=Original, medium=720p,  low=480p
        """
        source_height = self.video_info['height']

        print(f"\n🎯 Determining quality ladder for {source_height}p source...")

        if source_height >= 2160:  # 4K or higher
            if self.best_quality:
                # Maximum quality settings
                self.quality_profiles = {
                    'high': {
                        'name': 'high',
                        'height': source_height,
                        'video_bitrate': '18000k',
                        'maxrate': '20000k',
                        'bufsize': '27000k',
                        'crf': '18',
                        'preset': 'slow',
                        'use_advanced': True,
                    },
                    'medium': {
                        'name': 'medium',
                        'height': 1080,
                        'video_bitrate': '6000k',
                        'maxrate': '6500k',
                        'bufsize': '9000k',
                        'crf': '21',
                        'preset': 'slow',
                        'use_advanced': True,
                    },
                    'low': {
                        'name': 'low',
                        'height': 480,
                        'video_bitrate': '1800k',
                        'maxrate': '2000k',
                        'bufsize': '2700k',
                        'crf': '23',
                        'preset': 'medium',
                        'use_advanced': True,
                    },
                }
                print(f"   📊 4K Quality Ladder: {source_height}p → 1080p → 480p")
                print(f"   ⚙️  Encoding Mode: ⭐ MAXIMUM QUALITY (slow presets + advanced x264)")
            else:
                # Fast mode settings
                self.quality_profiles = {
                    'high': {
                        'name': 'high',
                        'height': source_height,
                        'video_bitrate': '16000k',
                        'maxrate': '17000k',
                        'bufsize': '24000k',
                        'crf': '20',
                        'preset': 'medium',
                        'use_advanced': False,
                    },
                    'medium': {
                        'name': 'medium',
                        'height': 1080,
                        'video_bitrate': '5000k',
                        'maxrate': '5350k',
                        'bufsize': '7500k',
                        'crf': '23',
                        'preset': 'medium',
                        'use_advanced': False,
                    },
                    'low': {
                        'name': 'low',
                        'height': 480,
                        'video_bitrate': '1400k',
                        'maxrate': '1500k',
                        'bufsize': '2100k',
                        'crf': '26',
                        'preset': 'fast',
                        'use_advanced': False,
                    },
                }
                print(f"   📊 4K Quality Ladder: {source_height}p → 1080p → 480p")
                print(f"   ⚙️  Encoding Mode: ⚡ BALANCED (medium/fast presets)")

        else:  # Under 4K - use original resolution
            # Calculate appropriate bitrate based on source resolution
            if source_height >= 1080:
                if self.best_quality:
                    high_bitrate = '6000k'
                    high_maxrate = '6500k'
                    high_bufsize = '9000k'
                    high_crf = '19'
                    high_preset = 'slow'
                else:
                    high_bitrate = '5000k'
                    high_maxrate = '5350k'
                    high_bufsize = '7500k'
                    high_crf = '21'
                    high_preset = 'medium'
            elif source_height >= 720:
                if self.best_quality:
                    high_bitrate = '3500k'
                    high_maxrate = '3800k'
                    high_bufsize = '5200k'
                    high_crf = '20'
                    high_preset = 'slow'
                else:
                    high_bitrate = '2800k'
                    high_maxrate = '3000k'
                    high_bufsize = '4200k'
                    high_crf = '22'
                    high_preset = 'medium'
            else:  # 480p or lower
                if self.best_quality:
                    high_bitrate = '1800k'
                    high_maxrate = '2000k'
                    high_bufsize = '2700k'
                    high_crf = '21'
                    high_preset = 'medium'
                else:
                    high_bitrate = '1400k'
                    high_maxrate = '1500k'
                    high_bufsize = '2100k'
                    high_crf = '23'
                    high_preset = 'fast'

            if self.best_quality:
                self.quality_profiles = {
                    'high': {
                        'name': 'high',
                        'height': source_height,
                        'video_bitrate': high_bitrate,
                        'maxrate': high_maxrate,
                        'bufsize': high_bufsize,
                        'crf': high_crf,
                        'preset': high_preset,
                        'use_advanced': True,
                    },
                    'medium': {
                        'name': 'medium',
                        'height': 720,
                        'video_bitrate': '3500k',
                        'maxrate': '3800k',
                        'bufsize': '5200k',
                        'crf': '21',
                        'preset': 'slow',
                        'use_advanced': True,
                    },
                    'low': {
                        'name': 'low',
                        'height': 480,
                        'video_bitrate': '1800k',
                        'maxrate': '2000k',
                        'bufsize': '2700k',
                        'crf': '23',
                        'preset': 'medium',
                        'use_advanced': True,
                    },
                }
                print(f"   📊 Quality Ladder: {source_height}p (original) → 720p → 480p")
                print(f"   ⚙️  Encoding Mode: ⭐ MAXIMUM QUALITY (slow presets + advanced x264)")
            else:
                self.quality_profiles = {
                    'high': {
                        'name': 'high',
                        'height': source_height,
                        'video_bitrate': high_bitrate,
                        'maxrate': high_maxrate,
                        'bufsize': high_bufsize,
                        'crf': high_crf,
                        'preset': high_preset,
                        'use_advanced': False,
                    },
                    'medium': {
                        'name': 'medium',
                        'height': 720,
                        'video_bitrate': '2800k',
                        'maxrate': '3000k',
                        'bufsize': '4200k',
                        'crf': '23',
                        'preset': 'medium',
                        'use_advanced': False,
                    },
                    'low': {
                        'name': 'low',
                        'height': 480,
                        'video_bitrate': '1400k',
                        'maxrate': '1500k',
                        'bufsize': '2100k',
                        'crf': '26',
                        'preset': 'fast',
                        'use_advanced': False,
                    },
                }
                print(f"   📊 Quality Ladder: {source_height}p (original) → 720p → 480p")
                print(f"   ⚙️  Encoding Mode: ⚡ BALANCED (medium/fast presets)")

        # Show which qualities will be generated
        enabled_resolutions = []
        for quality in self.enabled_qualities:
            _, w, h = self._calculate_scale(self.quality_profiles[quality]['height'])
            enabled_resolutions.append(f"{quality}={w}x{h}")

        print(f"   🎯 Enabled Qualities: {', '.join(enabled_resolutions)}")

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

            # Determine quality ladder based on source resolution
            self._determine_quality_ladder()

            print(f"\n{'='*70}")
            print(f"📹 Video: {self.video_info['codec']} "
                  f"{self.video_info['width']}x{self.video_info['height']} "
                  f"@ {self.video_info['fps']} fps")

            # Show resolution category
            if self.video_info['height'] >= 2160:
                print(f"   🎬 Resolution Category: 4K/UHD")
            elif self.video_info['height'] >= 1440:
                print(f"   🎬 Resolution Category: 2K/QHD")
            elif self.video_info['height'] >= 1080:
                print(f"   🎬 Resolution Category: Full HD")
            elif self.video_info['height'] >= 720:
                print(f"   🎬 Resolution Category: HD")
            else:
                print(f"   🎬 Resolution Category: SD")

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
        preset = profile['preset']

        # Build encoding command
        cmd = [
            'ffmpeg',
            '-i', self.input_file,
            '-map', f"0:{self.video_info['index']}",
            '-c:v', 'libx264',
            '-preset', preset,
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
        ]

        # Add advanced x264 options if in best quality mode
        if profile.get('use_advanced', False):
            cmd.extend([
                '-x264-params',
                'ref=5:bframes=5:b-adapt=2:direct=auto:me=umh:subme=9:trellis=2:aq-mode=3:aq-strength=0.8'
            ])

        # Continue with remaining options
        cmd.extend([
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
        ])

        try:
            print(f"   Settings:")
            print(f"      Resolution: {width}x{height}")
            print(f"      Bitrate: {profile['video_bitrate']} (max: {profile['maxrate']})")
            print(f"      CRF: {profile['crf']} (lower = better quality)")
            print(f"      Preset: {preset}")
            if profile.get('use_advanced', False):
                print(f"      Advanced x264: ref=5, bframes=5, subme=9, trellis=2")
            print(f"   Encoding...")

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
        """Convert all audio tracks for enabled quality levels only"""
        if not self.audio_streams:
            print("\n⚠️  No audio streams found - creating video-only streams")
            return True

        print(f"\n{'='*70}")
        print(f"🔊 Converting {len(self.audio_streams)} audio track(s) x {len(self.enabled_qualities)} qualities")
        print(f"{'='*70}")

        success = True
        for i, audio in enumerate(self.audio_streams):
            print(f"\n📻 Audio Track #{i}: {audio['title']} ({audio['language']})")
            for quality in self.enabled_qualities:
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

            # Audio track declarations (only for enabled qualities)
            if self.audio_streams:
                f.write("# Audio tracks\n")
                for i, audio in enumerate(self.audio_streams):
                    safe_lang = re.sub(r'[^\w\-]', '_', audio['language'])

                    for quality_level in self.enabled_qualities:
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

            # Video variants (only for enabled qualities, sorted by quality: high, medium, low)
            f.write("# Video variants\n")
            for profile_name in ['high', 'medium', 'low']:
                if profile_name not in self.enabled_qualities:
                    continue

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

                    f.write(f'\n{video_playlist}\n')

        print(f"   ✓ Master playlist created: {master_file}")
        return str(master_file)

    def convert(self) -> bool:
        """Main conversion process"""
        mode_label = "Maximum Quality Mode" if self.best_quality else "Balanced Mode"
        print("\n" + "="*70)
        print(f" "*10 + f"🎥 HLS VIDEO CONVERTER ({mode_label})")
        print("="*70)
        print(f"📁 Input:  {self.input_file}")
        print(f"📁 Output: {self.output_dir}")
        if self.best_quality:
            print(f"⭐ Quality: MAXIMUM (slow presets, advanced x264)")
        else:
            print(f"⚡ Quality: BALANCED (medium/fast presets)")
        print(f"🎯 Qualities: {', '.join(self.enabled_qualities)}")
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

        # Convert video (only enabled qualities)
        print(f"\n{'='*70}")
        print(f"PHASE 1: Converting Video Streams ({len(self.enabled_qualities)} qualities)")
        print(f"{'='*70}")

        video_success = True
        for profile_name in self.enabled_qualities:
            if not self.convert_video_quality_variant(profile_name, self.quality_profiles[profile_name]):
                video_success = False

        # Convert audio (only for enabled qualities)
        print(f"\n{'='*70}")
        print("PHASE 2: Converting Audio Streams")
        print(f"{'='*70}")

        audio_success = self.convert_all_audio_tracks()

        # Create master playlist
        master_playlist = self.create_master_playlist()

        # Summary - only show enabled qualities
        quality_summary = []
        for profile_name in self.enabled_qualities:
            _, w, h = self._calculate_scale(self.quality_profiles[profile_name]['height'])
            quality_summary.append({
                'name': profile_name,
                'width': w,
                'height': h,
                'bitrate': self.quality_profiles[profile_name]['video_bitrate'],
                'crf': self.quality_profiles[profile_name]['crf'],
                'preset': self.quality_profiles[profile_name]['preset']
            })

        # Get audio bitrate summary for enabled qualities
        audio_bitrates = '/'.join([self.audio_profiles[q]['bitrate'] for q in self.enabled_qualities])

        print("\n" + "="*70)
        print(" "*20 + "✅ CONVERSION COMPLETED!")
        print("="*70)
        print(f"📁 Output directory:    {self.output_dir}")
        print(f"🎬 Master playlist:     {master_playlist}")
        print(f"📺 Source resolution:   {self.video_info['width']}x{self.video_info['height']}")
        print(f"\n   Generated Quality Tiers ({len(self.enabled_qualities)}):")

        for q in quality_summary:
            label = q['name'].capitalize()
            print(f"   • {label:6} {q['width']}x{q['height']} @ {q['bitrate']}, "
                  f"CRF {q['crf']}, preset={q['preset']}")

        print(f"\n🔊 Audio tracks:        {len(self.audio_streams)} x {len(self.enabled_qualities)} qualities ({audio_bitrates})")
        print(f"💬 Subtitle tracks:     {len(self.converted_subtitles)} converted")
        if self.converted_subtitles:
            print(f"📄 Subtitle manifest:   subtitles.json")
        print("="*70)

        # Show quality ladder explanation
        if self.video_info['height'] >= 2160:
            available = "4K → 1080p → 480p"
        else:
            available = "Original → 720p → 480p"

        generated = ' → '.join([f"{self.enabled_qualities[i]}" for i in range(len(self.enabled_qualities))])

        print(f"\n💡 Available Qualities: {available}")
        print(f"   Generated: {generated}")

        if self.best_quality:
            print("\n   ⭐ Maximum Quality Optimizations Applied:")
            print("      • Slow encoding presets (3-10x longer encoding time)")
            print("      • Lower CRF values (18-23) for higher quality")
            print("      • Advanced x264: ref=5, bframes=5, subme=9, trellis=2")
            print("      • Adaptive quantization (aq-mode=3)")
            print("      • Higher audio bitrates (256k/192k/128k)")
        else:
            print("\n   ⚡ Balanced Mode:")
            print("      • Medium/fast presets (faster encoding)")
            print("      • Balanced quality settings")
            print("      • Good quality/speed ratio")

        print("      • Lanczos scaling algorithm")
        print("="*70 + "\n")

        return True


def main():
    parser = argparse.ArgumentParser(
        description='Convert MKV/MP4 to HLS with adaptive quality streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Balanced mode with all 3 qualities (default)
  %(prog)s input.mkv output_dir/

  # Maximum quality mode with all 3 qualities
  %(prog)s input.mkv output_dir/ --best-quality

  # Generate only high and low quality
  %(prog)s input.mkv output_dir/ --explicit-qualities=high,low

  # Generate only medium quality
  %(prog)s input.mkv output_dir/ --explicit-qualities=medium

  # Maximum quality, only high quality output
  %(prog)s input.mkv output_dir/ --best-quality --explicit-qualities=high

Quality Modes:
  Balanced (default):  Medium/fast presets, CRF 20-26, good speed/quality ratio
  Best Quality (-b):   Slow presets, CRF 18-23, advanced x264, 3-10x slower

Quality Levels:
  high:    Original/4K resolution (high bitrate)
  medium:  720p/1080p resolution (medium bitrate)
  low:     480p resolution (low bitrate, always compatible)
        """
    )

    parser.add_argument('input', help='Input video file (MKV or MP4)')
    parser.add_argument('output', help='Output directory for HLS files')
    parser.add_argument('--best-quality', '-b', action='store_true',
                        help='Use slowest presets and best quality settings (much slower)')
    parser.add_argument('--explicit-qualities', '-q', type=str, default=None,
                        help='Comma-separated list of qualities to generate (high,medium,low). Default: all three')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"❌ ERROR: Input file does not exist: {args.input}")
        sys.exit(1)

    # Parse explicit qualities if provided
    explicit_qualities = None
    if args.explicit_qualities:
        explicit_qualities = [q.strip().lower() for q in args.explicit_qualities.split(',')]

        # Validate quality names
        valid_qualities = {'high', 'medium', 'low'}
        invalid = set(explicit_qualities) - valid_qualities
        if invalid:
            print(f"❌ ERROR: Invalid quality level(s): {', '.join(invalid)}")
            print(f"   Valid options: high, medium, low")
            sys.exit(1)

        # Remove duplicates while preserving order
        seen = set()
        explicit_qualities = [q for q in explicit_qualities if not (q in seen or seen.add(q))]

        if not explicit_qualities:
            print(f"❌ ERROR: No valid qualities specified")
            sys.exit(1)

    valid_extensions = ['.mkv', '.mp4', '.avi', '.mov', '.m4v', '.webm']
    file_ext = os.path.splitext(args.input)[1].lower()
    if file_ext not in valid_extensions:
        print(f"⚠️  WARNING: File extension '{file_ext}' may not be supported")
        print(f"   Supported: {', '.join(valid_extensions)}")
        response = input("   Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

    converter = HLSConverter(args.input, args.output,
                            best_quality=args.best_quality,
                            explicit_qualities=explicit_qualities)

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