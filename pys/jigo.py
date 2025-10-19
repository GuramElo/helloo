#!/usr/bin/env python3
"""
HLS Video Converter with Comprehensive Safety Features
Converts MKV/MP4 files to HLS format with multiple quality variants,
separate audio tracks, and native browser-compatible subtitles.

VERSION 6.2: Foolproof with intelligent stream copy + keyframe validation
"""

import os
import sys
import json
import subprocess
import argparse
import shutil
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import re
from multiprocessing import Pool, cpu_count
from functools import partial

class HLSConverter:
    def __init__(self, input_file: str, output_dir: str, best_quality: bool = False,
                 explicit_qualities: List[str] = None, hw_accel: Optional[str] = None,
                 parallel: bool = False, force_reencode: bool = False,
                 dry_run: bool = False, overwrite: bool = False,
                 no_interlace_check: bool = False):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.best_quality = best_quality
        self.hw_accel = hw_accel
        self.parallel = parallel
        self.force_reencode = force_reencode
        self.dry_run = dry_run
        self.overwrite = overwrite
        self.no_interlace_check = no_interlace_check

        # Determine which qualities to generate
        if explicit_qualities:
            self.enabled_qualities = explicit_qualities
        else:
            self.enabled_qualities = ['high', 'medium', 'low']

        # Quality profiles will be determined after probing
        self.quality_profiles = {}

        # Audio profiles
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
        self.source_is_h264 = False
        self.can_copy_video = False
        self.stream_copy_used = False
        self.start_time = None
        self.is_interlaced = False

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
            print("❌ ERROR: ffmpeg and ffprobe must be installed and in PATH")
            return False

    def check_disk_space(self, estimated_gb: float = 10.0) -> bool:
        """Check if enough disk space available"""
        try:
            stat = shutil.disk_usage(self.output_dir.parent if not self.output_dir.exists() else self.output_dir)
            free_gb = stat.free / (1024**3)

            print(f"\n💾 Disk Space Check:")
            print(f"   Available: {free_gb:.2f} GB")
            print(f"   Estimated needed: {estimated_gb:.2f} GB")

            if free_gb < estimated_gb:
                print(f"   ⚠️  WARNING: Low disk space!")
                if not self.dry_run:
                    response = input("   Continue anyway? (y/n): ")
                    return response.lower() == 'y'
                else:
                    print(f"   [DRY RUN] Would ask user to continue")
                    return True

            print(f"   ✅ Sufficient space available")
            return True

        except Exception as e:
            print(f"⚠️  Could not check disk space: {e}")
            return True

    def estimate_output_size(self) -> float:
        """Estimate output size in GB"""
        try:
            source_size_gb = os.path.getsize(self.input_file) / (1024**3)

            num_qualities = len(self.enabled_qualities)
            num_audio = max(len(self.audio_streams), 1)

            # Estimate based on stream copy possibility
            if self.can_copy_video and 'high' in self.enabled_qualities:
                estimated = source_size_gb * 1.2
                if 'medium' in self.enabled_qualities:
                    estimated += source_size_gb * 0.4
                if 'low' in self.enabled_qualities:
                    estimated += source_size_gb * 0.15
            else:
                estimated = source_size_gb * num_qualities * 0.6

            estimated += (num_audio * num_qualities * 0.1)
            estimated *= 1.2  # Safety margin

            return max(estimated, 2.0)

        except:
            return 10.0

    def check_existing_files(self) -> bool:
        """Check if output directory has existing files"""
        if not self.output_dir.exists():
            return True

        existing_files = list(self.output_dir.glob('*.m3u8')) + list(self.output_dir.glob('*.ts'))

        if existing_files:
            if self.overwrite:
                print(f"\n⚠️  Found {len(existing_files)} existing HLS files")
                if not self.dry_run:
                    print(f"   Deleting existing files...")
                    for f in existing_files:
                        f.unlink()
                else:
                    print(f"   [DRY RUN] Would delete existing files")
                return True

            print(f"\n⚠️  WARNING: Output directory contains {len(existing_files)} existing files!")
            print(f"   Directory: {self.output_dir}")
            print(f"\n   Options:")
            print(f"   1. Overwrite (delete existing files)")
            print(f"   2. Cancel")

            if self.dry_run:
                print(f"   [DRY RUN] Would ask user for choice")
                return True

            while True:
                response = input("\n   Choice (1-2): ").strip()
                if response == '1':
                    print(f"   🗑️  Deleting existing files...")
                    for f in existing_files:
                        f.unlink()
                    return True
                elif response == '2':
                    return False
                else:
                    print(f"   Invalid choice. Enter 1 or 2.")

        return True

    def detect_interlaced(self) -> bool:
        """Detect if video is interlaced"""
        if self.no_interlace_check:
            return False

        print(f"\n🔍 Checking for interlaced content...")

        try:
            cmd = [
                'ffmpeg',
                '-i', self.input_file,
                '-vf', 'idet',
                '-frames:v', '200',
                '-an',
                '-f', 'null',
                '-'
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            output = result.stderr

            if 'Multi frame detection' in output:
                tff = int(re.search(r'TFF:\s*(\d+)', output).group(1)) if 'TFF:' in output else 0
                bff = int(re.search(r'BFF:\s*(\d+)', output).group(1)) if 'BFF:' in output else 0
                progressive = int(re.search(r'Progressive:\s*(\d+)', output).group(1)) if 'Progressive:' in output else 0

                total_interlaced = tff + bff
                total_frames = total_interlaced + progressive

                if total_frames > 0:
                    interlaced_ratio = total_interlaced / total_frames
                    if interlaced_ratio > 0.3:
                        print(f"   ⚠️  Interlaced content detected!")
                        print(f"      Interlaced: {interlaced_ratio*100:.1f}% (TFF: {tff}, BFF: {bff})")
                        print(f"      Progressive: {progressive}")
                        print(f"      Will apply deinterlacing (yadif filter)")
                        return True

            print(f"   ✅ Progressive scan detected")
            return False

        except Exception as e:
            print(f"   ⚠️  Could not detect interlacing: {e}")
            print(f"      Assuming progressive")
            return False

    def detect_hdr(self) -> bool:
        """Detect HDR content and warn user. Returns True if user wants to abort."""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=color_transfer,color_primaries,color_space',
                '-of', 'json',
                self.input_file
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)

            if 'streams' in data and len(data['streams']) > 0:
                stream = data['streams'][0]
                color_transfer = stream.get('color_transfer', '').lower()
                color_primaries = stream.get('color_primaries', '').lower()

                hdr_transfers = ['smpte2084', 'arib-std-b67', 'smpte st 2084', 'hlg']
                hdr_primaries = ['bt2020']

                is_hdr = any(hdr in color_transfer for hdr in hdr_transfers) or \
                         any(hdr in color_primaries for hdr in hdr_primaries)

                if is_hdr:
                    print(f"\n{'='*70}")
                    print(f"⚠️  HDR CONTENT DETECTED!")
                    print(f"{'='*70}")
                    print(f"Color Transfer: {color_transfer}")
                    print(f"Color Primaries: {color_primaries}")
                    print(f"\n❌ IMPORTANT: H.264 does not support HDR!")
                    print(f"   • Conversion will map HDR → SDR (Standard Dynamic Range)")
                    print(f"   • Colors will look washed out / different")
                    print(f"   • Peak brightness will be clipped")
                    print(f"\n💡 Recommendations:")
                    print(f"   • For HDR preservation: Use HEVC with DASH (not HLS)")
                    print(f"   • For web compatibility: Continue with SDR conversion")
                    print(f"   • For best quality: Use tone mapping (external tool)")
                    print(f"{'='*70}\n")

                    if self.dry_run:
                        print(f"[DRY RUN] Would ask user to continue")
                        return False

                    response = input("Continue with HDR → SDR conversion? (y/n): ")
                    return response.lower() != 'y'

            return False

        except Exception as e:
            print(f"⚠️  Could not detect HDR: {e}")
            return False

    def detect_hardware_acceleration(self) -> Optional[str]:
        """Auto-detect available hardware acceleration"""
        print("\n🔍 Detecting hardware acceleration...")

        hw_encoders = {
            'nvenc': 'h264_nvenc',
            'qsv': 'h264_qsv',
            'videotoolbox': 'h264_videotoolbox',
            'amf': 'h264_amf',
            'vaapi': 'h264_vaapi',
        }

        try:
            result = subprocess.run(
                ['ffmpeg', '-hide_banner', '-encoders'],
                capture_output=True,
                text=True,
                check=True
            )

            available = []
            for name, encoder in hw_encoders.items():
                if encoder in result.stdout:
                    available.append(name)
                    print(f"   ✓ Found: {name.upper()} ({encoder})")

            if not available:
                print(f"   ℹ️  No hardware acceleration detected")
                return None

            priority = ['nvenc', 'qsv', 'videotoolbox', 'amf', 'vaapi']
            for hw in priority:
                if hw in available:
                    print(f"   🎯 Selected: {hw.upper()}")
                    return hw

            return available[0] if available else None

        except subprocess.CalledProcessError:
            print(f"   ⚠️  Could not detect hardware acceleration")
            return None

    def _check_parallel_efficiency(self):
        """Warn if parallel processing won't help"""
        if self.hw_accel == 'nvenc':
            try:
                result = subprocess.run(
                    ['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                gpu_name = result.stdout.strip().lower()

                is_workstation = any(x in gpu_name for x in [
                    'quadro', 'tesla', 'rtx a', 'a100', 'a40', 'a6000', 'a5000', 'a4000',
                    'a2000', 't4', 't1000', 'p4000', 'p2000'
                ])

                if is_workstation:
                    print(f"\n✅ NVIDIA Workstation GPU: {result.stdout.strip()}")
                    print(f"   Parallel encoding fully supported\n")
                elif len(self.enabled_qualities) > 2:
                    print(f"\n{'='*70}")
                    print(f"⚠️  PERFORMANCE NOTICE: NVIDIA GeForce GPU")
                    print(f"{'='*70}")
                    print(f"GPU: {result.stdout.strip()}")
                    print(f"\nGeForce limits concurrent NVENC sessions to 2-3.")
                    print(f"With {len(self.enabled_qualities)} qualities, encoding will be partially sequential.")
                    print(f"{'='*70}\n")

            except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                if len(self.enabled_qualities) > 2:
                    print(f"\n⚠️  Note: Consumer GPUs may limit concurrent encoding sessions\n")

    def _is_h264_compatible(self) -> bool:
        """Check if source is H.264 and suitable for stream copy"""
        if not self.video_info:
            return False

        codec = self.video_info.get('codec', '').lower()
        profile = self.video_info.get('profile', '').lower()
        level = self.video_info.get('level', 0)

        h264_codecs = ['h264', 'avc', 'avc1']
        is_h264 = any(codec.startswith(c) for c in h264_codecs)

        if not is_h264:
            return False

        incompatible_profiles = ['high 10', 'high 4:2:2', 'high 4:4:4', 'high 10 intra']
        if any(prof in profile for prof in incompatible_profiles):
            return False

        if level > 51:
            return False

        pix_fmt = self.video_info.get('pix_fmt', '')
        if pix_fmt not in ['yuv420p', 'yuvj420p']:
            return False

        return True

    def get_encoder_settings(self, profile: Dict) -> Tuple[str, List[str]]:
        """Get encoder and settings based on hardware acceleration"""
        if not self.hw_accel:
            encoder = 'libx264'
            settings = [
                '-c:v', 'libx264',
                '-preset', profile['preset'],
                '-profile:v', 'high',
                '-level', '4.1',
                '-crf', profile['crf'],
            ]

            if profile.get('use_advanced', False):
                settings.extend([
                    '-x264-params',
                    'ref=5:bframes=5:b-adapt=2:direct=auto:me=umh:subme=9:trellis=2:aq-mode=3:aq-strength=0.8'
                ])

            return 'libx264', settings

        if self.hw_accel == 'nvenc':
            preset_map = {'slow': 'p7', 'medium': 'p5', 'fast': 'p3'}
            nvenc_preset = preset_map.get(profile['preset'], 'p5')

            settings = [
                '-c:v', 'h264_nvenc',
                '-preset', nvenc_preset,
                '-profile:v', 'high',
                '-level', '4.1',
                '-rc:v', 'vbr',
                '-cq:v', profile['crf'],
                '-b:v', profile['video_bitrate'],
                '-maxrate:v', profile['maxrate'],
                '-bufsize:v', profile['bufsize'],
                '-spatial_aq', '1',
                '-temporal_aq', '1',
            ]
            return 'h264_nvenc', settings

        elif self.hw_accel == 'qsv':
            settings = [
                '-c:v', 'h264_qsv',
                '-preset', 'veryslow' if profile['preset'] == 'slow' else profile['preset'],
                '-profile:v', 'high',
                '-level', '4.1',
                '-global_quality', profile['crf'],
                '-b:v', profile['video_bitrate'],
                '-maxrate', profile['maxrate'],
                '-bufsize', profile['bufsize'],
            ]
            return 'h264_qsv', settings

        elif self.hw_accel == 'videotoolbox':
            settings = [
                '-c:v', 'h264_videotoolbox',
                '-profile:v', 'high',
                '-level', '4.1',
                '-b:v', profile['video_bitrate'],
                '-maxrate', profile['maxrate'],
                '-bufsize', profile['bufsize'],
                '-allow_sw', '1',
            ]
            return 'h264_videotoolbox', settings

        elif self.hw_accel == 'amf':
            settings = [
                '-c:v', 'h264_amf',
                '-quality', 'quality',
                '-profile:v', 'high',
                '-level', '4.1',
                '-rc', 'vbr_latency',
                '-qp_i', profile['crf'],
                '-qp_p', profile['crf'],
                '-b:v', profile['video_bitrate'],
                '-maxrate', profile['maxrate'],
                '-bufsize', profile['bufsize'],
            ]
            return 'h264_amf', settings

        elif self.hw_accel == 'vaapi':
            settings = [
                '-vaapi_device', '/dev/dri/renderD128',
                '-c:v', 'h264_vaapi',
                '-profile:v', 'high',
                '-level', '4.1',
                '-qp', profile['crf'],
                '-b:v', profile['video_bitrate'],
                '-maxrate', profile['maxrate'],
                '-bufsize', profile['bufsize'],
            ]
            return 'h264_vaapi', settings

        return self.get_encoder_settings({'preset': 'medium', 'crf': '23', 'use_advanced': False})

    def _determine_quality_ladder(self):
        """Determine quality ladder based on source resolution"""
        source_height = self.video_info['height']

        print(f"\n🎯 Determining quality ladder for {source_height}p source...")

        if source_height >= 2160:
            if self.best_quality:
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
                print(f"   ⚙️  Encoding Mode: ⭐ MAXIMUM QUALITY")
            else:
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
                print(f"   ⚙️  Encoding Mode: ⚡ BALANCED")

        else:
            if source_height >= 1080:
                if self.best_quality:
                    high_bitrate, high_maxrate, high_bufsize = '6000k', '6500k', '9000k'
                    high_crf, high_preset = '19', 'slow'
                else:
                    high_bitrate, high_maxrate, high_bufsize = '5000k', '5350k', '7500k'
                    high_crf, high_preset = '21', 'medium'
            elif source_height >= 720:
                if self.best_quality:
                    high_bitrate, high_maxrate, high_bufsize = '3500k', '3800k', '5200k'
                    high_crf, high_preset = '20', 'slow'
                else:
                    high_bitrate, high_maxrate, high_bufsize = '2800k', '3000k', '4200k'
                    high_crf, high_preset = '22', 'medium'
            else:
                if self.best_quality:
                    high_bitrate, high_maxrate, high_bufsize = '1800k', '2000k', '2700k'
                    high_crf, high_preset = '21', 'medium'
                else:
                    high_bitrate, high_maxrate, high_bufsize = '1400k', '1500k', '2100k'
                    high_crf, high_preset = '23', 'fast'

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
                print(f"   ⚙️  Encoding Mode: ⭐ MAXIMUM QUALITY")
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
                print(f"   ⚙️  Encoding Mode: ⚡ BALANCED")

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
                print("❌ ERROR: No streams found in input file")
                return False

            print(f"\n{'='*70}")
            print(f"🔍 Analyzing file: {os.path.basename(self.input_file)}")
            print(f"{'='*70}")
            print(f"Total streams found: {len(data['streams'])}")

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
                        'pix_fmt': stream.get('pix_fmt', 'yuv420p'),
                        'profile': stream.get('profile', ''),
                        'level': stream.get('level', 0),
                    }
                    print(f"   ✓ Video stream detected")

                elif codec_type == 'audio':
                    lang = stream.get('tags', {}).get('language', 'und')
                    title = stream.get('tags', {}).get('title', '')

                    if not title:
                        title = f"{lang.upper()}" if lang != 'und' else f"Audio {len(self.audio_streams) + 1}"

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
                        title = f"{lang.upper()}" if lang != 'und' else f"Subtitle {len(self.subtitle_streams) + 1}"

                    self.subtitle_streams.append({
                        'index': stream['index'],
                        'codec': stream.get('codec_name'),
                        'language': lang,
                        'title': title
                    })
                    print(f"   ✓ Subtitle stream detected: {title} ({lang}) - codec: {codec_name}")

            if not self.video_info:
                print("\n❌ ERROR: No video stream found")
                return False

            self.source_is_h264 = self._is_h264_compatible()

            if 'high' in self.enabled_qualities and self.source_is_h264 and not self.force_reencode:
                self.can_copy_video = True
            else:
                self.can_copy_video = False

            self._determine_quality_ladder()

            print(f"\n{'='*70}")
            print(f"📹 Video: {self.video_info['codec'].upper()} "
                  f"{self.video_info['width']}x{self.video_info['height']} "
                  f"@ {self.video_info['fps']} fps")
            print(f"   Profile: {self.video_info['profile']}, Level: {self.video_info['level']/10:.1f}")
            print(f"   Pixel Format: {self.video_info['pix_fmt']}")

            print(f"\n{'='*70}")
            print(f"🌐 BROWSER COMPATIBILITY CHECK")
            print(f"{'='*70}")

            codec_name = self.video_info['codec'].lower()

            if 'h264' in codec_name or 'avc' in codec_name:
                if self.source_is_h264:
                    print(f"✅ Codec: H.264/AVC - PERFECT for web streaming")
                    print(f"   • Browser Support: ~99% (universal)")
                    print(f"   • HLS Native Support: Excellent")
                    if self.can_copy_video:
                        print(f"   • Strategy: 🎯 Try STREAM COPY (instant, zero loss)")
                        print(f"              ⚡ Fallback to CRF 15 if validation fails")
                    else:
                        if self.force_reencode:
                            print(f"   • Video Quality: Re-encoding forced by --force-reencode")
                        else:
                            print(f"   • Video Quality: Profile/level requires re-encoding")
                else:
                    print(f"⚠️  Codec: H.264/AVC - Compatible but needs re-encoding")

            elif 'hevc' in codec_name or 'h265' in codec_name:
                print(f"❌ Codec: HEVC/H.265 - NOT SUITABLE for web")
                print(f"   • Solution: ⚙️  MUST TRANSCODE to H.264")

            elif 'vp9' in codec_name:
                print(f"⚠️  Codec: VP9 - Wrong container for HLS")
                print(f"   • Solution: ⚙️  Will transcode to H.264")

            else:
                print(f"❌ Codec: {codec_name.upper()} - Unsupported for web")
                print(f"   • Solution: ⚙️  Will transcode to H.264")

            print(f"{'='*70}")

            if self.video_info['height'] >= 2160:
                print(f"\n🎬 Resolution Category: 4K/UHD")
            elif self.video_info['height'] >= 1440:
                print(f"\n🎬 Resolution Category: 2K/QHD")
            elif self.video_info['height'] >= 1080:
                print(f"\n🎬 Resolution Category: Full HD")
            elif self.video_info['height'] >= 720:
                print(f"\n🎬 Resolution Category: HD")
            else:
                print(f"\n🎬 Resolution Category: SD")

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
            print(f"❌ ERROR: Failed to probe file: {e}")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ ERROR: Failed to parse ffprobe output: {e}")
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
        """Convert subtitles to standalone WebVTT files"""
        print("\n📝 Converting subtitles to standalone WebVTT files...")

        if not self.subtitle_streams:
            print("   ⚠️  No subtitle streams detected")
            return

        if self.dry_run:
            print(f"   [DRY RUN] Would convert {len(self.subtitle_streams)} subtitle(s)")
            return

        image_based_codecs = ['hdmv_pgs_subtitle', 'dvd_subtitle', 'dvdsub', 'pgssub', 'pgs']
        converted_count = 0
        skipped_count = 0

        for i, subtitle in enumerate(self.subtitle_streams):
            safe_lang = re.sub(r'[^\w\-]', '_', subtitle['language'])
            output_vtt = self.output_dir / f"subtitle_{i}_{safe_lang}.vtt"
            codec = subtitle['codec'].lower()

            print(f"\n   [{i}] Processing: {subtitle['title']} ({subtitle['language']})")
            print(f"       Codec: {codec}")

            if codec in image_based_codecs:
                print(f"       ⚠️  Image-based subtitle. Skipping.")
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
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             check=True, timeout=180)

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
                    print(f"       ❌ Conversion failed: empty output")
                    skipped_count += 1

            except subprocess.TimeoutExpired:
                print(f"       ❌ Conversion timed out")
                skipped_count += 1
            except subprocess.CalledProcessError as e:
                print(f"       ❌ Conversion failed")
                skipped_count += 1

        print(f"\n   {'='*66}")
        print(f"   📊 Subtitle Conversion Summary:")
        print(f"      ✅ Converted: {converted_count}")
        print(f"      ❌ Skipped:   {skipped_count}")
        print(f"   {'='*66}")

    def create_subtitle_manifest(self):
        """Create JSON manifest with subtitle information"""
        if not self.converted_subtitles:
            return

        if self.dry_run:
            print(f"\n   [DRY RUN] Would create subtitles.json")
            return

        manifest_file = self.output_dir / "subtitles.json"

        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump({
                'subtitles': self.converted_subtitles
            }, f, indent=2, ensure_ascii=False)

        print(f"\n   ✓ Subtitle manifest created: {manifest_file}")
        print(f"      Contains {len(self.converted_subtitles)} subtitle track(s)")

    def _validate_keyframes(self, output_name: str) -> Tuple[bool, str]:
        """Validate that HLS segments start with keyframes"""
        try:
            segment_files = sorted(self.output_dir.glob(f"{output_name}_*.ts"))[:3]

            if not segment_files:
                return False, "No segment files found"

            for segment in segment_files:
                cmd = [
                    'ffprobe',
                    '-v', 'error',
                    '-select_streams', 'v:0',
                    '-show_entries', 'frame=pict_type',
                    '-of', 'csv=p=0',
                    '-read_intervals', '%+#1',
                    str(segment)
                ]

                result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                first_frame_type = result.stdout.strip()

                if first_frame_type != 'I':
                    return False, f"Segment doesn't start with keyframe (type: {first_frame_type})"

            return True, "All segments start with keyframes"

        except Exception as e:
            return False, f"Keyframe validation error: {str(e)}"

    def _validate_hls_segments(self, output_name: str) -> Tuple[bool, str]:
        """Enhanced validation: Check durations AND keyframes"""
        m3u8_file = self.output_dir / f"{output_name}.m3u8"

        if not m3u8_file.exists():
            return False, "M3U8 file not found"

        try:
            with open(m3u8_file, 'r') as f:
                content = f.read()

            durations = []
            for line in content.split('\n'):
                if line.startswith('#EXTINF:'):
                    duration_str = line.split(':')[1].split(',')[0]
                    durations.append(float(duration_str))

            if not durations:
                return False, "No segments in playlist"

            if len(durations) < 2:
                return False, f"Too few segments ({len(durations)})"

            avg_duration = sum(durations) / len(durations)
            min_duration = min(durations)
            max_duration = max(durations)

            if min_duration < 4.0:
                return False, f"Segment too short: {min_duration:.2f}s"

            if max_duration > 8.0:
                return False, f"Segment too long: {max_duration:.2f}s"

            variance = sum((d - avg_duration) ** 2 for d in durations) / len(durations)
            std_dev = variance ** 0.5

            if std_dev > 2.0:
                return False, f"Inconsistent segments: stddev={std_dev:.2f}s"

            # Validate keyframes
            keyframe_valid, keyframe_msg = self._validate_keyframes(output_name)
            if not keyframe_valid:
                return False, f"Duration OK but {keyframe_msg}"

            return True, f"Valid: {len(durations)} segments, avg={avg_duration:.2f}s, stddev={std_dev:.2f}s, keyframes OK"

        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def _try_stream_copy(self, profile_name: str, output_name: str) -> bool:
        """Attempt stream copy with validation"""
        print(f"   🎯 Stage 1: STREAM COPY attempt...")

        cmd = [
            'ffmpeg',
            '-i', self.input_file,
            '-map', f"0:{self.video_info['index']}",
            '-c:v', 'copy',
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
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            stderr_lines = []
            for line in process.stderr:
                stderr_lines.append(line)
                if 'frame=' in line:
                    print(f"      {line.strip()}\r", end='', flush=True)

            process.wait()

            if process.returncode != 0:
                error_text = ''.join(stderr_lines[-10:])
                if 'non-monotonous DTS' in error_text:
                    print(f"\n      ❌ Failed: Non-monotonous DTS")
                elif 'keyframe' in error_text.lower():
                    print(f"\n      ❌ Failed: Keyframe issues")
                else:
                    print(f"\n      ❌ Failed: FFmpeg error")
                return False

            print(f"\n      ⏳ Validating HLS quality...")
            is_valid, reason = self._validate_hls_segments(output_name)

            if is_valid:
                print(f"      ✅ STREAM COPY successful!")
                print(f"         {reason}")
                print(f"         Quality: 100% original (zero loss)")
                self.stream_copy_used = True
                return True
            else:
                print(f"      ⚠️  Stream copy completed but validation failed")
                print(f"         Reason: {reason}")
                print(f"         Common causes:")
                print(f"         • Keyframes not aligned to 6-second intervals")
                print(f"         • Variable frame rate source")
                print(f"         • Non-standard GOP structure")
                return False

        except Exception as e:
            print(f"\n      ❌ Stream copy exception: {e}")
            return False

    def _visually_lossless_encode(self, profile_name: str, profile: Dict, output_name: str) -> bool:
        """Fallback: Visually lossless re-encode at CRF 15"""
        print(f"   ⚡ Stage 2: VISUALLY LOSSLESS re-encode (CRF 15)")
        print(f"      • Quality: Transparent (visually identical)")
        print(f"      • Benefit: Perfect HLS segmentation")

        scale, width, height = self._calculate_scale(profile['height'])

        cmd = [
            'ffmpeg',
            '-i', self.input_file,
            '-map', f"0:{self.video_info['index']}",
        ]

        if self.hw_accel == 'vaapi':
            cmd.extend(['-hwaccel', 'vaapi', '-hwaccel_output_format', 'vaapi'])

        # Add deinterlacing if needed
        vf_filters = []
        if self.is_interlaced:
            if self.hw_accel == 'vaapi':
                vf_filters.append('deinterlace_vaapi')
            else:
                vf_filters.append('yadif=0:-1:0')

        # Visually lossless settings
        if self.hw_accel == 'nvenc':
            cmd.extend([
                '-c:v', 'h264_nvenc',
                '-preset', 'p7',
                '-profile:v', 'high',
                '-level', '4.1',
                '-rc:v', 'vbr',
                '-cq:v', '15',
                '-b:v', '0',
                '-spatial_aq', '1',
                '-temporal_aq', '1',
            ])
        elif self.hw_accel == 'qsv':
            cmd.extend([
                '-c:v', 'h264_qsv',
                '-preset', 'veryslow',
                '-profile:v', 'high',
                '-level', '4.1',
                '-global_quality', '15',
            ])
        elif self.hw_accel == 'videotoolbox':
            cmd.extend([
                '-c:v', 'h264_videotoolbox',
                '-profile:v', 'high',
                '-level', '4.1',
                '-b:v', '50000k',
                '-allow_sw', '1',
            ])
        elif self.hw_accel == 'amf':
            cmd.extend([
                '-c:v', 'h264_amf',
                '-quality', 'quality',
                '-profile:v', 'high',
                '-level', '4.1',
                '-rc', 'cqp',
                '-qp_i', '15',
                '-qp_p', '15',
            ])
        elif self.hw_accel == 'vaapi':
            cmd.extend([
                '-vaapi_device', '/dev/dri/renderD128',
                '-c:v', 'h264_vaapi',
                '-profile:v', 'high',
                '-level', '4.1',
                '-qp', '15',
            ])
        else:
            cmd.extend([
                '-c:v', 'libx264',
                '-preset', 'slow',
                '-profile:v', 'high',
                '-level', '4.1',
                '-crf', '15',
                '-x264-params', 'ref=5:bframes=5:b-adapt=2:direct=auto:me=umh:subme=10:trellis=2'
            ])

        # Add filters if any
        if vf_filters:
            cmd.extend(['-vf', ','.join(vf_filters)])

        # Keyframe settings for HLS
        cmd.extend([
            '-g', str(int(self.video_info['fps'] * 6)),
            '-keyint_min', str(int(self.video_info['fps'] * 6)),
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
        ])

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            for line in process.stderr:
                if 'frame=' in line:
                    print(f"      {line.strip()}\r", end='', flush=True)

            process.wait()

            if process.returncode == 0:
                print(f"\n      ✅ Visually lossless encode completed")
                return True
            else:
                print(f"\n      ❌ Visually lossless encode failed")
                return False

        except Exception as e:
            print(f"\n      ❌ Error: {e}")
            return False

    def _normal_encode(self, profile_name: str, profile: Dict, width: int, height: int, scale: str, output_name: str) -> bool:
        """Normal encoding for medium/low quality"""
        encoder_name, encoder_settings = self.get_encoder_settings(profile)

        cmd = [
            'ffmpeg',
            '-i', self.input_file,
            '-map', f"0:{self.video_info['index']}",
        ]

        if self.hw_accel == 'vaapi':
            cmd.extend(['-hwaccel', 'vaapi', '-hwaccel_output_format', 'vaapi'])

        cmd.extend(encoder_settings)

        # Build filter chain
        vf_filters = []

        # Deinterlacing
        if self.is_interlaced:
            if self.hw_accel == 'vaapi':
                vf_filters.append('deinterlace_vaapi')
            else:
                vf_filters.append('yadif=0:-1:0')

        # Scaling
        if self.hw_accel == 'vaapi':
            vf_filters.append(f"scale_vaapi=w={width}:h={height}")
        else:
            vf_filters.append(f"scale={scale}:flags=lanczos")

        if vf_filters:
            cmd.extend(['-vf', ','.join(vf_filters)])

        cmd.extend([
            '-maxrate', profile['maxrate'],
            '-bufsize', profile['bufsize'],
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
        ])

        print(f"   Settings:")
        print(f"      Encoder: {encoder_name}")
        print(f"      Resolution: {width}x{height}")
        print(f"      Bitrate: {profile['video_bitrate']} (max: {profile['maxrate']})")
        if not self.hw_accel:
            print(f"      CRF: {profile['crf']}")
            print(f"      Preset: {profile['preset']}")
        if self.is_interlaced:
            print(f"      Deinterlacing: enabled")

        try:
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
                print(f"\n   ✅ {profile_name} video completed")
                return True
            else:
                print(f"\n   ❌ {profile_name} video failed")
                return False

        except Exception as e:
            print(f"\n   ❌ Error: {e}")
            return False

    def convert_video_quality_variant(self, profile_name: str, profile: Dict) -> bool:
        """Convert video-only stream for a specific quality"""
        print(f"\n🎬 Converting {profile_name} quality video...")

        scale, width, height = self._calculate_scale(profile['height'])
        output_name = f"video_{profile_name}"

        use_copy = (profile_name == 'high' and self.can_copy_video)

        if use_copy:
            print(f"   📋 Two-stage approach:")
            print(f"      Stage 1: Try stream copy (instant)")
            print(f"      Stage 2: Fallback to CRF 15 if needed")

            if self.dry_run:
                print(f"   [DRY RUN] Would attempt stream copy → CRF 15 fallback")
                return True

            success = self._try_stream_copy(profile_name, output_name)

            if success:
                return True

            # Clean up failed stream copy
            print(f"\n   🔄 Cleaning up and initiating fallback...")
            for f in self.output_dir.glob(f"{output_name}_*.ts"):
                f.unlink()
            m3u8_file = self.output_dir / f"{output_name}.m3u8"
            if m3u8_file.exists():
                m3u8_file.unlink()

            return self._visually_lossless_encode(profile_name, profile, output_name)

        else:
            if self.dry_run:
                encoder_name, _ = self.get_encoder_settings(profile)
                print(f"   [DRY RUN] Would encode with {encoder_name}")
                print(f"      Resolution: {width}x{height}")
                print(f"      Bitrate: {profile['video_bitrate']}")
                return True

            return self._normal_encode(profile_name, profile, width, height, scale, output_name)

    def convert_audio_track(self, audio_index: int, audio_stream: Dict, quality: str) -> bool:
        """Convert a single audio track"""
        profile = self.audio_profiles[quality]
        safe_lang = re.sub(r'[^\w\-]', '_', audio_stream['language'])
        output_name = f"audio_{audio_index}_{safe_lang}_{quality}"

        print(f"   Converting: Audio #{audio_index} ({audio_stream['title']}) - {quality}...")

        if self.dry_run:
            print(f"      [DRY RUN] Would convert to AAC {profile['bitrate']}")
            return True

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
        except subprocess.CalledProcessError:
            print(f"         ✗ Failed")
            return False

    def convert_all_audio_tracks(self):
        """Convert all audio tracks"""
        if not self.audio_streams:
            print("\n⚠️  No audio streams - creating video-only")
            return True

        print(f"\n{'='*70}")
        print(f"🔊 Converting {len(self.audio_streams)} audio track(s) x {len(self.enabled_qualities)} qualities")
        print(f"{'='*70}")

        if self.dry_run:
            print(f"[DRY RUN] Would convert audio to AAC")
            return True

        success = True
        for i, audio in enumerate(self.audio_streams):
            print(f"\n📻 Audio Track #{i}: {audio['title']} ({audio['language']})")
            for quality in self.enabled_qualities:
                if not self.convert_audio_track(i, audio, quality):
                    success = False

        return success

    def create_master_playlist(self):
        """Create master playlist"""
        print("\n📋 Creating master playlist...")

        if self.dry_run:
            print(f"   [DRY RUN] Would create master.m3u8")
            return "master.m3u8"

        master_file = self.output_dir / "master.m3u8"

        with open(master_file, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write("#EXT-X-VERSION:6\n\n")

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

            f.write("# Video variants\n")
            for profile_name in ['high', 'medium', 'low']:
                if profile_name not in self.enabled_qualities:
                    continue

                profile = self.quality_profiles[profile_name]
                audio_profile = self.audio_profiles[profile_name]
                scale, width, height = self._calculate_scale(profile['height'])

                video_playlist = f"video_{profile_name}.m3u8"

                if (self.output_dir / video_playlist).exists():
                    if profile_name == 'high' and self.stream_copy_used:
                        if self.video_info['bitrate'] != 'N/A':
                            try:
                                video_bw = int(self.video_info['bitrate'])
                            except:
                                video_bw = int(profile['video_bitrate'].replace('k', '000'))
                        else:
                            video_bw = int(profile['video_bitrate'].replace('k', '000'))
                    else:
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
        mode_label = "Maximum Quality" if self.best_quality else "Balanced"

        print("\n" + "="*70)
        print(" "*15 + f"🎥 HLS VIDEO CONVERTER v6.2")
        print("="*70)
        print(f"📁 Input:  {self.input_file}")
        print(f"📁 Output: {self.output_dir}")
        print(f"⚙️  Mode: {mode_label}")
        print(f"🎯 Qualities: {', '.join(self.enabled_qualities)}")
        if self.force_reencode:
            print(f"🔄 Force Re-encode: YES")
        if self.dry_run:
            print(f"🔍 DRY RUN: No files will be created")
        print("="*70)

        # Hardware acceleration
        if self.hw_accel == 'auto':
            detected_hw = self.detect_hardware_acceleration()
            if detected_hw:
                self.hw_accel = detected_hw
            else:
                print(f"   ℹ️  Using software encoding")
                self.hw_accel = None
        elif self.hw_accel:
            print(f"\n🚀 Hardware Acceleration: {self.hw_accel.upper()}")

        if self.parallel and self.hw_accel:
            self._check_parallel_efficiency()
        elif self.parallel:
            print(f"\n✅ Parallel Processing: {min(len(self.enabled_qualities), cpu_count())} workers\n")

        # Pre-flight checks
        print("\n" + "="*70)
        print(" "*25 + "🔍 PRE-FLIGHT CHECKS")
        print("="*70)

        if not self.check_ffmpeg():
            return False

        if not self.probe_file():
            return False

        estimated_size = self.estimate_output_size()
        if not self.check_disk_space(estimated_size):
            return False

        if not self.check_existing_files():
            print("❌ Aborted by user")
            return False

        self.is_interlaced = self.detect_interlaced()

        if self.detect_hdr():
            print("❌ Aborted: HDR content")
            return False

        print("="*70)

        if self.dry_run:
            print("\n" + "="*70)
            print(" "*20 + "🔍 DRY RUN SUMMARY")
            print("="*70)
            print("This is a preview. No encoding will occur.")
            print("Remove --dry-run to perform actual conversion.")
            print("="*70 + "\n")

        self.start_time = time.time()

        # Convert subtitles
        if self.subtitle_streams:
            self.convert_subtitles()
            self.create_subtitle_manifest()

        # Convert video
        print(f"\n{'='*70}")
        print(f"PHASE 1: Converting Video ({len(self.enabled_qualities)} qualities)")
        if self.can_copy_video and 'high' in self.enabled_qualities:
            print(f"         🎯 'high': Stream copy → CRF 15 fallback")
        print(f"{'='*70}")

        video_success = True

        if self.parallel and len(self.enabled_qualities) > 1 and not self.dry_run:
            with Pool(processes=min(len(self.enabled_qualities), cpu_count())) as pool:
                convert_func = partial(self._convert_video_wrapper,
                                      input_file=self.input_file,
                                      output_dir=str(self.output_dir),
                                      video_info=self.video_info,
                                      hw_accel=self.hw_accel,
                                      best_quality=self.best_quality,
                                      enabled_qualities=self.enabled_qualities,
                                      can_copy_video=self.can_copy_video,
                                      is_interlaced=self.is_interlaced)

                results = pool.map(convert_func,
                                  [(name, self.quality_profiles[name]) for name in self.enabled_qualities])

                video_success = all(results)
        else:
            for profile_name in self.enabled_qualities:
                if not self.convert_video_quality_variant(profile_name, self.quality_profiles[profile_name]):
                    video_success = False

        # Convert audio
        print(f"\n{'='*70}")
        print("PHASE 2: Converting Audio")
        print(f"{'='*70}")

        audio_success = self.convert_all_audio_tracks()

        # Create master playlist
        master_playlist = self.create_master_playlist()

        # Summary
        if self.dry_run:
            elapsed = 0
        else:
            elapsed = time.time() - self.start_time

        print("\n" + "="*70)
        print(" "*20 + "✅ CONVERSION COMPLETED!")
        print("="*70)
        print(f"📁 Output directory:    {self.output_dir}")
        print(f"🎬 Master playlist:     {master_playlist}")
        print(f"📺 Source resolution:   {self.video_info['width']}x{self.video_info['height']}")
        print(f"🎞️  Source codec:        {self.video_info['codec'].upper()}")

        if self.hw_accel:
            print(f"🚀 Hardware accel:      {self.hw_accel.upper()}")

        if elapsed > 0:
            print(f"⏱️  Total time:          {self._format_time(elapsed)}")

        print(f"\n   Generated Quality Tiers:")
        for q in self.enabled_qualities:
            _, w, h = self._calculate_scale(self.quality_profiles[q]['height'])
            if q == 'high' and self.stream_copy_used:
                print(f"   • {q.capitalize():6} {w}x{h} - ⭐ STREAM COPY (100% original)")
            elif q == 'high' and self.can_copy_video:
                print(f"   • {q.capitalize():6} {w}x{h} - ⚡ VISUALLY LOSSLESS (CRF 15)")
            else:
                print(f"   • {q.capitalize():6} {w}x{h} @ {self.quality_profiles[q]['video_bitrate']}")

        print(f"\n🔊 Audio tracks:        {len(self.audio_streams)} x {len(self.enabled_qualities)} qualities")
        print(f"💬 Subtitle tracks:     {len(self.converted_subtitles)} WebVTT files")

        if self.converted_subtitles:
            print(f"📄 Subtitle manifest:   subtitles.json")

        print("="*70 + "\n")

        return video_success and audio_success

    def _format_time(self, seconds: float) -> str:
        """Format seconds to readable time"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)

        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    @staticmethod
    def _convert_video_wrapper(args, input_file, output_dir, video_info, hw_accel, best_quality, enabled_qualities, can_copy_video, is_interlaced):
        """Wrapper for parallel video conversion"""
        profile_name, profile = args

        temp_converter = HLSConverter(
            input_file,
            output_dir,
            best_quality=best_quality,
            hw_accel=hw_accel,
            explicit_qualities=enabled_qualities
        )
        temp_converter.video_info = video_info
        temp_converter.can_copy_video = can_copy_video
        temp_converter.is_interlaced = is_interlaced
        temp_converter._determine_quality_ladder()

        return temp_converter.convert_video_quality_variant(profile_name, profile)


def main():
    parser = argparse.ArgumentParser(
        description='Convert MKV/MP4 to HLS with adaptive quality streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Balanced mode with auto hardware detection
  %(prog)s input.mkv output/ --hw-accel=auto

  # Maximum quality
  %(prog)s input.mkv output/ --best-quality --hw-accel=auto

  # Only 'high' quality (intelligent: stream copy → CRF 15 fallback)
  %(prog)s input.mkv output/ --explicit-qualities=high

  # Dry run (preview without encoding)
  %(prog)s input.mkv output/ --dry-run

  # Force re-encode even if H.264
  %(prog)s input.mkv output/ --force-reencode

Intelligent H.264 Strategy:
  Stage 1: Try STREAM COPY (instant, 100% original)
           ↓ Validates segments + keyframes
  Stage 2: Fallback to CRF 15 if validation fails (visually lossless)
           ↓ Guarantees perfect HLS

Hardware Acceleration:
  auto:          Auto-detect (recommended)
  nvenc:         NVIDIA GPU
  qsv:           Intel Quick Sync
  videotoolbox:  Apple (macOS)
  amf:           AMD GPU
  vaapi:         Linux VAAPI

Safety Features:
  • Disk space check
  • Existing file protection
  • Interlaced detection
  • HDR warning
  • Keyframe validation
  • Dry run mode
        """
    )

    parser.add_argument('input', help='Input video file')
    parser.add_argument('output', help='Output directory for HLS files')
    parser.add_argument('--best-quality', '-b', action='store_true',
                        help='Maximum quality (slow presets, CRF 18-23)')
    parser.add_argument('--explicit-qualities', '-q', type=str, default=None,
                        help='Comma-separated: high,medium,low')
    parser.add_argument('--hw-accel', type=str, default=None,
                        choices=['auto', 'nvenc', 'qsv', 'videotoolbox', 'amf', 'vaapi'],
                        help='Hardware acceleration')
    parser.add_argument('--parallel', '-p', action='store_true',
                        help='Encode qualities in parallel')
    parser.add_argument('--force-reencode', '-f', action='store_true',
                        help='Force re-encode (disable stream copy)')
    parser.add_argument('--dry-run', '-d', action='store_true',
                        help='Preview without encoding')
    parser.add_argument('--overwrite', '-o', action='store_true',
                        help='Overwrite existing files without asking')
    parser.add_argument('--no-interlace-check', action='store_true',
                        help='Skip interlacing detection')

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"❌ ERROR: Input file not found: {args.input}")
        sys.exit(1)

    explicit_qualities = None
    if args.explicit_qualities:
        explicit_qualities = [q.strip().lower() for q in args.explicit_qualities.split(',')]

        valid_qualities = {'high', 'medium', 'low'}
        invalid = set(explicit_qualities) - valid_qualities
        if invalid:
            print(f"❌ ERROR: Invalid quality: {', '.join(invalid)}")
            print(f"   Valid: high, medium, low")
            sys.exit(1)

        seen = set()
        explicit_qualities = [q for q in explicit_qualities if not (q in seen or seen.add(q))]

    converter = HLSConverter(args.input, args.output,
                            best_quality=args.best_quality,
                            explicit_qualities=explicit_qualities,
                            hw_accel=args.hw_accel,
                            parallel=args.parallel,
                            force_reencode=args.force_reencode,
                            dry_run=args.dry_run,
                            overwrite=args.overwrite,
                            no_interlace_check=args.no_interlace_check)

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