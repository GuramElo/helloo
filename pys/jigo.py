#!/usr/bin/env python3
"""
HLS Video Converter with Stable Subtitle Support
Converts MKV/MP4 files to HLS format with multiple quality variants,
separate audio tracks, and native browser-compatible subtitles.

VERSION 6.0: Stream copy for H.264 sources + codec compatibility detection
"""

import os
import sys
import json
import subprocess
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import re
from multiprocessing import Pool, cpu_count
from functools import partial

class HLSConverter:
    def __init__(self, input_file: str, output_dir: str, best_quality: bool = False,
                 explicit_qualities: List[str] = None, hw_accel: Optional[str] = None,
                 parallel: bool = False, force_reencode: bool = False):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.best_quality = best_quality
        self.hw_accel = hw_accel
        self.parallel = parallel
        self.force_reencode = force_reencode

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
        self.source_is_h264 = False
        self.can_copy_video = False

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

    def detect_hardware_acceleration(self) -> Optional[str]:
        """Auto-detect available hardware acceleration"""
        print("\n🔍 Detecting hardware acceleration capabilities...")

        hw_encoders = {
            'nvenc': 'h264_nvenc',      # NVIDIA
            'qsv': 'h264_qsv',           # Intel Quick Sync
            'videotoolbox': 'h264_videotoolbox',  # Apple
            'amf': 'h264_amf',           # AMD
            'vaapi': 'h264_vaapi',       # Linux VAAPI
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
                print(f"   ⚠️  No hardware acceleration detected")
                return None

            # Priority order: NVENC > Quick Sync > VideoToolbox > AMF > VAAPI
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
        """Warn if parallel processing won't help much with current hardware"""

        if self.hw_accel == 'nvenc':
            # Check if it's a consumer GeForce card
            try:
                result = subprocess.run(
                    ['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                gpu_name = result.stdout.strip().lower()

                # Workstation GPUs support unlimited sessions
                is_workstation = any(x in gpu_name for x in [
                    'quadro', 'tesla', 'rtx a', 'a100', 'a40', 'a6000', 'a5000', 'a4000',
                    'a2000', 't4', 't1000', 'p4000', 'p2000'
                ])

                if is_workstation:
                    print(f"\n✅ NVIDIA Workstation GPU detected: {result.stdout.strip()}")
                    print(f"   Parallel encoding fully supported (unlimited NVENC sessions)\n")
                elif len(self.enabled_qualities) > 2:
                    print(f"\n{'='*70}")
                    print(f"⚠️  PERFORMANCE NOTICE: NVIDIA GeForce GPU Detected")
                    print(f"{'='*70}")
                    print(f"GPU: {result.stdout.strip()}")
                    print(f"\nGeForce cards limit NVENC to 2-3 concurrent encoding sessions.")
                    print(f"With {len(self.enabled_qualities)} qualities, encoding will be partially sequential.")
                    print(f"\n💡 For maximum parallel speedup, consider:")
                    print(f"   • --explicit-qualities=high,medium (only 2 qualities)")
                    print(f"   • Or continue anyway (still faster than pure CPU!)")
                    print(f"\n⏱️  Expected behavior:")
                    print(f"   • 2 qualities will encode simultaneously")
                    print(f"   • Remaining qualities will queue and encode when slots free")
                    print(f"{'='*70}\n")

            except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                # Can't detect specific GPU, show generic warning
                if len(self.enabled_qualities) > 2:
                    print(f"\n⚠️  Note: NVIDIA consumer GPUs typically limit concurrent NVENC sessions to 2-3")
                    print(f"   With {len(self.enabled_qualities)} qualities, parallel speedup may be limited\n")

        elif self.hw_accel == 'videotoolbox':
            if len(self.enabled_qualities) > 2:
                print(f"\n{'='*70}")
                print(f"⚠️  PERFORMANCE NOTICE: VideoToolbox Limitations")
                print(f"{'='*70}")
                print(f"VideoToolbox typically limits concurrent encoding sessions to 2-3.")
                print(f"\n💡 For better performance, consider:")
                print(f"   • --explicit-qualities=high,medium (only 2 qualities)")
                print(f"   • Remove --parallel (sequential may be more efficient)")
                print(f"{'='*70}\n")

        elif self.hw_accel == 'vaapi':
            print(f"\n💡 VAAPI parallel performance varies by GPU model")
            print(f"   Monitor system resources during encoding\n")

        elif self.hw_accel in ['qsv', 'amf']:
            print(f"\n✅ {self.hw_accel.upper()} typically supports {len(self.enabled_qualities)} concurrent sessions well\n")

    def _is_h264_compatible(self) -> bool:
        """
        Check if source video is H.264/AVC and suitable for stream copy.
        Returns True if we can safely use -c:v copy for the high quality variant.
        """
        if not self.video_info:
            return False

        codec = self.video_info.get('codec', '').lower()
        profile = self.video_info.get('profile', '').lower()
        level = self.video_info.get('level', 0)

        # Check if it's H.264
        h264_codecs = ['h264', 'avc', 'avc1']
        is_h264 = any(codec.startswith(c) for c in h264_codecs)

        if not is_h264:
            return False

        # Check profile compatibility (High profile or below is good)
        # High 10, High 4:2:2, High 4:4:4 might have limited browser support
        incompatible_profiles = ['high 10', 'high 4:2:2', 'high 4:4:4', 'high 10 intra']
        if any(prof in profile for prof in incompatible_profiles):
            return False

        # Check level (4.1 and below is universally supported)
        # Level above 5.1 might have issues on some devices
        if level > 51:  # 5.1
            return False

        # Check pixel format
        pix_fmt = self.video_info.get('pix_fmt', '')
        if pix_fmt not in ['yuv420p', 'yuvj420p']:
            return False

        return True

    def get_encoder_settings(self, profile: Dict) -> Tuple[str, List[str]]:
        """Get encoder and its settings based on hardware acceleration"""
        if not self.hw_accel:
            # Software encoding (libx264)
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

        # Hardware encoding
        if self.hw_accel == 'nvenc':
            # NVIDIA NVENC
            preset_map = {
                'slow': 'p7',      # Highest quality
                'medium': 'p5',    # Balanced
                'fast': 'p3'       # Fast
            }
            nvenc_preset = preset_map.get(profile['preset'], 'p5')

            settings = [
                '-c:v', 'h264_nvenc',
                '-preset', nvenc_preset,
                '-profile:v', 'high',
                '-level', '4.1',
                '-rc:v', 'vbr',
                '-cq:v', profile['crf'],  # Quality level
                '-b:v', profile['video_bitrate'],
                '-maxrate:v', profile['maxrate'],
                '-bufsize:v', profile['bufsize'],
                '-spatial_aq', '1',
                '-temporal_aq', '1',
            ]
            return 'h264_nvenc', settings

        elif self.hw_accel == 'qsv':
            # Intel Quick Sync
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
            # Apple VideoToolbox
            settings = [
                '-c:v', 'h264_videotoolbox',
                '-profile:v', 'high',
                '-level', '4.1',
                '-b:v', profile['video_bitrate'],
                '-maxrate', profile['maxrate'],
                '-bufsize', profile['bufsize'],
                '-allow_sw', '1',  # Allow software fallback
            ]
            return 'h264_videotoolbox', settings

        elif self.hw_accel == 'amf':
            # AMD AMF
            settings = [
                '-c:v', 'h264_amf',
                '-quality', 'quality',  # quality/balanced/speed
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
            # Linux VAAPI
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

        # Fallback to software
        return self.get_encoder_settings({'preset': 'medium', 'crf': '23', 'use_advanced': False})

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
                        'pix_fmt': stream.get('pix_fmt', 'yuv420p'),
                        'profile': stream.get('profile', ''),
                        'level': stream.get('level', 0),
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

            # Check if source is H.264 and compatible for stream copy
            self.source_is_h264 = self._is_h264_compatible()

            # Determine if we can use stream copy for "high" quality
            if 'high' in self.enabled_qualities and self.source_is_h264 and not self.force_reencode:
                self.can_copy_video = True
            else:
                self.can_copy_video = False

            # Determine quality ladder based on source resolution
            self._determine_quality_ladder()

            print(f"\n{'='*70}")
            print(f"📹 Video: {self.video_info['codec'].upper()} "
                  f"{self.video_info['width']}x{self.video_info['height']} "
                  f"@ {self.video_info['fps']} fps")
            print(f"   Profile: {self.video_info['profile']}, Level: {self.video_info['level']/10:.1f}")
            print(f"   Pixel Format: {self.video_info['pix_fmt']}")

            # Show codec compatibility status
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
                        print(f"   • Video Quality: 🎯 'high' quality will use STREAM COPY")
                        print(f"                    ⭐ ZERO quality loss - original preserved!")
                    else:
                        if self.force_reencode:
                            print(f"   • Video Quality: Re-encoding forced by --force-reencode flag")
                        else:
                            print(f"   • Video Quality: Profile/level requires re-encoding")
                else:
                    print(f"⚠️  Codec: H.264/AVC - Compatible but needs re-encoding")
                    profile = self.video_info['profile'].lower()
                    if 'high 10' in profile or 'high 4:2:2' in profile or 'high 4:4:4' in profile:
                        print(f"   • Reason: Profile '{self.video_info['profile']}' has limited browser support")
                        print(f"   • Solution: Will re-encode to High Profile (universally supported)")
                    elif self.video_info['level'] > 51:
                        print(f"   • Reason: Level {self.video_info['level']/10:.1f} may have device compatibility issues")
                        print(f"   • Solution: Will re-encode to Level 4.1 (universally supported)")
                    elif self.video_info['pix_fmt'] not in ['yuv420p', 'yuvj420p']:
                        print(f"   • Reason: Pixel format '{self.video_info['pix_fmt']}' not universally supported")
                        print(f"   • Solution: Will re-encode to yuv420p")

            elif 'hevc' in codec_name or 'h265' in codec_name:
                print(f"❌ Codec: HEVC/H.265 - NOT SUITABLE for web streaming")
                print(f"   • Chrome/Firefox/Edge: ❌ No support")
                print(f"   • Safari (Apple devices): ⚠️  Limited support")
                print(f"   • Solution: ⚙️  MUST TRANSCODE to H.264")
                print(f"   • Impact: Quality loss inevitable (will use highest quality settings)")

            elif 'vp9' in codec_name:
                print(f"⚠️  Codec: VP9 - Wrong container for HLS")
                print(f"   • Browser Support: Good (90%+ for VP9 itself)")
                print(f"   • HLS Support: ❌ VP9 designed for DASH/WebM, not HLS")
                print(f"   • Solution: ⚙️  Will transcode to H.264")

            elif 'vp8' in codec_name:
                print(f"❌ Codec: VP8 - Not suitable for HLS")
                print(f"   • Solution: ⚙️  Will transcode to H.264")

            elif 'av1' in codec_name:
                print(f"⚠️  Codec: AV1 - Too early for production web")
                print(f"   • Browser Support: ~70% (Chrome/Firefox modern versions)")
                print(f"   • HLS Support: Experimental")
                print(f"   • Decoding: CPU-intensive without hardware support")
                print(f"   • Solution: ⚙️  Will transcode to H.264 for universal compatibility")

            elif 'mpeg2' in codec_name or codec_name == 'mpeg2video':
                print(f"❌ Codec: MPEG-2 - Legacy codec, no browser support")
                print(f"   • Solution: ⚙️  Will transcode to H.264")

            else:
                print(f"❌ Codec: {codec_name.upper()} - Unknown/unsupported for web")
                print(f"   • Solution: ⚙️  Will transcode to H.264")

            print(f"{'='*70}")

            # Show resolution category
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

        # Check if we should use stream copy for "high" quality
        use_copy = (profile_name == 'high' and self.can_copy_video)

        if use_copy:
            print(f"   🎯 Using STREAM COPY - preserving original video (zero quality loss)")
            cmd = [
                'ffmpeg',
                '-i', self.input_file,
                '-map', f"0:{self.video_info['index']}",
                '-c:v', 'copy',  # Stream copy!
                '-an',  # No audio
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

            # Add forced keyframes for proper HLS segmentation
            # This is needed because source might not have keyframes at ideal intervals
            cmd.insert(-4, '-force_key_frames')
            cmd.insert(-4, 'expr:gte(t,n_forced*6)')  # Force keyframe every 6 seconds (HLS segment time)

        else:
            # Normal encoding path
            encoder_name, encoder_settings = self.get_encoder_settings(profile)

            # Build encoding command
            cmd = [
                'ffmpeg',
                '-i', self.input_file,
                '-map', f"0:{self.video_info['index']}",
            ]

            # Add hardware acceleration input if using VAAPI
            if self.hw_accel == 'vaapi':
                cmd.extend(['-hwaccel', 'vaapi', '-hwaccel_output_format', 'vaapi'])

            # Add encoder settings
            cmd.extend(encoder_settings)

            # Scaling - use hardware scaling for VAAPI
            if self.hw_accel == 'vaapi':
                cmd.extend(['-vf', f"scale_vaapi=w={width}:h={height}"])
            else:
                cmd.extend(['-vf', f"scale={scale}:flags=lanczos"])

            # Add common settings
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
                if profile.get('use_advanced', False):
                    print(f"      Advanced x264: enabled")

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
                if use_copy:
                    print(f"\n   ✅ {profile_name} video completed (stream copy - original quality preserved)")
                else:
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
        print(f"   Note: Audio is always re-encoded to AAC (required for universal HLS compatibility)")
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
                    # For "high" quality with stream copy, estimate bandwidth from source
                    if profile_name == 'high' and self.can_copy_video:
                        # Try to get actual bitrate from source
                        if self.video_info['bitrate'] != 'N/A':
                            try:
                                video_bw = int(self.video_info['bitrate'])
                            except:
                                video_bw = int(profile['video_bitrate'].replace('k', '000'))
                        else:
                            # Fallback to profile bitrate
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
        mode_label = "Maximum Quality Mode" if self.best_quality else "Balanced Mode"
        print("\n" + "="*70)
        print(f" "*10 + f"🎥 HLS VIDEO CONVERTER v6.0 ({mode_label})")
        print("="*70)
        print(f"📁 Input:  {self.input_file}")
        print(f"📁 Output: {self.output_dir}")
        if self.best_quality:
            print(f"⭐ Quality: MAXIMUM (slow presets, advanced x264)")
        else:
            print(f"⚡ Quality: BALANCED (medium/fast presets)")
        print(f"🎯 Qualities: {', '.join(self.enabled_qualities)}")
        if self.force_reencode:
            print(f"⚙️  Force Re-encode: YES (even for H.264 sources)")

        # Hardware acceleration detection
        if self.hw_accel == 'auto':
            detected_hw = self.detect_hardware_acceleration()
            if detected_hw:
                self.hw_accel = detected_hw
            else:
                print(f"   ℹ️  Falling back to software encoding")
                self.hw_accel = None
        elif self.hw_accel:
            print(f"🚀 Hardware Acceleration: {self.hw_accel.upper()}")

        # Check parallel efficiency with hardware
        if self.parallel and self.hw_accel:
            self._check_parallel_efficiency()
        elif self.parallel:
            workers = min(len(self.enabled_qualities), cpu_count())
            print(f"\n✅ CPU Parallel Processing: {workers} workers (excellent for CPU encoding)\n")

        if self.parallel and not self.hw_accel:
            workers = min(len(self.enabled_qualities), cpu_count())
            print(f"⚡ Parallel Processing: {workers} workers")

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
        print(f"PHASE 1: Converting Video Streams ({len(self.enabled_qualities)} qualities)")
        if self.can_copy_video and 'high' in self.enabled_qualities:
            print(f"         🎯 'high' quality: STREAM COPY (zero quality loss)")
        if self.parallel:
            print(f"         Running in PARALLEL mode with {min(len(self.enabled_qualities), cpu_count())} workers")
        print(f"{'='*70}")

        video_success = True

        if self.parallel and len(self.enabled_qualities) > 1:
            # Parallel processing
            with Pool(processes=min(len(self.enabled_qualities), cpu_count())) as pool:
                convert_func = partial(self._convert_video_wrapper,
                                      input_file=self.input_file,
                                      output_dir=str(self.output_dir),
                                      video_info=self.video_info,
                                      hw_accel=self.hw_accel,
                                      best_quality=self.best_quality,
                                      enabled_qualities=self.enabled_qualities,
                                      can_copy_video=self.can_copy_video)

                results = pool.map(convert_func,
                                  [(name, self.quality_profiles[name]) for name in self.enabled_qualities])

                video_success = all(results)
        else:
            # Sequential processing
            for profile_name in self.enabled_qualities:
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
        quality_summary = []
        for profile_name in self.enabled_qualities:
            _, w, h = self._calculate_scale(self.quality_profiles[profile_name]['height'])

            if profile_name == 'high' and self.can_copy_video:
                quality_summary.append({
                    'name': profile_name,
                    'width': w,
                    'height': h,
                    'mode': 'COPY',
                    'note': 'Original quality preserved'
                })
            else:
                quality_summary.append({
                    'name': profile_name,
                    'width': w,
                    'height': h,
                    'bitrate': self.quality_profiles[profile_name]['video_bitrate'],
                    'crf': self.quality_profiles[profile_name]['crf'],
                    'preset': self.quality_profiles[profile_name]['preset'],
                    'mode': 'ENCODE'
                })

        audio_bitrates = '/'.join([self.audio_profiles[q]['bitrate'] for q in self.enabled_qualities])

        print("\n" + "="*70)
        print(" "*20 + "✅ CONVERSION COMPLETED!")
        print("="*70)
        print(f"📁 Output directory:    {self.output_dir}")
        print(f"🎬 Master playlist:     {master_playlist}")
        print(f"📺 Source resolution:   {self.video_info['width']}x{self.video_info['height']}")
        print(f"🎞️  Source codec:        {self.video_info['codec'].upper()}")

        if self.hw_accel:
            print(f"🚀 Hardware acceleration: {self.hw_accel.upper()}")
        if self.parallel:
            print(f"⚡ Parallel processing: ENABLED")

        print(f"\n   Generated Quality Tiers ({len(self.enabled_qualities)}):")

        for q in quality_summary:
            label = q['name'].capitalize()
            if q['mode'] == 'COPY':
                print(f"   • {label:6} {q['width']}x{q['height']} - ⭐ STREAM COPY ({q['note']})")
            else:
                print(f"   • {label:6} {q['width']}x{q['height']} @ {q['bitrate']}, "
                      f"CRF {q['crf']}, preset={q['preset']}")

        print(f"\n🔊 Audio tracks:        {len(self.audio_streams)} x {len(self.enabled_qualities)} qualities ({audio_bitrates})")
        print(f"                        Always re-encoded to AAC (HLS requirement)")
        print(f"💬 Subtitle tracks:     {len(self.converted_subtitles)} converted to WebVTT")
        if self.converted_subtitles:
            print(f"📄 Subtitle manifest:   subtitles.json")
        print("="*70)

        if self.video_info['height'] >= 2160:
            available = "4K → 1080p → 480p"
        else:
            available = "Original → 720p → 480p"

        generated = ' → '.join([self.enabled_qualities[i] for i in range(len(self.enabled_qualities))])

        print(f"\n💡 Available Qualities: {available}")
        print(f"   Generated: {generated}")

        if self.can_copy_video and 'high' in self.enabled_qualities:
            print(f"\n   ⭐ QUALITY PRESERVATION:")
            print(f"      • 'high' quality: ZERO quality loss (stream copy)")
            print(f"      • Source codec: {self.video_info['codec'].upper()} (perfect for web)")
            print(f"      • Other qualities: Re-encoded with {mode_label.lower()}")

        if self.best_quality:
            print("\n   ⭐ Maximum Quality Optimizations:")
            if self.hw_accel:
                print(f"      • Hardware encoding ({self.hw_accel.upper()}) - 5-10x faster")
            else:
                print("      • Slow encoding presets (better compression)")
            print("      • Lower CRF values (18-23) for higher quality")
            if not self.hw_accel:
                print("      • Advanced x264: ref=5, bframes=5, subme=9, trellis=2")
            print("      • Higher audio bitrates (256k/192k/128k)")
        else:
            print("\n   ⚡ Balanced Mode:")
            if self.hw_accel:
                print(f"      • Hardware encoding ({self.hw_accel.upper()}) - 5-10x faster")
            else:
                print("      • Medium/fast presets (faster encoding)")
            print("      • Balanced quality settings")

        print("      • Lanczos scaling algorithm")
        print("="*70 + "\n")

        return video_success and audio_success

    @staticmethod
    def _convert_video_wrapper(args, input_file, output_dir, video_info, hw_accel, best_quality, enabled_qualities, can_copy_video):
        """Wrapper for parallel video conversion"""
        profile_name, profile = args

        # Create a temporary converter instance for this process
        temp_converter = HLSConverter(
            input_file,
            output_dir,
            best_quality=best_quality,
            hw_accel=hw_accel,
            explicit_qualities=enabled_qualities
        )
        temp_converter.video_info = video_info
        temp_converter.can_copy_video = can_copy_video
        temp_converter._determine_quality_ladder()

        return temp_converter.convert_video_quality_variant(profile_name, profile)


def main():
    parser = argparse.ArgumentParser(
        description='Convert MKV/MP4 to HLS with adaptive quality streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Balanced mode - if source is H.264, 'high' uses stream copy (zero quality loss)
  %(prog)s input.mkv output_dir/

  # With hardware acceleration (auto-detect) - RECOMMENDED
  %(prog)s input.mkv output_dir/ --hw-accel=auto

  # Force re-encode even if source is H.264 (if you want consistent quality)
  %(prog)s input.mkv output_dir/ --force-reencode

  # Maximum quality with hardware acceleration
  %(prog)s input.mkv output_dir/ --best-quality --hw-accel=auto

  # Only generate 'high' quality with stream copy (fastest, perfect quality)
  %(prog)s input.mkv output_dir/ --explicit-qualities=high

  # HEVC/H.265 source (will be transcoded to H.264)
  %(prog)s hevc_input.mkv output_dir/ --best-quality --hw-accel=auto

Browser Codec Compatibility:
  ✅ H.264/AVC:     Universal (~99% browsers) - RECOMMENDED
                   • If source is H.264: 'high' quality uses stream copy (zero loss)
                   • If not: transcodes to H.264 with best quality settings

  ❌ HEVC/H.265:    Limited (Safari only, not Chrome/Firefox/Edge)
                   • Must transcode to H.264 - quality loss inevitable
                   • Use --best-quality to minimize loss

  ⚠️  VP9/AV1:      Wrong container (designed for DASH, not HLS)
                   • Must transcode to H.264

  ❌ Other codecs:  No browser support - must transcode

Hardware Acceleration:
  auto:          Auto-detect best available (RECOMMENDED)
  nvenc:         NVIDIA GPU (h264_nvenc) - 5-10x faster
  qsv:           Intel Quick Sync (h264_qsv) - 3-5x faster
  videotoolbox:  Apple VideoToolbox (macOS) - 3-5x faster
  amf:           AMD GPU (h264_amf) - 5-8x faster
  vaapi:         Linux VAAPI - 3-5x faster

Quality Modes:
  Balanced (default):  Medium/fast presets, CRF 20-26
  Best Quality (-b):   Slow presets, CRF 18-23, advanced x264

Stream Copy Behavior:
  • Source is H.264 compatible → 'high' quality uses stream copy (ZERO quality loss)
  • Source is other codec → transcodes all qualities to H.264
  • Use --force-reencode to always re-encode (even H.264)

Parallel Processing:
  --parallel:    Encode multiple qualities simultaneously
                 • Best for: CPU encoding (no HW accel)
                 • Good for: Workstation GPUs, Intel QSV, AMD AMF
                 • Limited: Consumer NVIDIA (GeForce), VideoToolbox

Performance Tips:
  • Fastest with perfect quality:  --explicit-qualities=high (H.264 source)
  • HEVC/H.265 source:              --best-quality --hw-accel=auto
  • Maximum web compatibility:      Always use H.264 output (automatic)
        """
    )

    parser.add_argument('input', help='Input video file (MKV or MP4)')
    parser.add_argument('output', help='Output directory for HLS files')
    parser.add_argument('--best-quality', '-b', action='store_true',
                        help='Use slowest presets and best quality settings')
    parser.add_argument('--explicit-qualities', '-q', type=str, default=None,
                        help='Comma-separated list of qualities (high,medium,low)')
    parser.add_argument('--hw-accel', type=str, default=None,
                        choices=['auto', 'nvenc', 'qsv', 'videotoolbox', 'amf', 'vaapi'],
                        help='Hardware acceleration method (auto-detect or specify)')
    parser.add_argument('--parallel', '-p', action='store_true',
                        help='Encode multiple qualities in parallel (auto-checks efficiency)')
    parser.add_argument('--force-reencode', '-f', action='store_true',
                        help='Force re-encoding even for H.264 sources (disable stream copy)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"❌ ERROR: Input file does not exist: {args.input}")
        sys.exit(1)

    # Parse explicit qualities if provided
    explicit_qualities = None
    if args.explicit_qualities:
        explicit_qualities = [q.strip().lower() for q in args.explicit_qualities.split(',')]

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
                            explicit_qualities=explicit_qualities,
                            hw_accel=args.hw_accel,
                            parallel=args.parallel,
                            force_reencode=args.force_reencode)

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