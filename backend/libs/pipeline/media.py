"""Reading what a video actually is, via ffprobe (ADR-0014).

The CLI is invoked with an explicit argv list and never a shell. That is both
the ADR-0014 decision — the CLI is the interface every ffmpeg answer is written
against, and it runs out of process so a hostile file crashes a subprocess
rather than the worker — and the ADR-0015 rule about untrusted input.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any

from pipeline.obs import tracer
from pipeline.retry import TerminalError, TransientError

FFPROBE = "ffprobe"

# ADR-0015 §1: "validate container/codec via ffprobe before transcoding, and
# reject formats outside an allow-list; enable only the demuxers we need
# where feasible." ffmpeg's decoder is the actual attack surface (media
# demuxers are a long-standing source of memory-safety CVEs) — this rejects
# an unexpected codec before worker_transcode ever hands the file to ffmpeg
# for real decoding, not just for the cheap ffprobe read already done here.
# worker_transcode's own output is always libx264 (transcode.py) regardless
# of input, so this is purely about what ffmpeg is asked to *decode* — kept
# to the codecs the frontend's own ALLOWED_TYPES containers (mp4/mov/mkv/
# webm/avi) commonly carry. Widen deliberately if a real upload needs it,
# never silently.
ALLOWED_VIDEO_CODECS = frozenset({"h264", "hevc", "vp8", "vp9", "av1"})


@dataclass(frozen=True)
class MediaInfo:
    duration_s: float
    width: int
    height: int
    video_codec: str
    audio_codec: str | None
    rotation: int = 0


def _rotation_of(stream: dict[str, Any]) -> int:
    """Rotation, which ffprobe reports in two different places by version."""
    for side_data in stream.get("side_data_list", []):
        if "rotation" in side_data:
            return int(float(side_data["rotation"])) % 360
    tags = stream.get("tags", {})
    if "rotate" in tags:
        return int(float(tags["rotate"])) % 360
    return 0


def parse_ffprobe(payload: dict[str, Any]) -> MediaInfo:
    """Turn ffprobe JSON into MediaInfo.

    Separate from running ffprobe so it can be unit-tested on a machine with no
    ffmpeg installed — which is most of them (ADR-0011).
    """
    streams = payload.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    if video is None:
        raise TerminalError("file contains no video stream; retrying cannot help")
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)

    # Duration lives on the format object; some containers omit it there and
    # some omit it on the stream, so try both before giving up.
    raw_duration = payload.get("format", {}).get("duration") or video.get("duration")
    if raw_duration is None:
        raise TerminalError("could not determine duration from ffprobe output")

    try:
        width = int(video["width"])
        height = int(video["height"])
        duration = float(raw_duration)
    except (KeyError, TypeError, ValueError) as exc:
        raise TerminalError(f"malformed ffprobe output: {exc}") from exc

    if duration <= 0:
        raise TerminalError(f"non-positive duration {duration}")

    video_codec = str(video.get("codec_name", "unknown"))
    if video_codec not in ALLOWED_VIDEO_CODECS:
        # Retrying can't help: the file's codec doesn't change on redelivery.
        # Straight to the DLQ (RetryPolicy routes TerminalError there), same
        # as every other "this input is fundamentally rejected" case above.
        raise TerminalError(f"unsupported video codec {video_codec!r}")

    return MediaInfo(
        duration_s=duration,
        width=width,
        height=height,
        video_codec=video_codec,
        # Plenty of real video has no audio track at all.
        audio_codec=str(audio["codec_name"]) if audio and "codec_name" in audio else None,
        rotation=_rotation_of(video),
    )


def probe(path: str, timeout_s: float = 60.0) -> MediaInfo:
    """Run ffprobe against a local file."""
    try:
        with tracer().start_as_current_span("ffprobe"):
            result = subprocess.run(  # noqa: S603
                [
                    FFPROBE,
                    "-v",
                    "error",
                    "-print_format",
                    "json",
                    "-show_format",
                    "-show_streams",
                    path,
                ],
                capture_output=True,
                text=True,
                timeout=timeout_s,
                check=False,
            )
    except subprocess.TimeoutExpired as exc:
        # A probe that hangs is a malformed file, not a busy system.
        raise TerminalError(f"ffprobe timed out after {timeout_s}s") from exc
    except FileNotFoundError as exc:
        # The binary is missing: an environment problem, so retry elsewhere.
        raise TransientError("ffprobe is not installed in this image") from exc

    if result.returncode != 0:
        raise TerminalError(f"ffprobe failed: {result.stderr.strip()[:300]}")

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TerminalError(f"ffprobe returned invalid JSON: {exc}") from exc

    return parse_ffprobe(payload)
