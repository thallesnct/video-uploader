"""Segmenting an already-transcoded rendition into HLS (ADR-0012).

Remuxes, never re-encodes: the input is the MP4 `worker_transcode` just
produced at the target rendition's exact dimensions/bitrate, so `-c copy`
repackages it into segments without a second encode pass. Same split as
media.py/transcode.py/thumbnail.py: argv construction is pure, execution is
not, so the pure part is unit-tested without ffmpeg (ADR-0011). The CLI is
invoked with an explicit argv list, never a shell (ADR-0014, ADR-0015).
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

from pipeline.retry import TerminalError
from pipeline.transcode import bitrate_for

FFMPEG = "ffmpeg"
DEFAULT_SEGMENT_SECONDS = 6
SEGMENT_PATTERN = "seg%03d.ts"
PLAYLIST_NAME = "playlist.m3u8"


def build_hls_argv(
    source: str,
    output_dir: str,
    *,
    segment_seconds: int = DEFAULT_SEGMENT_SECONDS,
) -> list[str]:
    return [
        FFMPEG,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        source,
        "-c",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        str(segment_seconds),
        "-hls_playlist_type",
        "vod",
        "-hls_flags",
        "independent_segments",
        "-hls_segment_filename",
        os.path.join(output_dir, SEGMENT_PATTERN),
        os.path.join(output_dir, PLAYLIST_NAME),
    ]


@dataclass(frozen=True)
class HlsResult:
    playlist_path: str
    segment_paths: tuple[str, ...]


def generate_hls(
    source: str,
    output_dir: str,
    *,
    segment_seconds: int = DEFAULT_SEGMENT_SECONDS,
    timeout_s: float = 60.0,
) -> HlsResult:
    os.makedirs(output_dir, exist_ok=True)
    argv = build_hls_argv(source, output_dir, segment_seconds=segment_seconds)
    try:
        result = subprocess.run(  # noqa: S603
            argv, capture_output=True, text=True, timeout=timeout_s, check=False
        )
    except subprocess.TimeoutExpired as exc:
        raise TerminalError(f"ffmpeg exceeded its {timeout_s}s budget for hls") from exc

    if result.returncode != 0:
        raise TerminalError(f"ffmpeg failed for hls: {result.stderr.strip()[:500]}")

    playlist_path = os.path.join(output_dir, PLAYLIST_NAME)
    if not os.path.exists(playlist_path) or os.path.getsize(playlist_path) == 0:
        raise TerminalError("ffmpeg reported success but wrote no HLS playlist")

    segment_paths = tuple(
        sorted(
            os.path.join(output_dir, name)
            for name in os.listdir(output_dir)
            if name.endswith(".ts")
        )
    )
    if not segment_paths:
        raise TerminalError("ffmpeg reported success but wrote no HLS segments")

    return HlsResult(playlist_path=playlist_path, segment_paths=segment_paths)


def build_master_playlist(renditions: list[str]) -> str:
    """The multivariant playlist worker_package writes (ADR-0013). Pure text,
    no ffmpeg involved — this is just string formatting against a spec.

    URIs are relative to master.m3u8's own key
    (`.../hls/master.m3u8` -> `.../hls/{rendition}/playlist.m3u8`), never a
    full object key: a player resolves `{rendition}/playlist.m3u8` against
    wherever it fetched the master from, so pasting an owner-prefixed S3 key
    here would send every player to the wrong place.

    Sorted ascending by height (BANDWIDTH), the conventional order for
    adaptive players choosing a starting rendition — and a deterministic one,
    since `renditions` arrives as whatever order a dict's keys happened to be
    in.
    """
    ordered = sorted(renditions, key=lambda r: int(r.rstrip("p")))
    lines = ["#EXTM3U", "#EXT-X-VERSION:3"]
    for rendition in ordered:
        bandwidth = bitrate_for(rendition) * 1000
        lines.append(f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth}")
        lines.append(f"{rendition}/playlist.m3u8")
    return "\n".join(lines) + "\n"
