"""Poster frame, sprite sheet, and WebVTT cues for the scrubber (ADR-0012).

Same split as media.py/transcode.py: the layout math (how many tiles, what
grid, what each WebVTT cue says) is pure and unit-tested without ffmpeg
installed (ADR-0011); only the two functions that shell out belong in the
`tests/ffmpeg`-covered path. The CLI is invoked with an explicit argv list,
never a shell (ADR-0014, ADR-0015).
"""

from __future__ import annotations

import math
import os
import subprocess
from dataclasses import dataclass

from pipeline.obs import tracer
from pipeline.retry import TerminalError

FFMPEG = "ffmpeg"

TILE_WIDTH = 160
TILE_HEIGHT = 90
DEFAULT_INTERVAL_S = 5.0
MAX_COLUMNS = 10
# Bounds the sprite sheet for a pathologically long source: at the default
# interval a 100-tile sheet already covers over 8 minutes, and a fixed ceiling
# keeps the image small regardless of how long the video actually is.
MAX_TILES = 100


def poster_timestamp_s(duration_s: float) -> float:
    """Where to grab the poster frame: 10% in, never the first frame (often
    black/blank on encoder start-up), never past the last second."""
    if duration_s <= 1.0:
        return 0.0
    return min(duration_s * 0.1, duration_s - 0.5)


def build_poster_argv(source: str, destination: str, timestamp_s: float) -> list[str]:
    return [
        FFMPEG,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{timestamp_s:.3f}",
        "-i",
        source,
        "-frames:v",
        "1",
        destination,
    ]


def tile_count(duration_s: float, interval_s: float = DEFAULT_INTERVAL_S) -> int:
    """How many sprite tiles a video of this length produces.

    At least one (even a sub-interval clip gets a single tile), capped at
    MAX_TILES so the sheet stays a fixed, bounded size.
    """
    if duration_s <= 0:
        return 1
    return max(1, min(MAX_TILES, math.ceil(duration_s / interval_s)))


def sprite_grid(count: int, *, max_columns: int = MAX_COLUMNS) -> tuple[int, int]:
    """(columns, rows) for a near-square grid, capped at max_columns wide."""
    columns = min(count, max_columns)
    rows = math.ceil(count / columns)
    return columns, rows


@dataclass(frozen=True)
class SpriteLayout:
    count: int
    columns: int
    rows: int
    interval_s: float
    tile_width: int = TILE_WIDTH
    tile_height: int = TILE_HEIGHT


def plan_sprite(duration_s: float, interval_s: float = DEFAULT_INTERVAL_S) -> SpriteLayout:
    """`interval_s` only chooses how many tiles to make (via tile_count); the
    spacing actually used to place them is always duration_s / (count + 1),
    for two reasons verified empirically:

    1. ffmpeg's fps filter needs to see input *past* a sample's period before
       it will emit that sample — otherwise it silently emits nothing at all
       ("No filtered frames for output stream"), not even at EOF. Since
       `tile_count` rounds up (`ceil`), the nominal cadence's last period
       almost always extends past the source's actual end, which would make
       every sprite sheet whose duration isn't an exact multiple of
       `interval_s` come out empty. Evenly dividing the real duration with one
       spare slot of margin keeps every sample point safely inside it.
    2. When `count` is capped at MAX_TILES for a very long source, the
       nominal cadence would only cover the first `count * interval_s`
       seconds. Spacing across the whole duration instead gives full-video
       scrubbing coverage rather than just the first chunk.
    """
    count = tile_count(duration_s, interval_s)
    columns, rows = sprite_grid(count)
    effective_interval_s = duration_s / (count + 1) if duration_s > 0 else interval_s
    return SpriteLayout(count=count, columns=columns, rows=rows, interval_s=effective_interval_s)


def build_sprite_argv(source: str, destination: str, layout: SpriteLayout) -> list[str]:
    """One image containing `layout.columns` x `layout.rows` tiles, sampled
    every `layout.interval_s` seconds. `-frames:v 1` caps ffmpeg's tile filter
    to exactly one full grid — later-sampled frames beyond columns*rows are
    read and discarded, which is what we want for a video longer than
    MAX_TILES * interval_s.

    `-pix_fmt yuvj420p` is required, not cosmetic: verified empirically that
    the mjpeg encoder rejects the tile filter's default limited-range yuv420p
    output ("Non full-range YUV is non-standard"), and refuses to open at all
    without it — every sprite sheet would fail, not just look wrong.
    """
    return [
        FFMPEG,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        source,
        "-vf",
        (
            f"fps=1/{layout.interval_s},"
            f"scale={layout.tile_width}:{layout.tile_height},"
            f"tile={layout.columns}x{layout.rows}"
        ),
        "-frames:v",
        "1",
        "-pix_fmt",
        "yuvj420p",
        destination,
    ]


def _format_vtt_timestamp(seconds: float) -> str:
    seconds = max(0.0, seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{secs:06.3f}"


def build_vtt(layout: SpriteLayout, sprite_key: str, duration_s: float) -> str:
    """WebVTT text: one cue per tile, each pointing at its `#xywh=` region of
    the sprite sheet. `sprite_key` is embedded verbatim as the media URI —
    the caller (worker_thumbnail) is responsible for it being whatever the
    player can actually fetch; this function only lays out cues and regions.
    """
    lines = ["WEBVTT", ""]
    for index in range(layout.count):
        start = index * layout.interval_s
        is_last = index == layout.count - 1
        # The last cue always reaches the true end, regardless of interval_s —
        # plan_sprite deliberately spaces samples inside duration_s with a
        # margin (ffmpeg-safety, not a scrubber concern), so nominal spacing
        # alone would leave a dead zone at the end where no cue is active.
        end = duration_s if is_last else (index + 1) * layout.interval_s
        end = max(end, start + 0.001)  # a cue's end must be strictly after its start
        column = index % layout.columns
        row = index // layout.columns
        x = column * layout.tile_width
        y = row * layout.tile_height
        lines.append(f"{_format_vtt_timestamp(start)} --> {_format_vtt_timestamp(end)}")
        lines.append(f"{sprite_key}#xywh={x},{y},{layout.tile_width},{layout.tile_height}")
        lines.append("")
    return "\n".join(lines)


def generate_poster(
    source: str, destination: str, *, duration_s: float, timeout_s: float = 30.0
) -> None:
    argv = build_poster_argv(source, destination, poster_timestamp_s(duration_s))
    _run(argv, destination, timeout_s=timeout_s, label="poster")


def generate_sprite(
    source: str, destination: str, layout: SpriteLayout, *, timeout_s: float = 60.0
) -> None:
    argv = build_sprite_argv(source, destination, layout)
    _run(argv, destination, timeout_s=timeout_s, label="sprite")


def _run(argv: list[str], destination: str, *, timeout_s: float, label: str) -> None:
    try:
        with tracer().start_as_current_span("ffmpeg") as span:
            span.set_attribute("asset", label)
            result = subprocess.run(  # noqa: S603
                argv, capture_output=True, text=True, timeout=timeout_s, check=False
            )
    except subprocess.TimeoutExpired as exc:
        raise TerminalError(f"ffmpeg exceeded its {timeout_s}s budget for {label}") from exc

    if result.returncode != 0:
        raise TerminalError(f"ffmpeg failed for {label}: {result.stderr.strip()[:500]}")

    if not os.path.exists(destination) or os.path.getsize(destination) == 0:
        raise TerminalError(f"ffmpeg reported success but wrote no output for {label}")
