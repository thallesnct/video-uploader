"""Building and running the ffmpeg transcode invocation (ADR-0014).

argv construction is pure and separated from execution for the same reason
media.py splits ffprobe parsing from the subprocess call: it is the part worth
testing carefully, and it is testable without ffmpeg installed (ADR-0011).

The CLI is invoked with an explicit argv list, never a shell — the ADR-0014
decision (out-of-process, so a hostile input crashes a subprocess and not the
worker) and the ADR-0015 rule about untrusted input in the same place.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

from pipeline.obs import tracer
from pipeline.retry import TerminalError

FFMPEG = "ffmpeg"

# height -> bitrate in kbps. Chosen for a reasonable quality/size trade-off,
# not tuned — revisit once real footage is measured.
_BITRATE_KBPS: dict[int, int] = {
    240: 400,
    360: 800,
    480: 1400,
    720: 2800,
    1080: 5000,
}


def _rung(rendition: str) -> int:
    """'720p' -> 720."""
    return int(rendition.rstrip("p"))


def bitrate_for(rendition: str) -> int:
    """kbps for a rendition, interpolating for any non-standard rung
    (ADR-0012's ladder can produce e.g. '818p' for an odd source size)."""
    rung = _rung(rendition)
    if rung in _BITRATE_KBPS:
        return _BITRATE_KBPS[rung]
    rungs = sorted(_BITRATE_KBPS)
    if rung <= rungs[0]:
        return _BITRATE_KBPS[rungs[0]]
    if rung >= rungs[-1]:
        return _BITRATE_KBPS[rungs[-1]]
    lower = max(r for r in rungs if r < rung)
    upper = min(r for r in rungs if r > rung)
    span = upper - lower
    weight = (rung - lower) / span
    return round(_BITRATE_KBPS[lower] + weight * (_BITRATE_KBPS[upper] - _BITRATE_KBPS[lower]))


def build_argv(source: str, destination: str, rendition: str) -> list[str]:
    """The ffmpeg invocation for one rendition.

    Scales to the rendition's height, preserving aspect ratio (width computed
    as -2 so it always divides evenly by 2, which H.264 requires).

    `-c:a aac` is passed unconditionally rather than threading an audio flag
    through the whole pipeline: verified empirically that ffmpeg tolerates an
    audio codec option when the source has no audio stream to encode (exit 0,
    output simply carries no audio track) — there is nothing to make
    conditional.
    """
    height = _rung(rendition)
    kbps = bitrate_for(rendition)
    return [
        FFMPEG,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        source,
        "-vf",
        f"scale=-2:{height}",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-b:v",
        f"{kbps}k",
        "-maxrate",
        f"{kbps}k",
        "-bufsize",
        f"{kbps * 2}k",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        destination,
    ]


@dataclass(frozen=True)
class TranscodeResult:
    output_path: str


def transcode(
    source: str,
    destination: str,
    rendition: str,
    *,
    timeout_s: float,
) -> TranscodeResult:
    """Run the transcode. Raises TerminalError on any ffmpeg failure — a
    transcode that fails on valid input from a successfully-probed source is
    not something a retry can fix (ADR-0005)."""
    argv = build_argv(source, destination, rendition)
    try:
        with tracer().start_as_current_span("ffmpeg") as span:
            span.set_attribute("rendition", rendition)
            result = subprocess.run(  # noqa: S603
                argv, capture_output=True, text=True, timeout=timeout_s, check=False
            )
    except subprocess.TimeoutExpired as exc:
        raise TerminalError(f"ffmpeg exceeded its {timeout_s}s budget for {rendition}") from exc

    if result.returncode != 0:
        raise TerminalError(f"ffmpeg failed for {rendition}: {result.stderr.strip()[:500]}")

    if not os.path.exists(destination) or os.path.getsize(destination) == 0:
        raise TerminalError(f"ffmpeg reported success but wrote no output for {rendition}")

    return TranscodeResult(output_path=destination)


def timeout_budget_s(source_duration_s: float, *, realtime_ratio_ceiling: float = 8.0) -> float:
    """How long a transcode may run before it is considered hung.

    Generous on purpose: this is the backstop, not the primary control.
    ADR-0004's pause/resume loop is what keeps the consumer alive during a long
    transcode; this timeout exists only to catch a genuinely stuck ffmpeg
    process, classified terminal rather than left to run forever.
    """
    floor_s = 120.0
    return max(floor_s, source_duration_s * realtime_ratio_ceiling)
