"""Choosing which renditions a source deserves (ADR-0012).

Pure and synchronous on purpose: this is the decision the whole fan-out hangs
off, and ffmpeg is not installed on every machine that needs to test it
(ADR-0011). Give it numbers, get a ladder.

The rule is never to upscale. Encoding a 720p source to 1080p spends real CPU
producing a file that is larger and no better, and then the packaging join in
ADR-0013 waits for it like any other rendition.
"""

from __future__ import annotations

from pipeline.retry import TerminalError

# The rungs, shortest side first.
STANDARD_RUNGS: tuple[int, ...] = (360, 480, 720, 1080)

# Below this, it is not a video worth transcoding — and the rendition label
# pattern (\\d{3,4}p) cannot express it either.
MIN_SUPPORTED_SHORT_SIDE = 100


def display_dimensions(width: int, height: int, rotation: int = 0) -> tuple[int, int]:
    """Dimensions as a viewer sees them.

    Phone video is routinely stored landscape with a rotation flag, so the raw
    stream dimensions are not what anyone watches.
    """
    if rotation % 180 == 90:
        return height, width
    return width, height


def select_ladder(width: int, height: int, rotation: int = 0) -> list[str]:
    """The renditions to produce for a source of this size.

    Keyed on the **short side**, not on height: a 1080x1920 portrait video is a
    1080-class source, and keying on height would treat it as 1920-class and
    produce nothing but upscales.
    """
    display_width, display_height = display_dimensions(width, height, rotation)
    short_side = min(display_width, display_height)

    if short_side < MIN_SUPPORTED_SHORT_SIDE:
        raise TerminalError(
            f"source short side {short_side}px is below the {MIN_SUPPORTED_SHORT_SIDE}px "
            "minimum; retrying cannot help"
        )

    rungs = [rung for rung in STANDARD_RUNGS if rung <= short_side]
    if not rungs:
        # Smaller than every standard rung. Still transcode it once at its own
        # size: the container and codec need normalising for HLS even when the
        # resolution does not change. An empty ladder would be worse — packaging
        # would consider a video with zero renditions trivially complete.
        rungs = [short_side]
    return [f"{rung}p" for rung in rungs]
