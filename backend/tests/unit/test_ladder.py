"""Ladder selection — pure, so every edge case is cheap to pin down."""

from __future__ import annotations

import pytest
from pipeline.ladder import MIN_SUPPORTED_SHORT_SIDE, display_dimensions, select_ladder
from pipeline.retry import TerminalError


@pytest.mark.parametrize(
    ("width", "height", "expected"),
    [
        (1920, 1080, ["360p", "480p", "720p", "1080p"]),
        (1280, 720, ["360p", "480p", "720p"]),
        (854, 480, ["360p", "480p"]),
        (640, 360, ["360p"]),
    ],
)
def test_ladder_never_upscales(width: int, height: int, expected: list[str]) -> None:
    """Encoding 720p up to 1080p burns CPU for a bigger, no-better file — and
    then packaging waits for it like any other rendition (ADR-0013)."""
    assert select_ladder(width, height) == expected


def test_portrait_video_is_classed_by_its_short_side() -> None:
    """1080x1920 is a 1080-class source. Keying on height would call it
    1920-class and emit nothing but upscales."""
    assert select_ladder(1080, 1920) == ["360p", "480p", "720p", "1080p"]


def test_rotated_video_matches_its_upright_equivalent() -> None:
    """Phone video is stored landscape with a rotation flag; the ladder must be
    the same either way."""
    assert select_ladder(1920, 1080, rotation=90) == select_ladder(1080, 1920)


@pytest.mark.parametrize("rotation", [0, 90, 180, 270])
def test_rotation_never_changes_the_ladder(rotation: int) -> None:
    """Rotation swaps the sides, and the short side is rotation-invariant."""
    assert select_ladder(1920, 1080, rotation=rotation) == select_ladder(1920, 1080)


def test_a_source_below_every_rung_still_gets_one_rendition() -> None:
    """An empty ladder would be worse than a small one: packaging would consider
    a video with zero renditions trivially complete (ADR-0013)."""
    assert select_ladder(320, 240) == ["240p"]


def test_a_source_that_is_not_really_video_is_terminal() -> None:
    with pytest.raises(TerminalError, match="below the"):
        select_ladder(64, 48)


def test_the_minimum_boundary_is_inclusive() -> None:
    assert select_ladder(200, MIN_SUPPORTED_SHORT_SIDE) == [f"{MIN_SUPPORTED_SHORT_SIDE}p"]


def test_letterboxed_odd_dimensions_do_not_break_selection() -> None:
    assert select_ladder(1920, 818) == ["360p", "480p", "720p"]


@pytest.mark.parametrize(
    ("width", "height", "rotation", "expected"),
    [
        (1920, 1080, 0, (1920, 1080)),
        (1920, 1080, 90, (1080, 1920)),
        (1920, 1080, 180, (1920, 1080)),
        (1920, 1080, 270, (1080, 1920)),
    ],
)
def test_display_dimensions_follow_rotation(
    width: int, height: int, rotation: int, expected: tuple[int, int]
) -> None:
    assert display_dimensions(width, height, rotation) == expected


def test_every_rendition_label_matches_the_event_contract() -> None:
    """Labels must satisfy the Rendition pattern, or the event will not validate."""
    import re

    for width, height in ((1920, 1080), (320, 240), (640, 360)):
        for label in select_ladder(width, height):
            assert re.fullmatch(r"\d{3,4}p", label), label
