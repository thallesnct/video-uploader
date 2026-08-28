"""Poster/sprite/VTT layout math — pure, tested without ffmpeg (ADR-0011)."""

from __future__ import annotations

import pytest
from pipeline.thumbnail import (
    MAX_TILES,
    SpriteLayout,
    build_poster_argv,
    build_sprite_argv,
    build_vtt,
    plan_sprite,
    poster_timestamp_s,
    sprite_grid,
    tile_count,
)


def test_poster_grabs_a_frame_ten_percent_in() -> None:
    assert poster_timestamp_s(100.0) == 10.0


def test_poster_never_lands_on_the_first_frame_of_a_very_short_clip() -> None:
    assert poster_timestamp_s(0.5) == 0.0


@pytest.mark.parametrize("duration_s", [1.5, 2.0, 5.0, 100.0])
def test_poster_never_lands_past_the_last_half_second(duration_s: float) -> None:
    assert poster_timestamp_s(duration_s) <= duration_s - 0.5


def test_poster_argv_is_a_list_ending_at_the_destination() -> None:
    argv = build_poster_argv("in.mp4", "out.jpg", 4.2)
    assert argv[0] == "ffmpeg"
    assert argv[-1] == "out.jpg"
    assert argv[argv.index("-ss") + 1] == "4.200"


def test_a_hostile_filename_is_one_argv_element_not_shell_syntax() -> None:
    """subprocess.run receives a list, never a joined string (ADR-0015)."""
    hostile = "clip.mp4; rm -rf /"
    argv = build_poster_argv(hostile, "out.jpg", 1.0)
    assert argv[argv.index("-i") + 1] == hostile
    assert all(isinstance(part, str) for part in argv)


@pytest.mark.parametrize(
    ("duration_s", "interval_s", "expected"),
    [(0.0, 5.0, 1), (4.0, 5.0, 1), (5.0, 5.0, 1), (5.1, 5.0, 2), (23.0, 5.0, 5)],
)
def test_tile_count_covers_the_whole_duration(
    duration_s: float, interval_s: float, expected: int
) -> None:
    assert tile_count(duration_s, interval_s) == expected


def test_tile_count_is_capped_for_a_pathologically_long_source() -> None:
    assert tile_count(duration_s=100_000.0, interval_s=5.0) == MAX_TILES


@pytest.mark.parametrize(
    ("count", "columns", "rows"),
    [(1, 1, 1), (5, 5, 1), (10, 10, 1), (11, 10, 2), (100, 10, 10)],
)
def test_sprite_grid_stays_near_square_capped_at_ten_wide(
    count: int, columns: int, rows: int
) -> None:
    assert sprite_grid(count) == (columns, rows)


def test_plan_sprite_derives_a_consistent_layout() -> None:
    layout = plan_sprite(duration_s=23.0, interval_s=5.0)
    assert layout.count == 5
    assert layout.columns * layout.rows >= layout.count


def test_sprite_argv_samples_at_the_layout_interval_and_tiles_the_grid() -> None:
    layout = SpriteLayout(count=6, columns=3, rows=2, interval_s=5.0)
    argv = build_sprite_argv("in.mp4", "out.jpg", layout)

    vf = argv[argv.index("-vf") + 1]
    assert "fps=1/5.0" in vf
    assert "tile=3x2" in vf
    assert argv[-1] == "out.jpg"


def test_poster_and_sprite_threads_are_capped_by_default_and_overridable() -> None:
    """ADR-0015 §1 containment, same reasoning as transcode's own cap."""
    layout = SpriteLayout(count=1, columns=1, rows=1, interval_s=5.0)

    poster_argv = build_poster_argv("in.mp4", "out.jpg", 1.0)
    assert poster_argv[poster_argv.index("-threads") + 1] == "2"

    custom_poster_argv = build_poster_argv("in.mp4", "out.jpg", 1.0, threads=4)
    assert custom_poster_argv[custom_poster_argv.index("-threads") + 1] == "4"

    sprite_argv = build_sprite_argv("in.mp4", "out.jpg", layout)
    assert sprite_argv[sprite_argv.index("-threads") + 1] == "2"


def test_vtt_starts_with_the_webvtt_header() -> None:
    layout = SpriteLayout(count=1, columns=1, rows=1, interval_s=5.0)
    vtt = build_vtt(layout, "sprite.jpg", duration_s=3.0)
    assert vtt.startswith("WEBVTT\n")


def test_vtt_has_one_cue_per_tile_with_the_right_region() -> None:
    layout = SpriteLayout(
        count=4, columns=2, rows=2, interval_s=5.0, tile_width=160, tile_height=90
    )
    vtt = build_vtt(layout, "sprite.jpg", duration_s=20.0)

    # Tile index 3 sits at column 1, row 1 of a 2x2 grid.
    assert "sprite.jpg#xywh=160,90,160,90" in vtt
    # Tile index 0 sits at the origin.
    assert "sprite.jpg#xywh=0,0,160,90" in vtt


def test_vtt_cue_timestamps_are_sequential_and_non_overlapping() -> None:
    layout = SpriteLayout(count=3, columns=3, rows=1, interval_s=5.0)
    vtt = build_vtt(layout, "sprite.jpg", duration_s=15.0)
    cue_lines = [line for line in vtt.splitlines() if "-->" in line]

    assert cue_lines == [
        "00:00:00.000 --> 00:00:05.000",
        "00:00:05.000 --> 00:00:10.000",
        "00:00:10.000 --> 00:00:15.000",
    ]


def test_the_final_cue_is_clamped_to_the_actual_duration() -> None:
    """A source shorter than count * interval_s (the last, partial tile)
    must not produce a cue that extends past the video's own end."""
    layout = SpriteLayout(count=2, columns=2, rows=1, interval_s=5.0)
    vtt = build_vtt(layout, "sprite.jpg", duration_s=7.0)
    cue_lines = [line for line in vtt.splitlines() if "-->" in line]

    assert cue_lines[-1] == "00:00:05.000 --> 00:00:07.000"


@pytest.mark.parametrize("duration_s", [2.0, 5.0, 23.0, 3600.0])
def test_a_planned_layouts_vtt_spans_the_whole_duration(duration_s: float) -> None:
    """The invariant a scrubber actually depends on: end-to-end from a real
    plan_sprite output, not a hand-built SpriteLayout — plan_sprite spaces
    tiles by duration_s / (count + 1), not by the nominal interval_s, so this
    is the only test that would catch the two drifting apart."""
    layout = plan_sprite(duration_s)
    vtt = build_vtt(layout, "sprite.jpg", duration_s)
    cue_lines = [line for line in vtt.splitlines() if "-->" in line]
    last_end = cue_lines[-1].split(" --> ")[1]

    hours, minutes, seconds = last_end.split(":")
    last_end_s = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    assert last_end_s == pytest.approx(duration_s, abs=0.01)
