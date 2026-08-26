"""argv construction — pure, so it is tested without ffmpeg installed (ADR-0011)."""

from __future__ import annotations

import pytest
from pipeline.transcode import bitrate_for, build_argv, timeout_budget_s


@pytest.mark.parametrize(
    ("rendition", "kbps"),
    [("240p", 400), ("360p", 800), ("480p", 1400), ("720p", 2800), ("1080p", 5000)],
)
def test_standard_rungs_have_a_fixed_bitrate(rendition: str, kbps: int) -> None:
    assert bitrate_for(rendition) == kbps


def test_a_non_standard_rung_interpolates_between_its_neighbours() -> None:
    """The ladder can produce e.g. '818p' for an odd source size (ADR-0012)."""
    below, above = bitrate_for("720p"), bitrate_for("1080p")
    assert below < bitrate_for("900p") < above


def test_below_the_smallest_rung_uses_the_smallest_bitrate() -> None:
    assert bitrate_for("100p") == bitrate_for("240p")


def test_above_the_largest_rung_uses_the_largest_bitrate() -> None:
    assert bitrate_for("4000p") == bitrate_for("1080p")


def test_argv_scales_to_the_rendition_height() -> None:
    argv = build_argv("in.mp4", "out.mp4", "720p")

    assert "-vf" in argv
    assert argv[argv.index("-vf") + 1] == "scale=-2:720"


def test_a_hostile_filename_is_one_argv_element_not_shell_syntax() -> None:
    """subprocess.run receives a list, never a joined string (ADR-0015): a
    semicolon in a filename must stay inert, not become a second command."""
    hostile = "clip.mp4; rm -rf /"
    argv = build_argv(hostile, "out.mp4", "360p")

    assert argv[argv.index("-i") + 1] == hostile
    assert isinstance(argv, list)
    assert all(isinstance(part, str) for part in argv)


def test_argv_is_deterministic_and_ends_with_the_destination() -> None:
    argv = build_argv("source.mp4", "dest.mp4", "480p")
    assert argv[-1] == "dest.mp4"
    assert argv[0] == "ffmpeg"


def test_audio_codec_is_always_present() -> None:
    """Verified empirically: ffmpeg tolerates -c:a on a video-only source."""
    argv = build_argv("in.mp4", "out.mp4", "360p")
    assert "-c:a" in argv


def test_timeout_budget_has_a_floor_for_very_short_clips() -> None:
    assert timeout_budget_s(2.0) == 120.0


def test_timeout_budget_scales_with_source_duration() -> None:
    assert timeout_budget_s(3600.0) == 3600.0 * 8.0
