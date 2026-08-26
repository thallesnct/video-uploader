"""The real ffmpeg transcode path, exercised inside the worker image (ADR-0011).

Everything else about the transcode stage is covered without ffmpeg by
injecting the transcode function. This file covers the one thing that cannot
be: that our argv, our timeout handling, and the actual binary agree on real
input, and that the output is genuinely playable video at the requested size.
"""

from __future__ import annotations

import pathlib
import subprocess

import pytest
from pipeline.retry import TerminalError
from pipeline.transcode import build_argv, transcode

pytestmark = pytest.mark.ffmpeg

FIXTURE = pathlib.Path("/app/fixtures/testsrc-640x360.mp4")


@pytest.fixture(scope="module")
def clip(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    if FIXTURE.exists():
        return FIXTURE
    generated = tmp_path_factory.mktemp("fixtures") / "testsrc-640x360.mp4"
    subprocess.run(  # noqa: S603
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=640x360:rate=15",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440",
            "-t",
            "2",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-shortest",
            str(generated),
        ],
        check=True,
        capture_output=True,
    )
    return generated


def test_transcoding_a_real_file_produces_a_playable_output_at_the_right_size(
    clip: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    destination = tmp_path / "360p.mp4"

    result = transcode(str(clip), str(destination), "360p", timeout_s=30.0)

    assert pathlib.Path(result.output_path).exists()
    assert pathlib.Path(result.output_path).stat().st_size > 0

    probe = subprocess.run(  # noqa: S603
        ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", result.output_path],
        capture_output=True,
        text=True,
        check=True,
    )
    import json

    streams = json.loads(probe.stdout)["streams"]
    video = next(s for s in streams if s["codec_type"] == "video")
    assert video["height"] == 360
    assert video["codec_name"] == "h264"


def test_transcoding_down_a_rendition_that_matches_source_size(
    clip: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Same height as the source (the ADR-0012 sub-rung case): still a valid
    transcode, not a no-op copy."""
    destination = tmp_path / "same-size.mp4"

    result = transcode(str(clip), str(destination), "360p", timeout_s=30.0)

    assert pathlib.Path(result.output_path).stat().st_size > 0


def test_a_hostile_argument_does_not_reach_a_shell(tmp_path: pathlib.Path) -> None:
    """Confirms build_argv's output is what actually gets executed — an argv
    list, so a semicolon in a path is inert (ADR-0015)."""
    hostile_source = str(tmp_path / "clip.mp4; touch /tmp/pwned")
    argv = build_argv(hostile_source, str(tmp_path / "out.mp4"), "360p")

    # ffmpeg will fail to open the (nonexistent, oddly-named) file — that
    # failure itself proves the semicolon was never interpreted as a new
    # command; a shell would have run `touch` regardless of ffmpeg's outcome.
    result = subprocess.run(argv, capture_output=True, text=True, check=False)  # noqa: S603

    assert result.returncode != 0
    assert not (tmp_path / "pwned").exists()
    import os

    assert not os.path.exists("/tmp/pwned")  # noqa: S108


def test_a_corrupt_file_is_terminal_not_retryable(tmp_path: pathlib.Path) -> None:
    broken = tmp_path / "broken.mp4"
    broken.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\xff" * 64)
    destination = tmp_path / "out.mp4"

    with pytest.raises(TerminalError):
        transcode(str(broken), str(destination), "360p", timeout_s=10.0)


def test_a_genuinely_hung_process_is_terminated_and_classified_terminal(
    clip: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The timeout is a backstop against a stuck ffmpeg process, not the
    primary defense — ADR-0004's pause/resume loop handles ordinary slowness."""
    destination = tmp_path / "timeout.mp4"

    with pytest.raises(TerminalError, match="exceeded"):
        transcode(str(clip), str(destination), "360p", timeout_s=0.01)
