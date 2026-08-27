"""The real ffprobe path, exercised inside the worker image (ADR-0011).

Everything else about the probe stage is covered without ffmpeg by injecting the
prober. This file covers the one thing that cannot be: that our argv, our
parsing and the actual binary agree on a real file.
"""

from __future__ import annotations

import pathlib
import subprocess

import pytest
from pipeline.ladder import select_ladder
from pipeline.media import probe
from pipeline.retry import TerminalError

pytestmark = pytest.mark.ffmpeg

# Built into the worker image so every ffmpeg-dependent test uses the same clip.
FIXTURE = pathlib.Path("/app/fixtures/testsrc-640x360.mp4")


@pytest.fixture(scope="module")
def clip(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    if FIXTURE.exists():
        return FIXTURE
    # Outside the image (e.g. a developer with ffmpeg installed), generate the
    # identical clip rather than skipping.
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


def test_probe_reads_a_real_file(clip: pathlib.Path) -> None:
    info = probe(str(clip))

    assert (info.width, info.height) == (640, 360)
    assert info.video_codec == "h264"
    assert info.audio_codec == "aac"
    assert 1.5 <= info.duration_s <= 2.5


def test_a_real_probe_drives_the_expected_ladder(clip: pathlib.Path) -> None:
    """The Phase 4 gate's original wording, run where ffprobe actually exists:
    a 640x360 source yields the sub-360p ladder and no 1080p."""
    info = probe(str(clip))

    ladder = select_ladder(info.width, info.height, info.rotation)

    assert ladder == ["360p"]
    assert "1080p" not in ladder


def test_a_corrupt_file_is_terminal_not_retryable(tmp_path: pathlib.Path) -> None:
    """A truncated file will fail identically forever, so it must reach the DLQ
    rather than walk the retry ladder (ADR-0005)."""
    broken = tmp_path / "broken.mp4"
    broken.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\xff" * 64)

    with pytest.raises(TerminalError):
        probe(str(broken))


def test_a_text_file_masquerading_as_video_is_terminal(tmp_path: pathlib.Path) -> None:
    fake = tmp_path / "not-a-video.mp4"
    fake.write_text("this is definitely not an mp4")

    with pytest.raises(TerminalError):
        probe(str(fake))
