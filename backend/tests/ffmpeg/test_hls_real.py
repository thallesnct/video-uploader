"""The real ffmpeg HLS remux path, exercised inside the worker image
(ADR-0011).

Everything else about HLS segmentation is covered without ffmpeg by injecting
generate_hls. This file covers the one thing that cannot be: that our argv,
the actual binary, and a real transcoded rendition agree — a valid VOD
playlist referencing segments that genuinely exist and genuinely play.
"""

from __future__ import annotations

import pathlib
import subprocess

import pytest
from pipeline.hls import generate_hls
from pipeline.retry import TerminalError

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


def test_remuxing_a_real_file_produces_a_playable_vod_playlist(
    clip: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    output_dir = tmp_path / "hls"

    result = generate_hls(str(clip), str(output_dir), segment_seconds=1)

    assert pathlib.Path(result.playlist_path).exists()
    assert len(result.segment_paths) >= 1
    for segment_path in result.segment_paths:
        assert pathlib.Path(segment_path).exists()
        assert pathlib.Path(segment_path).stat().st_size > 0

    playlist_text = pathlib.Path(result.playlist_path).read_text()
    assert "#EXTM3U" in playlist_text
    assert "#EXT-X-ENDLIST" in playlist_text  # VOD, not a live playlist
    for segment_path in result.segment_paths:
        assert pathlib.Path(segment_path).name in playlist_text

    # The playlist genuinely plays: ffprobe can read a stream out of it.
    probe = subprocess.run(  # noqa: S603
        ["ffprobe", "-v", "error", "-show_streams", result.playlist_path],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "codec_type=video" in probe.stdout


def test_a_corrupt_file_is_terminal_not_retryable(tmp_path: pathlib.Path) -> None:
    broken = tmp_path / "broken.mp4"
    broken.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\xff" * 64)

    with pytest.raises(TerminalError):
        generate_hls(str(broken), str(tmp_path / "hls"))


def test_a_hostile_argument_does_not_reach_a_shell(tmp_path: pathlib.Path) -> None:
    """Confirms build_hls_argv's output is what actually gets executed — an
    argv list, so a semicolon in a path is inert (ADR-0015)."""
    from pipeline.hls import build_hls_argv

    hostile_source = str(tmp_path / "clip.mp4; touch /tmp/pwned")
    argv = build_hls_argv(hostile_source, str(tmp_path / "hls"))

    result = subprocess.run(argv, capture_output=True, text=True, check=False)  # noqa: S603

    assert result.returncode != 0
    import os

    assert not os.path.exists("/tmp/pwned")  # noqa: S108
