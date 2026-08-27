"""The real ffmpeg poster/sprite path, exercised inside the worker image
(ADR-0011).

Everything else about the thumbnail stage is covered without ffmpeg by
injecting poster_fn/sprite_fn. This file covers the one thing that cannot be:
that our argv and the actual binary agree on real input, and that the poster
and sprite sheet are genuinely valid images at the sizes we claim.
"""

from __future__ import annotations

import json
import pathlib
import subprocess

import pytest
from pipeline.thumbnail import (
    TILE_HEIGHT,
    TILE_WIDTH,
    generate_poster,
    generate_sprite,
    plan_sprite,
)

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


def _dimensions(image_path: pathlib.Path) -> tuple[int, int]:
    probe = subprocess.run(  # noqa: S603
        ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", str(image_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    stream = json.loads(probe.stdout)["streams"][0]
    return int(stream["width"]), int(stream["height"])


def test_generating_a_poster_from_a_real_file_produces_a_valid_image(
    clip: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    destination = tmp_path / "poster.jpg"

    generate_poster(str(clip), str(destination), duration_s=2.0)

    assert destination.exists()
    assert destination.stat().st_size > 0
    width, height = _dimensions(destination)
    assert (width, height) == (640, 360)


def test_generating_a_sprite_sheet_produces_the_expected_grid_size(
    clip: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    destination = tmp_path / "sprite.jpg"
    # The 2s fixture at the default 5s interval yields exactly one tile.
    layout = plan_sprite(duration_s=2.0)

    generate_sprite(str(clip), str(destination), layout)

    assert destination.exists()
    width, height = _dimensions(destination)
    assert width == TILE_WIDTH * layout.columns
    assert height == TILE_HEIGHT * layout.rows


def test_a_hostile_argument_does_not_reach_a_shell(tmp_path: pathlib.Path) -> None:
    """Confirms build_poster_argv's output is what actually gets executed — an
    argv list, so a semicolon in a path is inert (ADR-0015)."""
    from pipeline.thumbnail import build_poster_argv

    hostile_source = str(tmp_path / "clip.mp4; touch /tmp/pwned")
    argv = build_poster_argv(hostile_source, str(tmp_path / "out.jpg"), 1.0)

    result = subprocess.run(argv, capture_output=True, text=True, check=False)  # noqa: S603

    assert result.returncode != 0
    import os

    assert not os.path.exists("/tmp/pwned")  # noqa: S108


def test_a_corrupt_file_is_terminal_not_retryable(tmp_path: pathlib.Path) -> None:
    from pipeline.retry import TerminalError

    broken = tmp_path / "broken.mp4"
    broken.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\xff" * 64)
    destination = tmp_path / "poster.jpg"

    with pytest.raises(TerminalError):
        generate_poster(str(broken), str(destination), duration_s=2.0)
