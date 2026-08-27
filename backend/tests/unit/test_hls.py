"""HLS argv construction — pure, tested without ffmpeg (ADR-0011)."""

from __future__ import annotations

from pipeline.hls import build_hls_argv


def test_argv_targets_vod_playlist_type() -> None:
    argv = build_hls_argv("in.mp4", "/out")
    assert "-hls_playlist_type" in argv
    assert argv[argv.index("-hls_playlist_type") + 1] == "vod"


def test_argv_copies_rather_than_re_encodes() -> None:
    """The input is already at the target rendition — a second encode pass
    would be pure waste."""
    argv = build_hls_argv("in.mp4", "/out")
    assert argv[argv.index("-c") + 1] == "copy"


def test_argv_uses_the_requested_segment_duration() -> None:
    argv = build_hls_argv("in.mp4", "/out", segment_seconds=4)
    assert argv[argv.index("-hls_time") + 1] == "4"


def test_argv_writes_the_playlist_and_segments_into_the_output_dir() -> None:
    argv = build_hls_argv("in.mp4", "/out")
    assert argv[-1] == "/out/playlist.m3u8"
    assert argv[argv.index("-hls_segment_filename") + 1] == "/out/seg%03d.ts"


def test_a_hostile_filename_is_one_argv_element_not_shell_syntax() -> None:
    """subprocess.run receives a list, never a joined string (ADR-0015)."""
    hostile = "clip.mp4; rm -rf /"
    argv = build_hls_argv(hostile, "/out")
    assert argv[argv.index("-i") + 1] == hostile
    assert all(isinstance(part, str) for part in argv)
