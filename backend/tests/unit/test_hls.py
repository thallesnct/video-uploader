"""HLS argv construction — pure, tested without ffmpeg (ADR-0011)."""

from __future__ import annotations

from pipeline.hls import build_hls_argv, build_master_playlist


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


def test_master_playlist_uses_relative_uris_not_object_keys() -> None:
    """A player resolves the URI against wherever it fetched master.m3u8
    from — a full owner-prefixed S3 key here would send it to the wrong
    place entirely."""
    playlist = build_master_playlist(["360p"])
    assert "360p/playlist.m3u8" in playlist
    assert "users/" not in playlist


def test_master_playlist_lists_every_rendition_exactly_once() -> None:
    playlist = build_master_playlist(["720p", "360p", "1080p"])
    for rendition in ("360p", "720p", "1080p"):
        assert playlist.count(f"{rendition}/playlist.m3u8") == 1


def test_master_playlist_is_ordered_ascending_by_bandwidth() -> None:
    playlist = build_master_playlist(["1080p", "360p", "720p"])
    lines = [line for line in playlist.splitlines() if line.endswith("playlist.m3u8")]
    assert lines == ["360p/playlist.m3u8", "720p/playlist.m3u8", "1080p/playlist.m3u8"]


def test_master_playlist_declares_a_bandwidth_for_every_stream() -> None:
    playlist = build_master_playlist(["360p", "720p"])
    assert playlist.count("#EXT-X-STREAM-INF:BANDWIDTH=") == 2
