"""ffprobe output parsing, tested on synthetic JSON.

ffmpeg is not installed on this machine (ADR-0011), so the parse is separated
from the subprocess call precisely to make this possible.
"""

from __future__ import annotations

import pytest
from pipeline.media import parse_ffprobe
from pipeline.retry import TerminalError


def probe_payload(**overrides: object) -> dict:
    payload = {
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
            {"codec_type": "audio", "codec_name": "aac"},
        ],
        "format": {"duration": "12.5"},
    }
    payload.update(overrides)  # type: ignore[arg-type]
    return payload


def test_reads_the_basics() -> None:
    info = parse_ffprobe(probe_payload())

    assert (info.width, info.height) == (1920, 1080)
    assert info.duration_s == 12.5
    assert info.video_codec == "h264"
    assert info.audio_codec == "aac"


def test_video_without_audio_is_normal_not_an_error() -> None:
    """Plenty of real video has no audio track."""
    payload = probe_payload(
        streams=[{"codec_type": "video", "codec_name": "h264", "width": 640, "height": 360}]
    )

    assert parse_ffprobe(payload).audio_codec is None


def test_duration_falls_back_to_the_stream_when_the_container_omits_it() -> None:
    """Some containers carry duration on the stream and not on the format."""
    payload = {
        "streams": [
            {
                "codec_type": "video",
                "codec_name": "vp9",
                "width": 1280,
                "height": 720,
                "duration": "8.0",
            }
        ],
        "format": {},
    }

    assert parse_ffprobe(payload).duration_s == 8.0


@pytest.mark.parametrize(
    "stream_extra",
    [
        {"side_data_list": [{"rotation": -90}]},
        {"tags": {"rotate": "270"}},
    ],
)
def test_rotation_is_read_from_either_place_ffprobe_puts_it(stream_extra: dict) -> None:
    payload = probe_payload(
        streams=[
            {
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                **stream_extra,
            }
        ]
    )

    assert parse_ffprobe(payload).rotation == 270


def test_an_audio_only_file_is_terminal() -> None:
    """No video stream will still be no video stream on the next attempt."""
    payload = probe_payload(streams=[{"codec_type": "audio", "codec_name": "mp3"}])

    with pytest.raises(TerminalError, match="no video stream"):
        parse_ffprobe(payload)


def test_missing_duration_everywhere_is_terminal() -> None:
    payload = {
        "streams": [{"codec_type": "video", "codec_name": "h264", "width": 640, "height": 360}],
        "format": {},
    }
    with pytest.raises(TerminalError, match="duration"):
        parse_ffprobe(payload)


@pytest.mark.parametrize("bad", [{"duration": "0"}, {"duration": "-3"}])
def test_non_positive_duration_is_terminal(bad: dict) -> None:
    with pytest.raises(TerminalError, match="duration"):
        parse_ffprobe(probe_payload(format=bad))


def test_malformed_dimensions_are_terminal() -> None:
    payload = probe_payload(
        streams=[{"codec_type": "video", "codec_name": "h264", "width": "wide", "height": 360}]
    )
    with pytest.raises(TerminalError, match="malformed"):
        parse_ffprobe(payload)
