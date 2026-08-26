"""Shared collection rules.

ffmpeg lives only in the worker image (ADR-0011), so tests that shell out to it
skip with a clear reason on a bare machine instead of failing confusingly.
"""

from __future__ import annotations

import shutil
from typing import Any

import pytest


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    if shutil.which("ffprobe") and shutil.which("ffmpeg"):
        return
    skip = pytest.mark.skip(
        reason="ffmpeg is not installed here — run `make ffmpeg-tests` to run "
        "these inside the worker image"
    )
    for item in items:
        if "ffmpeg" in item.keywords:
            item.add_marker(skip)
