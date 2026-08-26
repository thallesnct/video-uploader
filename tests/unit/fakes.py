"""Fakes for the Kafka client surface the consumer loop uses.

The loop is written against a narrow protocol precisely so its behaviour can be
tested here, without a broker. The integration tests in Phase 5 then confirm the
real client behaves the way these fakes assume.
"""

from __future__ import annotations

from typing import Any


class FakeMessage:
    def __init__(
        self,
        value: bytes,
        topic: str = "rendition.requested",
        key: bytes = b"key",
        headers: list[tuple[str, bytes]] | None = None,
        error: Any = None,
    ) -> None:
        self._value, self._topic, self._key = value, topic, key
        self._headers = headers or []
        self._error = error

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers

    def error(self) -> Any:
        return self._error


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage] | None = None) -> None:
        self.queue: list[FakeMessage] = list(messages or [])
        self.assignment_value: list[str] = ["p0", "p1"]
        self.poll_calls = 0
        self.pause_calls: list[list[str]] = []
        self.resume_calls: list[list[str]] = []
        self.commits: list[FakeMessage] = []
        self.commit_error: Exception | None = None
        self.closed = False
        self.events: list[str] = []  # ordering ledger for assertions
        self.paused = False
        self.subscribed: list[str] = []
        self.on_revoke: Any = None

    def poll(self, timeout: float) -> FakeMessage | None:
        self.poll_calls += 1
        if self.queue:
            return self.queue.pop(0)
        return None

    def subscribe(self, topics: list[str], **callbacks: Any) -> None:
        self.subscribed = list(topics)
        self.on_revoke = callbacks.get("on_revoke")
        self.on_lost = callbacks.get("on_lost")

    def pause(self, partitions: list[Any]) -> None:
        self.paused = True
        self.pause_calls.append(list(partitions))
        self.events.append("pause")

    def resume(self, partitions: list[Any]) -> None:
        self.paused = False
        self.resume_calls.append(list(partitions))
        self.events.append("resume")

    def assignment(self) -> list[Any]:
        return list(self.assignment_value)

    def commit(self, message: Any = None, asynchronous: bool = True) -> None:
        self.events.append("commit")
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(message)

    def close(self) -> None:
        self.closed = True


class FakeProducer:
    def __init__(self, ledger: list[str] | None = None) -> None:
        self.published: list[tuple[str, bytes, bytes, list[tuple[str, bytes]]]] = []
        self.typed_published: list[tuple[str, Any]] = []
        self.flushes = 0
        self.events = ledger if ledger is not None else []

    def publish(self, topic: str, event: Any, headers: Any = None) -> None:
        """Records a typed event (e.g. PipelineFailed) separately from the raw
        retry/DLQ republishing that publish_raw handles."""
        self.typed_published.append((topic, event))
        self.events.append("produce")

    def publish_raw(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.published.append((topic, key, value, list(headers or [])))
        self.events.append("produce")

    def flush(self, timeout: float = 10.0) -> int:
        self.flushes += 1
        self.events.append("flush")
        return 0

    def header(self, name: str, index: int = -1) -> str | None:
        for key, value in self.published[index][3]:
            if key == name:
                return value.decode()
        return None
