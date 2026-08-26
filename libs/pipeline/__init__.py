"""Shared contracts and Kafka/storage plumbing for every service.

Filled in during Phase 2: event envelope, topic registry, the pause/resume
consumer loop of ADR-0004, retry/DLQ routing, storage helpers, observability
bootstrap. It exists now so the build backend has a package to resolve.
"""

__version__ = "0.1.0"
