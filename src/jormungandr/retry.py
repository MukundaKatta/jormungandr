"""Retry / backoff primitives for pipeline stages.

The framework already has transform and validation stages; real data
pipelines also need a principled way to retry flaky operations (API
calls, S3 reads, database writes under contention). This module
provides a retry decorator, a circuit breaker, and a jittered
exponential-backoff generator that the stages can compose freely.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Awaitable, Callable, Iterable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.25
    max_delay: float = 30.0
    jitter: float = 0.2
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    give_up_on: tuple[type[BaseException], ...] = ()


def delays(policy: RetryPolicy, *, rng: random.Random | None = None) -> Iterable[float]:
    """Yield successive sleep durations for an exponential+jitter schedule."""
    r = rng or random.Random()
    for attempt in range(policy.max_attempts - 1):
        base = min(policy.max_delay, policy.base_delay * (2 ** attempt))
        yield base * (1.0 + r.uniform(-policy.jitter, policy.jitter))


def retry(policy: RetryPolicy):
    """Decorator: retry a sync function according to `policy`."""
    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapped(*args, **kwargs) -> T:
            last: BaseException | None = None
            schedule = iter(delays(policy))
            for _ in range(policy.max_attempts):
                try:
                    return fn(*args, **kwargs)
                except policy.give_up_on:
                    raise
                except policy.retry_on as exc:
                    last = exc
                    try:
                        d = next(schedule)
                    except StopIteration:
                        break
                    time.sleep(d)
            assert last is not None
            raise last
        return wrapped
    return deco


def retry_async(policy: RetryPolicy):
    """Decorator: retry a coroutine according to `policy`."""
    def deco(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(fn)
        async def wrapped(*args, **kwargs) -> T:
            last: BaseException | None = None
            schedule = iter(delays(policy))
            for _ in range(policy.max_attempts):
                try:
                    return await fn(*args, **kwargs)
                except policy.give_up_on:
                    raise
                except policy.retry_on as exc:
                    last = exc
                    try:
                        d = next(schedule)
                    except StopIteration:
                        break
                    await asyncio.sleep(d)
            assert last is not None
            raise last
        return wrapped
    return deco


# --- Circuit breaker ---------------------------------------------------------

@dataclass
class CircuitBreaker:
    """Half-open circuit breaker keyed by a caller-chosen name.

    After `failure_threshold` consecutive failures, the circuit opens and
    calls fail fast for `cooldown_sec`; one probe is then allowed through
    to decide whether to close.
    """

    failure_threshold: int = 5
    cooldown_sec: float = 30.0
    _failures: int = field(default=0, init=False)
    _opened_at: float | None = field(default=None, init=False)

    def allow(self) -> bool:
        if self._opened_at is None:
            return True
        if time.monotonic() - self._opened_at >= self.cooldown_sec:
            # Half-open: let one probe through.
            return True
        return False

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold and self._opened_at is None:
            self._opened_at = time.monotonic()

    @property
    def state(self) -> str:
        if self._opened_at is None:
            return "closed"
        if time.monotonic() - self._opened_at >= self.cooldown_sec:
            return "half-open"
        return "open"


class CircuitOpenError(RuntimeError):
    """Raised when a call is short-circuited by an open breaker."""
