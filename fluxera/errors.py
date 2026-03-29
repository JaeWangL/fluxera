from __future__ import annotations


class FluxeraError(Exception):
    """Base class for Fluxera errors."""


class BrokerError(FluxeraError):
    """Base class for broker-related errors."""


class ActorNotFound(BrokerError):
    """Raised when a broker cannot resolve an actor by name."""


class QueueNotFound(BrokerError):
    """Raised when a broker cannot resolve a queue by name."""


class WorkerError(FluxeraError):
    """Raised when the worker runtime encounters an invalid state."""


class RemoteExecutionError(WorkerError):
    """Raised when a subprocess cannot propagate an exception cleanly."""

    def __init__(self, message: str, *, traceback_text: str | None = None) -> None:
        super().__init__(message)
        self.traceback_text = traceback_text


class RateLimitExceeded(FluxeraError):
    """Raised when a rate limiter cannot acquire a slot."""
