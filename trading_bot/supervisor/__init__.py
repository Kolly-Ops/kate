"""Top-level entry point — composes engine + dependencies + signal handling."""
from .runtime import KNOWN_INSTRUMENTS, InstrumentRuntime

__all__ = ["KNOWN_INSTRUMENTS", "InstrumentRuntime"]
