from __future__ import annotations

import hashlib
import time


def cpu_burn(ms: int) -> None:
    """
    Deterministic-ish CPU burn in pure Python.
    This is intentionally not 'fast'; it's meant to consume CPU in a controllable way.

    NOTE: It uses time-based stopping, so cost scales with CPU speed.
    """
    if ms <= 0:
        return

    deadline = time.perf_counter() + (ms / 1000.0)
    x = b"seed"
    # Do repeated hashing; this produces Python-level overhead + C hashing work.
    while time.perf_counter() < deadline:
        x = hashlib.sha256(x).digest()
