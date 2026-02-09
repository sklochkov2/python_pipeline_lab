from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Protocol


@dataclass(frozen=True)
class ValidatorResponse:
    status_code: int
    ok: bool
    body: str
