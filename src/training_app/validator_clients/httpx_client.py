from __future__ import annotations

import time
from typing import Any, Dict

from training_app.metrics import validator_latency_seconds, validator_requests_total
from training_app.validator_client import ValidatorResponse

try:
    import httpx
except Exception:  # pragma: no cover
    httpx = None


class HttpxValidatorClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        if httpx is None:
            raise RuntimeError("httpx is not installed. Install with: pip install -e '.[httpx]'")
        self.url = base_url.rstrip("/") + "/ingest"
        self.client = httpx.Client(timeout=timeout_seconds)

    def send(self, payload: Dict[str, Any]) -> ValidatorResponse:
        t0 = time.perf_counter()
        try:
            r = self.client.post(self.url, json=payload)
            dt = time.perf_counter() - t0
            validator_latency_seconds.observe(dt)

            if 200 <= r.status_code < 300:
                validator_requests_total.labels(outcome="ok").inc()
                return ValidatorResponse(r.status_code, True, r.text[:2000])
            if 400 <= r.status_code < 500:
                validator_requests_total.labels(outcome="4xx").inc()
                return ValidatorResponse(r.status_code, False, r.text[:2000])
            validator_requests_total.labels(outcome="5xx").inc()
            return ValidatorResponse(r.status_code, False, r.text[:2000])
        except Exception as e:
            dt = time.perf_counter() - t0
            validator_latency_seconds.observe(dt)
            validator_requests_total.labels(outcome="error").inc()
            return ValidatorResponse(0, False, f"exception: {type(e).__name__}: {e}")
