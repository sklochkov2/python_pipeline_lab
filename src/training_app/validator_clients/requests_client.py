from __future__ import annotations

import json
import time
from typing import Any, Dict

import requests

from training_app.metrics import validator_latency_seconds, validator_requests_total
from training_app.validator_client import ValidatorResponse


class RequestsValidatorClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.url = base_url.rstrip("/") + "/ingest"
        self.timeout = timeout_seconds
        self.session = requests.Session()

    def send(self, payload: Dict[str, Any]) -> ValidatorResponse:
        t0 = time.perf_counter()
        try:
            r = self.session.post(self.url, json=payload, timeout=self.timeout)
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
