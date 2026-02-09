from __future__ import annotations

import os
from dataclasses import dataclass


def _getenv(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v is not None else default


def _getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v is not None else default


def _getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Config:
    # identity
    app_id: str

    # sqs
    sqs_queue_url: str
    aws_region: str | None
    receive_wait_seconds: int
    max_batch: int
    visibility_timeout_seconds: int

    # validator
    validator_url: str
    validator_timeout_seconds: float
    validator_client: str  # requests|httpx

    # cpu simulation
    cpu_ms_per_message: int

    # metrics
    metrics_bind: str
    metrics_port: int

    # behaviour
    delete_on_4xx: bool
    delete_on_5xx: bool
    log_level: str

    @staticmethod
    def load() -> "Config":
        return Config(
            app_id=_getenv("APP_ID", "student-app"),

            sqs_queue_url=_getenv("SQS_QUEUE_URL"),
            aws_region=os.getenv("AWS_REGION"),
            receive_wait_seconds=_getenv_int("SQS_RECEIVE_WAIT_SECONDS", 10),
            max_batch=_getenv_int("SQS_MAX_BATCH", 10),
            visibility_timeout_seconds=_getenv_int("SQS_VISIBILITY_TIMEOUT_SECONDS", 60),

            validator_url=_getenv("VALIDATOR_URL"),
            validator_timeout_seconds=_getenv_float("VALIDATOR_TIMEOUT_SECONDS", 3.0),
            validator_client=_getenv("VALIDATOR_CLIENT", "requests"),

            cpu_ms_per_message=_getenv_int("CPU_MS_PER_MESSAGE", 0),

            metrics_bind=_getenv("METRICS_BIND", "127.0.0.1"),
            metrics_port=_getenv_int("METRICS_PORT", 9301),

            delete_on_4xx=_getenv_bool("DELETE_ON_4XX", True),
            delete_on_5xx=_getenv_bool("DELETE_ON_5XX", False),

            log_level=_getenv("LOG_LEVEL", "INFO"),
        )
