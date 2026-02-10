from __future__ import annotations

import json
import logging
import signal
import threading
import time
import requests
from typing import Any, Dict

from prometheus_client import start_http_server

from training_app.config import Config
from training_app.cpu_load import cpu_burn
from training_app.logging_setup import setup_logging
from training_app.sqs_reader import SqsReader
from training_app.validator_clients import HttpxValidatorClient, RequestsValidatorClient

log = logging.getLogger(__name__)

session = requests.Session()

class StopFlag:
    def __init__(self) -> None:
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def is_set(self) -> bool:
        return self._stop.is_set()


def _make_validator_client(cfg: Config):
    if cfg.validator_client.lower() == "httpx":
        return HttpxValidatorClient(cfg.validator_url, cfg.validator_timeout_seconds)
    return RequestsValidatorClient(cfg.validator_url, cfg.validator_timeout_seconds)


def _build_payload(cfg: Config, sqs_body: str, sqs_message_id: str) -> Dict[str, Any]:
    """
    Starter behaviour: pass through the original message content (parsed if possible),
    plus some metadata. Student will later add enrichment + signature fields.
    """
    meta = {
        "app_id": cfg.app_id,
        "sqs_message_id": sqs_message_id,
        "received_ts": int(time.time()),
    }
    try:
        obj = json.loads(sqs_body)
        if isinstance(obj, dict):
            obj.setdefault("meta", {})
            if isinstance(obj["meta"], dict):
                obj["meta"].update(meta)
            else:
                obj["meta"] = meta
            return obj
        # if it's JSON but not an object, wrap it
        return {"raw": obj, "meta": meta}
    except Exception:
        # malformed JSON; wrap it
        return {"raw_body": sqs_body[:2000], "meta": meta}

def _enrich_payload(base_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        user_id = payload['user_id']
        url = base_url.rstrip("/") + f"/v1/users/{user_id}"
        ref = session.get(url, timeout=(3.05, 10))
        data = ref.json()
        payload["enriched"] = data
        print(f"enrichment_api url={url} user_id={user_id}")
        print(f'payload after enrichment: {payload}')
    except Exception as e:
        log.warning("enrichment_api_exception url=%s type=%s err=%s", url, type(e).__name__, e)
    return payload

def main() -> None:
    cfg = Config.load()
    setup_logging(cfg.log_level)

    log.info("startup app_id=%s validator_client=%s metrics=%s:%s",
             cfg.app_id, cfg.validator_client, cfg.metrics_bind, cfg.metrics_port)

    # Metrics endpoint
    start_http_server(cfg.metrics_port, addr=cfg.metrics_bind)

    stop = StopFlag()

    def _handle_sig(_: int, __: Any) -> None:
        log.info("signal received, stopping")
        stop.stop()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    reader = SqsReader(
        queue_url=cfg.sqs_queue_url,
        region=cfg.aws_region,
        wait_seconds=cfg.receive_wait_seconds,
        max_batch=cfg.max_batch,
        visibility_timeout_seconds=cfg.visibility_timeout_seconds,
    )
    validator = _make_validator_client(cfg)

    while not stop.is_set():
        try:
            msgs = reader.receive()
        except Exception as e:
            log.warning("sqs_receive_error type=%s err=%s", type(e).__name__, e)
            time.sleep(1.0)
            continue

        if not msgs:
            continue


        for m in msgs:
            # t0 = time.perf_counter() not needed 
            try:
                payload = _build_payload(cfg, m.body, m.message_id)
                # enrichment here 
                payload = _enrich_payload(cfg.enrichment_api_url, payload)

                # CPU simulation (student may replace with more sophisticated work)
                if cfg.cpu_ms_per_message > 0:
                    # c0 = time.perf_counter() 
                    cpu_burn(cfg.cpu_ms_per_message)

                resp = validator.send(payload)

                if resp.ok:
                    # Delete message: processed successfully.
                    reader.delete(m.receipt_handle)
                else:
                    # Decide whether to delete based on status class.
                    status = resp.status_code
                    if 400 <= status < 500:
                        if cfg.delete_on_4xx:
                            reader.delete(m.receipt_handle)
                    elif 500 <= status < 600:
                        if cfg.delete_on_5xx:
                            reader.delete(m.receipt_handle)
                    else:
                        # status 0 or unknown
                        pass

                    # Log compactly; validator returns detailed reason anyway.
                    log.info("validator_reject status=%s msg_id=%s body=%s",
                             status, m.message_id, resp.body[:300].replace("\n", " "))

            except Exception as e:
                log.exception("processing_error msg_id=%s type=%s err=%s", m.message_id, type(e).__name__, e)

    log.info("shutdown complete")
