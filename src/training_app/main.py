from __future__ import annotations

import json
import logging
import signal
import time
import aiohttp
import asyncio
from multiprocessing import Pool
from typing import Any, Dict, Tuple

from prometheus_client import start_http_server

from training_app.config import Config
from training_app.cpu_load import cpu_burn
from training_app.logging_setup import setup_logging
from training_app.sqs_reader import SqsReader
from training_app.validator_clients import HttpxValidatorClient, RequestsValidatorClient

log = logging.getLogger(__name__)


class StopFlag:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def is_set(self) -> bool:
        return self._stop.is_set()


# def _make_validator_client(cfg: Config):
#     if cfg.validator_client.lower() == "httpx":
#         return HttpxValidatorClient(cfg.validator_url, cfg.validator_timeout_seconds)
#     return RequestsValidatorClient(cfg.validator_url, cfg.validator_timeout_seconds)


def _build_payload(cfg_dict: Dict[str, Any], sqs_body: str, sqs_message_id: str) -> Dict[str, Any]:
    """
    Starter behaviour: pass through the original message content (parsed if possible),
    plus some metadata.
    """
    meta = {
        "app_id": cfg_dict["app_id"],
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


async def _enrich_payload(session: aiohttp.ClientSession, base_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        user_id = payload['user_id']
        url = base_url.rstrip("/") + f"/v1/users/{user_id}"
        
        timeout = aiohttp.ClientTimeout(total=10, connect=3.05)
        async with session.get(url, timeout=timeout) as response:
            data = await response.json()
            data['tier'] = payload['expected']['tier']
            data['country'] = payload['expected']['country']
            payload["enriched"] = data
            
    except Exception as e:
        log.warning("enrichment_api_exception url=%s type=%s err=%s", url, type(e).__name__, e)
    
    return payload


async def _process_in_worker_async(
    cfg_dict: Dict[str, Any],
    msg_body: str,
    msg_id: str
) -> Tuple[bool, int, str]:

    try:
        payload = _build_payload(cfg_dict, msg_body, msg_id)
        
        async with aiohttp.ClientSession() as session:
            # Enrich
            payload = await _enrich_payload(session, cfg_dict["enrichment_api_url"], payload)
            
            if cfg_dict.get("cpu_ms_per_message", 0) > 0:
                cpu_burn(cfg_dict["cpu_ms_per_message"])
            
            # Validate using validator client
            if cfg_dict["validator_client"].lower() == "httpx":
                validator = HttpxValidatorClient(cfg_dict["validator_url"], cfg_dict["validator_timeout_seconds"])
            else:
                validator = RequestsValidatorClient(cfg_dict["validator_url"], cfg_dict["validator_timeout_seconds"])
            
            import asyncio
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, validator.send, payload)
            
            return (resp.ok, resp.status_code, "")
    
    except Exception as e:
        return (False, 0, str(e))


def _worker_wrapper(cfg_dict: Dict[str, Any], msg_body: str, msg_id: str) -> Tuple[bool, int, str]:
    """Runs async function in worker process."""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_process_in_worker_async(cfg_dict, msg_body, msg_id))
    finally:
        loop.close()


async def _process_message(
    cfg: Config,
    reader: SqsReader,
    m,
    loop: asyncio.AbstractEventLoop, 
    pool: Any
) -> None:
    try:
        # Prepare config dict
        cfg_dict = {
            "app_id": cfg.app_id,
            "cpu_ms_per_message": cfg.cpu_ms_per_message,
            "enrichment_api_url": cfg.enrichment_api_url,
            "validator_url": cfg.validator_url,
            "validator_timeout_seconds": cfg.validator_timeout_seconds,
            "validator_client": cfg.validator_client,
        }
        
        # Submit to worker
        async_result = pool.apply_async(_worker_wrapper, (cfg_dict, m.body, m.message_id))
        success, status_code, error_msg = await loop.run_in_executor(None, async_result.get)
        
        # Handle result
        should_delete = success or \
                       (400 <= status_code < 500 and cfg.delete_on_4xx) or \
                       (500 <= status_code < 600 and cfg.delete_on_5xx)
        
        if should_delete:
            await loop.run_in_executor(None, reader.delete, m.receipt_handle)
        elif not success and status_code > 0:
            log.info("validator_reject status=%s msg_id=%s", status_code, m.message_id)
        elif error_msg:
            log.error("processing_error msg_id=%s err=%s", m.message_id, error_msg)

    except Exception as e:
        log.exception("processing_error msg_id=%s type=%s err=%s", m.message_id, type(e).__name__, e)


async def process_messages_batch(
    cfg: Config,
    reader: SqsReader,
    msgs: list,
    loop: asyncio.AbstractEventLoop, 
    pool: Any
) -> None:
    tasks = [
        _process_message(cfg, reader, m, loop, pool)
        for m in msgs
    ]
    await asyncio.gather(*tasks)


async def async_main_loop(cfg: Config, stop: StopFlag) -> None:
    reader = SqsReader(
        queue_url=cfg.sqs_queue_url,
        region=cfg.aws_region,
        wait_seconds=cfg.receive_wait_seconds,
        max_batch=cfg.max_batch,
        visibility_timeout_seconds=cfg.visibility_timeout_seconds,
    )
    loop = asyncio.get_event_loop()

    num_processes = None  # Defaults to CPU count
    
    with Pool(processes=num_processes) as pool:
        print(f"Process pool size: {pool._processes}")
        
        while not stop.is_set():
            try:
                msgs = await loop.run_in_executor(None, reader.receive)
            except Exception as e:
                log.warning("sqs_receive_error type=%s err=%s", type(e).__name__, e)
                await asyncio.sleep(1.0)
                continue

            if not msgs:
                continue

            await process_messages_batch(cfg, reader, msgs, loop, pool)

    log.info("shutdown complete")


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

    asyncio.run(async_main_loop(cfg, stop))


    