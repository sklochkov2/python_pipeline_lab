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

log = logging.getLogger(__name__)


class StopFlag:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def is_set(self) -> bool:
        return self._stop.is_set()


async def _all_process_message_async(
    cfg_dict: Dict[str, Any],
    msg_body: str,
    msg_id: str,
    enrichment_url: str,
    validator_url: str,
    validator_timeout: float
) -> Tuple[bool, int, str, str]:  
    try:
        # Build payload
        meta = {
            "app_id": cfg_dict["app_id"],
            "sqs_message_id": msg_id,
            "received_ts": int(time.time()),
        }
        try:
            obj = json.loads(msg_body)
            if isinstance(obj, dict):
                obj.setdefault("meta", {})
                if isinstance(obj["meta"], dict):
                    obj["meta"].update(meta)
                else:
                    obj["meta"] = meta
                payload = obj
            else:
                payload = {"raw": obj, "meta": meta}
        except Exception:
            payload = {"raw_body": msg_body[:2000], "meta": meta}
        
        async with aiohttp.ClientSession() as session:
            # Enrich payload
            try:
                user_id = payload['user_id']
                url = enrichment_url.rstrip("/") + f"/v1/users/{user_id}"
                timeout = aiohttp.ClientTimeout(total=10, connect=3.05)
                
                async with session.get(url, timeout=timeout) as response:
                    data = await response.json()
                    data['tier'] = payload['expected']['tier']
                    data['country'] = payload['expected']['country']
                    payload["enriched"] = data
            except Exception as e:
                log.warning("enrichment_api_exception url=%s type=%s err=%s", url, type(e).__name__, e)
            
            if cfg_dict.get("cpu_ms_per_message", 0) > 0:
                cpu_burn(cfg_dict["cpu_ms_per_message"])
            
            async with session.post(validator_url, json=payload, timeout=validator_timeout) as response:
                return (response.ok, response.status, "", "")
        
    except Exception as e:
        return (False, 0, "", str(e))


def _worker_with_async_loop(
    cfg_dict: Dict[str, Any],
    msg_body: str,
    msg_id: str,
    enrichment_url: str,
    validator_url: str,
    validator_timeout: float
) -> Tuple[bool, int, str, str]:

    # Create new event loop for this worker
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(
            _all_process_message_async(cfg_dict, msg_body, msg_id, 
                                  enrichment_url, validator_url, validator_timeout)
        )
        return result
    finally:
        loop.close()



async def _process_message_with_async_worker(
    cfg: Config,
    reader: SqsReader,
    msg,
    loop: asyncio.AbstractEventLoop,
    pool: Any
) -> None:
    """Process message in worker using aiohttp (most efficient for I/O-heavy)."""
    try:
        cfg_dict = {
            "app_id": cfg.app_id,
            "cpu_ms_per_message": cfg.cpu_ms_per_message,
            "delete_on_4xx": cfg.delete_on_4xx,
            "delete_on_5xx": cfg.delete_on_5xx,
        }
        
        # Submit to worker with async event loop
        async_result = pool.apply_async(
            _worker_with_async_loop,
            (cfg_dict, msg.body, msg.message_id, cfg.enrichment_api_url, 
             cfg.validator_url, cfg.validator_timeout_seconds)
        )
        
        success, status_code, _, error_msg = await loop.run_in_executor(None, async_result.get)
        
        if success:
            await loop.run_in_executor(None, reader.delete, msg.receipt_handle)
        else:
            should_delete = (400 <= status_code < 500 and cfg.delete_on_4xx) or \
                           (500 <= status_code < 600 and cfg.delete_on_5xx)
            
            if should_delete:
                await loop.run_in_executor(None, reader.delete, msg.receipt_handle)
            
            if status_code > 0:
                log.info("validator_reject status=%s msg_id=%s", status_code, msg.message_id)
            elif error_msg:
                log.error("processing_error msg_id=%s err=%s", msg.message_id, error_msg)
    
    except Exception as e:
        log.exception("processing_error msg_id=%s type=%s err=%s", 
                     msg.message_id, type(e).__name__, e)





async def _process_messages_batch_in_async_workers(
    cfg: Config,
    reader: SqsReader,
    msgs: list,
    loop: asyncio.AbstractEventLoop,
    pool: Any
) -> None:
    """Process all messages in worker processes with aiohttp (best for I/O-heavy)."""
    await asyncio.gather(*[
        _process_message_with_async_worker(cfg, reader, msg, loop, pool)
        for msg in msgs
    ])


async def async_main_loop(cfg: Config, stop: StopFlag, mode: str = "async_workers") -> None:
    """
    Main async event loop with multiprocessing.Pool for CPU work.
    
    Args:
        cfg: Configuration
        stop: Stop flag
        mode: Processing mode:
            - "async_workers": Workers use aiohttp (best for I/O-heavy)
            - "sync_workers": Workers use requests (simpler)
            - "async_main": Main process uses asyncio, workers only for CPU
    """
    reader = SqsReader(
        queue_url=cfg.sqs_queue_url,
        region=cfg.aws_region,
        wait_seconds=cfg.receive_wait_seconds,
        max_batch=cfg.max_batch,
        visibility_timeout_seconds=cfg.visibility_timeout_seconds,
    )
    loop = asyncio.get_event_loop()

    # Create multiprocessing.Pool for CPU-bound work
    num_processes = None  # Defaults to CPU count
    
    with Pool(processes=num_processes) as pool:
        print(f"Process pool size: {pool._processes}")
        print(f"Mode: {mode}")
        
        # Mode 1: Workers use aiohttp (best for I/O-heavy workloads)
        while not stop.is_set():
            try:
                msgs = await loop.run_in_executor(None, reader.receive)
            except Exception as e:
                log.warning("sqs_receive_error type=%s err=%s", type(e).__name__, e)
                await asyncio.sleep(1.0)
                continue

            if not msgs:
                continue

            await _process_messages_batch_in_async_workers(cfg, reader, msgs, loop, pool)
            
        
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

    # Run the async event loop
    asyncio.run(async_main_loop(cfg, stop))