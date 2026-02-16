from __future__ import annotations
import os
import json
import logging
import signal
import time
import aiohttp
import asyncio
from multiprocessing import Process, Queue, Event
from typing import Any, Dict

from prometheus_client import start_http_server

from training_app.config import Config
from training_app.cpu_load import cpu_burn
from training_app.logging_setup import setup_logging
from training_app.sqs_reader import SqsReader
from training_app.validator_clients import HttpxValidatorClient, RequestsValidatorClient

log = logging.getLogger(__name__)


def _make_validator_client(cfg: Config):
    if cfg.validator_client.lower() == "httpx":
        return HttpxValidatorClient(cfg.validator_url, cfg.validator_timeout_seconds)
    return RequestsValidatorClient(cfg.validator_url, cfg.validator_timeout_seconds)


def _build_payload(cfg: Config, sqs_body: str, sqs_message_id: str) -> Dict[str, Any]:
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
        return {"raw": obj, "meta": meta}
    except Exception:
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


async def _process_message(
    session: aiohttp.ClientSession,
    cfg: Config,
    validator,
    reader: SqsReader,
    m
) -> None:
    try:
        payload = _build_payload(cfg, m.body, m.message_id)
        
        # Enrich (async I/O)
        payload = await _enrich_payload(session, cfg.enrichment_api_url, payload)

        # CPU work (no pool needed - runs directly in worker process)
        if cfg.cpu_ms_per_message > 0:
            cpu_burn(cfg.cpu_ms_per_message)
        
        # Validate (run in thread executor since validator.send is sync)
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, validator.send, payload)

        should_delete = resp.ok or \
                       (400 <= resp.status_code < 500 and cfg.delete_on_4xx) or \
                       (500 <= resp.status_code < 600 and cfg.delete_on_5xx)
        
        if should_delete:
            await loop.run_in_executor(None, reader.delete, m.receipt_handle)
        elif not resp.ok:
            log.info("validator_reject status=%s msg_id=%s body=%s",
                     resp.status_code, m.message_id, resp.body[:300].replace("\n", " "))

    except Exception as e:
        log.exception("processing_error msg_id=%s type=%s err=%s", m.message_id, type(e).__name__, e)


async def process_messages_batch(
    session: aiohttp.ClientSession,
    cfg: Config,
    validator,
    reader: SqsReader,
    msgs: list
) -> None:
    """Process batch concurrently within worker."""
    tasks = [
        _process_message(session, cfg, validator, reader, m)
        for m in msgs
    ]
    await asyncio.gather(*tasks)


async def async_worker_loop(cfg: Config, stop_event: Event, msg_queue: Queue) -> None:
    reader = SqsReader(
        queue_url=cfg.sqs_queue_url,
        region=cfg.aws_region,
        wait_seconds=cfg.receive_wait_seconds,
        max_batch=cfg.max_batch,
        visibility_timeout_seconds=cfg.visibility_timeout_seconds,
    )
    validator = _make_validator_client(cfg)
    
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            try:
                msgs = []
                try:
                    while len(msgs) < cfg.max_batch:
                        msg = msg_queue.get_nowait()
                        if msg is None:  # Poison pill
                            stop_event.set()
                            break
                        msgs.append(msg)
                except:
                    pass  # Queue empty
                
                if not msgs:
                    await asyncio.sleep(0.1)
                    continue

                # Process batch with all messages concurrently
                await process_messages_batch(session, cfg, validator, reader, msgs)

            except Exception as e:
                log.exception("worker_error type=%s err=%s", type(e).__name__, e)
                await asyncio.sleep(1.0)

    log.info("worker shutdown complete")


def worker_process(cfg: Config, stop_event: Event, msg_queue: Queue, worker_id: int) -> None:
    # Setup logging for worker
    setup_logging(cfg.log_level)
    log.info(f"Worker {worker_id} started")
    
    # Create and run async event loop
    asyncio.run(async_worker_loop(cfg, stop_event, msg_queue))


def main() -> None:
    cfg = Config.load()
    setup_logging(cfg.log_level)

    log.info("startup app_id=%s validator_client=%s metrics=%s:%s",
             cfg.app_id, cfg.validator_client, cfg.metrics_bind, cfg.metrics_port)

    # Metrics endpoint
    start_http_server(cfg.metrics_port, addr=cfg.metrics_bind)

    # Multiprocessing primitives
    num_workers = os.cpu_count()
    if num_workers is None:
        num_workers = 4
    
    stop_event = Event()
    msg_queue = Queue(maxsize=1000)
    
    print(f"Starting {num_workers} worker processes")
    
    # Start worker processes
    workers = []
    for i in range(num_workers):
        p = Process(target=worker_process, args=(cfg, stop_event, msg_queue, i))
        p.start()
        workers.append(p)
    
    # Setup signal handlers
    def _handle_sig(_: int, __: Any) -> None:
        log.info("signal received, stopping")
        stop_event.set()
        # Send poison pills
        for _ in range(num_workers):
            msg_queue.put(None)

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    
    # Main process: read from SQS and distribute to workers
    reader = SqsReader(
        queue_url=cfg.sqs_queue_url,
        region=cfg.aws_region,
        wait_seconds=cfg.receive_wait_seconds,
        max_batch=cfg.max_batch,
        visibility_timeout_seconds=cfg.visibility_timeout_seconds,
    )
    

    while not stop_event.is_set():
        try:
            msgs = reader.receive()
        except Exception as e:
            log.warning("sqs_receive_error type=%s err=%s", type(e).__name__, e)
            time.sleep(1.0)
            continue

        if not msgs:
            continue

        # Distribute messages to workers via queue
        for msg in msgs:
            msg_queue.put(msg)
            
    
    # Wait for workers to finish
    for p in workers:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
    
    log.info("shutdown complete")


