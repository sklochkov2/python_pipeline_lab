# Training lab: student app development guide

This repository contains the **student application** for the training lab. Your job is to read messages from **SQS**, enrich/process them, and submit results to the **validator**.

You will iterate by pushing commits. The system auto-deploys your changes and you’ll see feedback via:

* Grafana dashboards (metrics)
* CloudWatch logs (your app + validator)
* Telegram notifications (deploy success/failure)

---

## 1) Architecture overview

**Generator → SQS → your app → validator**

1. **Message generator** (infra host) produces synthetic messages and pushes them to **SQS**.
2. **Your app** (student host) consumes from SQS, does processing/enrichment, and POSTs the result to the validator.
3. **Validator** (infra host) checks correctness and logs accept/reject reasons.

Key services live on the **private VPC network**. You don’t need to expose anything publicly.

---

## 2) Service endpoints (private network)

### Validator (infra host)

* Base URL: `http://<INFRA_PRIVATE_IP>:9200`
* Endpoint:

  * `POST /ingest` — submit processed message for validation

### User enrichment API (infra host)

* Base URL: `http://<INFRA_PRIVATE_IP>:9102`
* Endpoints:

  * `GET /healthz`
  * `GET /v1/users/{user_id}`
  * `POST /v1/users:batch` (optional / recommended for batching)
  * `GET /metrics` (if enabled by infra)

### Prometheus / Grafana (infra host)

* You will typically access Grafana through an SSH tunnel or reverse proxy (handled by the instructor).

> Replace `<INFRA_PRIVATE_IP>` with the infra VM private IP you were given (e.g. `172.31.15.193`).

---

## 3) Message flow: what you receive and what you must produce

### 3.1 Input messages (from SQS)

Your app reads JSON strings from SQS. The “raw message” is a JSON document.

**Minimum fields you should expect:**

* `message_id` (string) — unique message id
* `user_id` (string/int) — used for enrichment
* `event_ts` (unix epoch seconds) or equivalent
* `payload` (object) — message-specific fields

> The exact shape may evolve; always parse defensively, validate required fields, and surface errors clearly in logs.

### 3.2 Enrichment (via user enrichment API)

For each message, you will call enrichment API for user info and merge it into the outgoing message.

#### Single-user call

**Request:**
`GET /v1/users/{user_id}`

**200 response example:**

```json
{
  "user_id": "12345",
  "segment": "power_user",
  "country": "DE",
  "city": "Berlin",
  "age_bucket": "25-34",
  "risk_score": 0.17,
  "is_premium": true,
  "interests": ["ml", "devops", "sports"],
  "signup_ts": 1735689600,
  "features": {
    "has_verified_email": true,
    "has_two_factor": false
  },
  "meta": {
    "request_id": "01H...",
    "scenario": "io_mild",
    "latency_ms": 73,
    "simulated_error": null
  }
}
```

**Non-200 responses:**

* `404` — unknown user (you must decide: drop? default? retry? mark invalid?)
* `429` — rate limit (honour `Retry-After` if present; consider backoff)
* `500` — transient server error (retry policy)
* `503` — brownout/unavailable (retry/backoff/circuit-breaker decisions)

#### Batch call (recommended if you implement batching)

**Request:**
`POST /v1/users:batch` with JSON:

```json
{"user_ids": ["123", "456", "789"]}
```

**200 response example:**

```json
{
  "users": [
    {"user_id":"123", "...":"..."},
    {"user_id":"456", "...":"..."}
  ],
  "missing": ["789"],
  "errors": [
    {"user_id":"999", "code":"rate_limited", "message":"..."}
  ],
  "meta": {"request_id":"...","scenario":"..."}
}
```

---

## 4) Validator API: how to submit results

### Endpoint

`POST http://<INFRA_PRIVATE_IP>:9200/ingest`

### Request

Your app should POST a JSON document representing the processed message.

At the start of the lab, the “boilerplate” app forwards messages without enrichment, so the validator rejects them. Your goal is to satisfy the validator’s rules (added progressively during the lab).

### Response

* `2xx` — accepted
* `4xx` — rejected (usually means your output is structurally wrong / missing fields)
* `5xx` — validator problem (rare; treat as transient)

When rejected, the validator returns a short reason and logs a detailed reason (viewable in CloudWatch/Grafana).

---

## 5) What you are expected to implement in the student app

You will iterate through multiple stages, usually:

1. **Basic correctness**

   * Parse SQS messages reliably
   * Enrich with user profile data (single or batch)
   * Send correct JSON to validator

2. **Error handling**

   * Handle enrichment API errors (retry/backoff)
   * Decide what to do with poison messages
   * Avoid deleting SQS messages that haven’t been successfully processed (unless configured)

3. **Performance**

   * Increase throughput without breaking correctness
   * Experiment with concurrency patterns:

     * sequential vs threads vs async vs hybrid
     * batching user enrichment
     * caching repeated user_ids
     * controlling concurrency limits (avoid overload)
   * Optionally compare HTTP clients (requests vs httpx vs aiohttp)

4. **Observability (later)**

   * Add minimal app metrics (optional at first)
   * Design logs that help you debug quickly

---

## 6) Local reasoning / design constraints

### Python interpreter constraints (important)

* Python’s GIL limits parallelism for CPU-bound work in threads.
* I/O concurrency is often achieved via async or threads, but “more concurrency” can make things worse (connection pools, scheduling overhead, timeouts).
* For CPU-bound sections, consider multiprocessing (but only when needed).

### Common patterns you’ll likely use

* Connection pooling (http client session)
* Concurrency limiters (semaphores / worker pools)
* Retry with exponential backoff + jitter
* Circuit breaker / “pause intake” when downstream is failing
* Batching + caching to reduce enrichment calls

---

## 7) Configuration (env vars)

Your app runs on the student host under systemd and reads configuration from an environment file.

Typical env vars you may see (names may vary depending on implementation):

### SQS

* `AWS_REGION`
* `SQS_QUEUE_URL`
* `SQS_RECEIVE_WAIT_SECONDS`
* `SQS_MAX_BATCH`
* `SQS_VISIBILITY_TIMEOUT_SECONDS`

### External services

* `VALIDATOR_URL` (e.g. `http://172.31.15.193:9200`)
* `ENRICH_URL` (e.g. `http://172.31.15.193:9102`)
* Optional: client timeouts / concurrency settings

### Behaviour flags (examples)

* `DELETE_ON_4XX` — delete message even if validator rejects (usually **false** during development)
* `DELETE_ON_5XX` — delete message on server errors (usually **false**)

---

## 8) Scenarios (enrichment API)

The enrichment API can simulate different scenarios (configured by the instructor). Common scenarios you’ll encounter:

* `io_mild`: low latency, rare errors
* `io_heavy`: higher latency and tail
* `flaky`: more frequent 5xx/429/timeouts, some 404s
* `brownout`: periodic periods of very high latency and 503 spikes

Your app should behave robustly across these scenarios.

---

## 9) Debugging workflow

### What to do when validator rejects

1. Find your app logs (CloudWatch / Grafana logs panel).
2. Identify the reject reason from validator response and/or validator logs.
3. Inspect the outgoing JSON payload you sent.
4. Fix your processing logic and push another commit.

### What to do when performance is poor

1. Check CPU usage patterns on Grafana:

   * one core pegged vs multi-core usage
2. Check request latencies and error spikes (enrichment API + validator)
3. Reduce or increase concurrency; measure again.
4. Profile locally if needed, then translate improvements to production run.

---

## 10) Curl examples for manual exploration

### Enrichment

```bash
curl -fsS http://<INFRA_PRIVATE_IP>:9102/healthz
curl -fsS http://<INFRA_PRIVATE_IP>:9102/v1/users/123 | jq .
curl -fsS http://<INFRA_PRIVATE_IP>:9102/v1/users:batch \
  -H 'content-type: application/json' \
  -d '{"user_ids":["123","456","789"]}' | jq .
```

### Validator

```bash
curl -fsS http://<INFRA_PRIVATE_IP>:9200/ingest \
  -H 'content-type: application/json' \
  -d '{"hello":"world"}' -v
```

---

## 11) Development expectations and etiquette

* Keep commits small and focused: “one hypothesis per commit”.
* When you change concurrency levels or retry logic, note it in commit message (makes comparing outcomes easier).
* Prefer clear logging around:

  * message id
  * user id
  * enrichment outcome
  * validator outcome

---

## 12) Glossary

* **SQS**: queue used for input messages.
* **Enrichment API**: synthetic upstream service that returns user profiles and simulates latency/errors.
* **Validator**: checks your output; you “win” by getting messages accepted at high throughput.
* **Scenario**: a preset for enrichment behaviour (latency/errors) used to test your resilience.

