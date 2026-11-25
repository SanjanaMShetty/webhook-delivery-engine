
## Phase 2: tenant API, endpoints, and event ingestion

The API uses tenant-scoped bearer API keys. Create a local tenant and key after applying migrations:

~~~bash
python -m scripts.create_tenant "Local Demo"
~~~

Copy the one-time key from the terminal and keep it private. In that same WSL terminal, set it for your current shell session:

~~~bash
export WEBHOOK_API_KEY='paste-the-key-here'
~~~

Run the API:

~~~bash
uvicorn app.main:app --reload
~~~

Open <http://127.0.0.1:8000/docs> for the interactive API reference. Register an endpoint (use a receiver URL you control):

~~~bash
curl -X POST http://127.0.0.1:8000/v1/endpoints \
  -H "Authorization: Bearer $WEBHOOK_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"Order receiver","url":"https://example.com/webhook","event_types":["order.created"]}'
~~~

Submit an event:

~~~bash
curl -X POST http://127.0.0.1:8000/v1/events \
  -H "Authorization: Bearer $WEBHOOK_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt-001","event_type":"order.created","payload":{"order_id":"ord-123"}}'
~~~

A successful request stores the event and, for each active matching endpoint, creates a queued delivery and its pending PostgreSQL outbox row in the same transaction. Repeating the same event_id and body returns the existing event and delivery IDs; reusing that ID with different event content returns HTTP 409. The background publisher and delivery worker are implemented in later phases.

For local development, endpoint registration currently accepts HTTP and HTTPS URLs, and event types may include * to subscribe to all event types. Outbound URL safety checks and encrypted signing-secret management are later security/delivery work; do not use untrusted URLs or store signing secrets yet.

## Phase 3: durable webhook delivery

The PostgreSQL outbox publisher leases due rows with SKIP LOCKED and enqueues deterministic ARQ job IDs. The ARQ worker posts the stored event bytes over HTTP, persists each attempt, and schedules exponential retries through the outbox. Transient network errors, HTTP 408/425/429 and 5xx responses are retried; other non-2xx responses become permanent failures. Exhausted retries become dead. POST /v1/deliveries/{delivery_id}/replay starts a fresh retry budget while retaining prior attempt history. Workers and publisher are separate processes.

Run the API, worker, and publisher in three terminals from the repository root (activate .venv in each):

    uvicorn app.main:app --reload
    arq app.worker.WorkerSettings
    python -m app.outbox_publisher

Ensure PostgreSQL and Redis from docker compose up -d are running first. Configure MAX_DELIVERY_ATTEMPTS, HTTP_TIMEOUT_SECONDS, RETRY_BASE_SECONDS, RETRY_MAX_SECONDS as needed.

## Webhook request signing
Each webhook endpoint must include a signing secret (at least 16 characters) when created, or set one through the endpoint update API. The service encrypts it in PostgreSQL with Fernet using WEBHOOK_ENCRYPTION_KEY. Generate a local key with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" and set WEBHOOK_ENCRYPTION_KEY in your untracked .env file. Keep the same key across API and worker processes; losing or rotating it without re-encrypting stored secrets makes those secrets unreadable.
Requests include X-Webhook-Timestamp and X-Webhook-Signature headers. The signature is sha256= plus the HMAC-SHA256 hex digest over timestamp, a dot, and the raw request body. Verify using constant-time comparison and reject stale timestamps at the receiver.

### Retry and dead-letter failure simulation

Run a local receiver that returns HTTP 500 for its first two requests, then HTTP 200:

    source .venv/bin/activate
    python scripts/failure_receiver.py --port 9001 --fail-first 2 --status 500

Register http://127.0.0.1:9001/webhook as an endpoint subscribed to the test event type. Publish an event and inspect it with GET /v1/deliveries/{delivery_id}. The attempt history should show two retryable HTTP 500 responses followed by a successful HTTP 200. Stop the receiver with Ctrl+C. Use --fail-first 99 to drive a delivery to the configured dead-letter limit; replay it with POST /v1/deliveries/{delivery_id}/replay after changing or restarting the receiver. This receiver is for local development only.

### Endpoint protection and isolation

The API accepts only HTTP or HTTPS webhook URLs, rejects URL credentials and local or non-public IP destinations, and checks DNS results again in the worker immediately before delivery. HTTPX does not follow redirects. Private destinations are disabled by default. For a local-only receiver, set ALLOW_PRIVATE_WEBHOOK_TARGETS=true for both API and worker processes; keep it false for normal use.

Per-endpoint distributed request limits use Redis and can be configured in the environment:

- ENDPOINT_RATE_LIMIT_PER_SECOND (default 10; fixed one-second window)
- ENDPOINT_CONCURRENCY_LIMIT (default 5; active request leases expire after the HTTP timeout plus a safety margin)
- CIRCUIT_BREAKER_FAILURE_THRESHOLD (default 5 retryable failures)
- CIRCUIT_BREAKER_COOLDOWN_SECONDS (default 30 seconds)

The breaker pauses new attempts after the failure threshold, allows one half-open probe after the cooldown, closes after a successful probe, and reopens after another retryable failure. A limiter or open breaker reschedules the delivery through the outbox without counting a network attempt. DNS checks reduce SSRF exposure but do not pin the resolved address through the HTTP connection; production deployment should also enforce outbound network rules.

## Analytics and operations

The API exposes Prometheus metrics at /metrics. The ARQ worker and outbox publisher expose their process metrics on ports 9101 and 9102 by default. Each process has its own counters, so scrape all three targets to see HTTP, delivery-attempt, and outbox activity. The API metrics route is unauthenticated; restrict access to it and the worker/outbox metrics ports to your monitoring network when deploying. Metric labels use bounded values such as route templates and status classes; payloads, API keys, signing secrets, and destination URLs are never metric labels.

Logs are JSON on stdout. API request logs include a request ID returned in the X-Request-ID response header; send your own safe correlation value with that header when debugging. Worker completion logs include delivery, event, and endpoint IDs. Request and response bodies are intentionally omitted.

FastAPI request spans and HTTPX delivery spans are available through OpenTelemetry. The API and worker spans can be correlated by the delivery ID in worker spans and logs; the current version does not propagate a single trace across the durable outbox boundary. Traces are disabled for export by default. Set OTEL_EXPORTER_OTLP_ENDPOINT to an OpenTelemetry Collector endpoint to export OTLP/HTTP traces. For a local console demonstration, set OTEL_CONSOLE_TRACES=true before starting the API and worker.

### Local observability checks

With the API, worker, outbox publisher, PostgreSQL, and Redis running:

```bash
curl -i -H 'X-Request-ID: phase7-health-check' http://127.0.0.1:8000/health
curl -sS http://127.0.0.1:8000/metrics | grep webhook_http_requests_total
curl -sS http://127.0.0.1:9101/metrics | grep webhook_delivery_attempts_total
curl -sS http://127.0.0.1:9102/metrics | grep webhook_outbox_published_total
```

The request should return the same request ID in its response header. After delivering a test event, the worker metrics should show attempt outcomes and the outbox metrics should show published records. Check terminal output for JSON request and attempt-completion logs.

### Recovery scenarios

Use the existing local failure receiver to make the first delivery return HTTP 500 and later requests succeed:

```bash
python scripts/failure_receiver.py --host 127.0.0.1 --port 9001 --fail-first 1 --status 500
```

In a second terminal, temporarily allow private webhook destinations in the local development environment, restart the API and worker, register an endpoint at http://127.0.0.1:9001/webhook, and submit an event. Confirm that attempt history records a retryable failure followed by success, and that worker metrics count both outcomes. Restore ALLOW_PRIVATE_WEBHOOK_TARGETS=false after the local simulation.

For a worker restart scenario, stop the ARQ worker while an event is queued, submit an event, then start the worker again. The durable outbox republishes queued work. If the worker stopped during an active attempt, allow the lease recovery window (90 seconds) and confirm the delivery is retried or reaches its terminal state through the delivery status endpoint. For Redis recovery, stop and start the local Redis Compose service and confirm the outbox publisher resumes publishing pending records after Redis returns:

```bash
docker compose stop redis
docker compose start redis
```

These are local development exercises. Avoid running the service-stop scenarios against shared or production data.

## Local Kubernetes deployment

Phase 8 deploys the API, ARQ workers, and outbox publisher as separate Kubernetes workloads, with PostgreSQL and Redis in the local cluster. Follow the build, migration, health-check, scaling, and cleanup steps in [deploy/k8s/README.md](deploy/k8s/README.md).

## Distributed tracing

The API, outbox publisher, and ARQ worker emit OpenTelemetry traces to Jaeger. The API writes W3C trace context into each outbox row in the same database transaction as its delivery record. The publisher continues that trace when it enqueues the ARQ job, and the worker continues it through the HTTPX webhook request. Structured logs include trace and span IDs for correlation. Jaeger is included for local development and uses in-memory storage, so traces disappear when its pod restarts.

For the local Kubernetes viewer, run `kubectl -n webhook-engine port-forward svc/jaeger 16686:16686` and open <http://localhost:16686>. Services use `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318` inside the cluster. Jaeger stores traces only; application logs remain in Kubernetes stdout and metrics remain available from the existing Prometheus metrics endpoints.
