# Webhook Delivery Engine

A multi-tenant webhook delivery service built as a portfolio project to explore reliable asynchronous processing, failure recovery, and cloud-native operations.

**Python · FastAPI · PostgreSQL · SQLAlchemy · Alembic · Redis · ARQ · HTTPX · HMAC-SHA256 · Docker · Kubernetes · OpenTelemetry**

## Overview

Applications publish events once. The engine finds active endpoints subscribed to each event type, creates a delivery for every match, and sends signed HTTP requests asynchronously. It records each attempt and supports retry, dead-letter handling, and manual replay.

The project focuses on practical distributed-systems concerns: a transactional outbox between PostgreSQL and Redis, at-least-once delivery, tenant isolation, endpoint protection, and trace correlation across the queue boundary.

## Architecture

~~~mermaid
flowchart LR
    Producer -->|Bearer API key| API[FastAPI]
    API -->|one transaction| DB[(PostgreSQL)]
    DB --> Events[Events and deliveries]
    DB --> Outbox[Transactional outbox]
    Outbox --> Publisher[Outbox publisher]
    Publisher -->|enqueue| Queue[(Redis and ARQ)]
    Queue --> Worker[Delivery worker]
    Worker -->|HTTP POST with HMAC| Endpoint[Subscriber endpoint]
    Worker -->|attempts and status| DB
    API --> WS[WebSocket clients]
    API -->|OTLP traces| Jaeger[Jaeger]
    Publisher -->|OTLP traces| Jaeger
    Worker -->|OTLP traces| Jaeger
~~~

The API documentation is available at /docs while the API is running.

## One event, end to end

A producer sends one authenticated `POST /v1/events` request. The webhook calls happen asynchronously, so event acceptance does not wait for every receiver to respond.

1. **Authenticate and validate.** FastAPI checks the tenant API key and validates the event body.
2. **Deduplicate and fan out.** The engine uses the event ID to detect a repeated submission, then finds active endpoints subscribed to the event type (including wildcard subscriptions).
3. **Commit durable work.** PostgreSQL stores the event, one delivery per matching endpoint, and corresponding outbox rows in a single transaction. The API returns the event ID and delivery IDs.
4. **Queue each delivery.** The outbox publisher claims pending rows and enqueues ARQ jobs in Redis. If publishing is interrupted, uncompleted outbox rows can be picked up again.
5. **Send the webhook.** An ARQ worker loads the delivery, signs the request with the endpoint's HMAC secret, and sends it with HTTPX.
6. **Record and report the outcome.** The worker stores the attempt and delivery status in PostgreSQL. Retryable failures are scheduled with backoff; exhausted deliveries become dead. Status changes are published through Redis Pub/Sub to connected WebSocket clients.

Follow a delivery with `GET /v1/deliveries/{delivery_id}`. The API also exposes metrics at `/metrics`, while OpenTelemetry traces can be explored in Jaeger when the local observability stack is running.

## Features

| Area | What is implemented |
|---|---|
| Event ingestion | Tenant-scoped bearer API keys, request validation, and event idempotency |
| Fan-out | Match event types to active endpoints, including wildcard subscriptions |
| Durable queueing | Write events, deliveries, and outbox messages in one PostgreSQL transaction; publish outbox work to Redis/ARQ |
| Delivery | ARQ worker sends HTTP POST requests with HTTPX and signs requests with HMAC-SHA256 |
| Reliability | Bounded retries with backoff, dead-letter state, delivery history, and manual replay |
| Protection | Destination URL checks, per-endpoint rate and concurrency limits, and a circuit breaker |
| Operations | Health and status APIs, Prometheus metrics, structured logs, WebSocket delivery updates, and OpenTelemetry traces |

## Delivery lifecycle

~~~mermaid
stateDiagram-v2
    [*] --> queued
    queued --> delivering
    delivering --> delivered: HTTP success
    delivering --> retrying: retryable failure
    retrying --> delivering: backoff elapsed
    delivering --> dead: attempts exhausted
    dead --> queued: manual replay
~~~

Delivery is **at least once**. If a receiver processes a request but the worker cannot record the response, a retry may send it again. Receivers should be idempotent.

## Technology choices

| Technology | Role |
|---|---|
| [FastAPI](https://fastapi.tiangolo.com/) | Async HTTP API, validation integration, and OpenAPI docs |
| [PostgreSQL](https://www.postgresql.org/docs/) | Durable source of truth for tenants, keys, events, endpoints, deliveries, attempts, and outbox records |
| [SQLAlchemy](https://docs.sqlalchemy.org/en/21/) + [Alembic](https://alembic.sqlalchemy.org/en/latest/) | Database access and versioned schema migrations |
| [Redis](https://redis.io/docs/latest/) + [ARQ](https://arq-docs.helpmanual.io/) | Background job queue and cross-instance WebSocket Pub/Sub |
| [HTTPX](https://www.python-httpx.org/) | Async outbound webhook requests |
| [HMAC-SHA256](https://www.rfc-editor.org/rfc/rfc2104) | Sign webhook requests so receivers can verify their origin and integrity |
| [Docker Compose](https://docs.docker.com/compose/) | Run [PostgreSQL](https://www.postgresql.org/docs/) and [Redis](https://redis.io/docs/latest/) for local development |
| [Kubernetes](https://kubernetes.io/docs/) (kind) | Run the API, worker, and outbox publisher as separate workloads |
| [OpenTelemetry](https://opentelemetry.io/docs/languages/python/) + [Jaeger](https://www.jaegertracing.io/docs/) | Create, propagate, export, and inspect distributed traces |
| [Prometheus](https://prometheus.io/docs/introduction/overview/) + [Ruff](https://docs.astral.sh/ruff/) + [GitHub Actions](https://docs.github.com/en/actions) | Metrics endpoint, linting, and CI checks |

## Quick start: local development

### Prerequisites

- Python 3.13 or newer
- Docker Desktop with Docker Compose
- Git

### 1. Clone and prepare Python

~~~bash
git clone <your-repository-url>
cd webhook-delivery-engine
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
~~~

Set WEBHOOK_ENCRYPTION_KEY in .env to a Fernet key. Generate one with:

~~~bash
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
~~~

Keep .env private and do not commit it.

### 2. Start PostgreSQL and Redis

~~~bash
docker compose up -d postgres redis
~~~

Apply the schema migrations:

~~~bash
alembic upgrade head
~~~

### 3. Create a local tenant and API key

The script creates a tenant and prints a new API key once. Save it privately. The database stores a hash, so the original key cannot be retrieved later.

~~~bash
python -m scripts.create_tenant "Local Demo"
read -rsp "Paste the one-time API key: " WEBHOOK_API_KEY; echo
export WEBHOOK_API_KEY
~~~

### 4. Start the API and background processes

Run each process in a separate terminal from the project directory, with the virtual environment activated.

~~~bash
# Terminal 1: API
uvicorn app.main:app --reload
~~~

~~~bash
# Terminal 2: ARQ delivery worker
arq app.worker.WorkerSettings
~~~

~~~bash
# Terminal 3: transactional outbox publisher
python -m app.outbox_publisher
~~~

Open http://127.0.0.1:8000/docs for interactive API documentation. The local API health endpoint is http://127.0.0.1:8000/health.

### 5. Send a test webhook

For a local receiver, use the included failure receiver in another terminal:

~~~bash
python scripts/failure_receiver.py --host 127.0.0.1 --port 9001 --fail-first 0 --status 200
~~~

Because this receiver uses a private local address, set ALLOW_PRIVATE_WEBHOOK_TARGETS=true in .env for local development only, then restart both the API and worker. Keep this setting disabled outside local testing.

Register an endpoint. The signing secret must be at least 16 characters:

~~~bash
curl -sS -X POST http://127.0.0.1:8000/v1/endpoints \
  -H "Authorization: Bearer $WEBHOOK_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"Local receiver","url":"http://127.0.0.1:9001/webhook","event_types":["order.created"],"signing_secret":"local-test-signing-secret-2026"}'
~~~

Submit an event with a unique event ID:

~~~bash
EVENT_ID="demo-$(python -c 'import uuid; print(uuid.uuid4())')"
curl -sS -X POST http://127.0.0.1:8000/v1/events \
  -H "Authorization: Bearer $WEBHOOK_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"event_id\":\"$EVENT_ID\",\"event_type\":\"order.created\",\"payload\":{\"order_id\":\"$EVENT_ID\",\"amount\":42}}"
~~~

The response includes delivery IDs for matched endpoints. Use one to inspect its status and attempt history:

~~~bash
export DELIVERY_ID="<delivery_id from the event response>"
curl -sS "http://127.0.0.1:8000/v1/deliveries/$DELIVERY_ID" \
  -H "Authorization: Bearer $WEBHOOK_API_KEY"
~~~

A replay can be requested for an eligible failed delivery:

~~~bash
curl -sS -X POST "http://127.0.0.1:8000/v1/deliveries/$DELIVERY_ID/replay" \
  -H "Authorization: Bearer $WEBHOOK_API_KEY"
~~~

## API reference

All tenant-protected routes use a bearer API key:

~~~http
Authorization: Bearer <tenant-api-key>
~~~

| Method | Route | Purpose |
|---|---|---|
| POST | /v1/endpoints | Register a webhook endpoint |
| GET | /v1/endpoints | List tenant endpoints |
| GET | /v1/endpoints/{endpoint_id} | Read an endpoint |
| PATCH | /v1/endpoints/{endpoint_id} | Update endpoint settings |
| DELETE | /v1/endpoints/{endpoint_id} | Disable an endpoint |
| POST | /v1/events | Accept an event and create matched deliveries |
| GET | /v1/events/{event_id} | Read event details and delivery status |
| GET | /v1/deliveries/{delivery_id} | Read delivery status and attempt history |
| POST | /v1/deliveries/{delivery_id}/replay | Replay an eligible failed delivery |
| GET | /health/live | Liveness check |
| GET | /health/ready | Readiness check |
| GET | /metrics | API Prometheus metrics |
| WS | /v1/ws/deliveries | Tenant-authenticated live delivery updates |

The endpoint event_types list accepts explicit event names and the wildcard * subscription. See /docs for request and response schemas.

## Request signing

Webhook requests include a signature header generated with the endpoint's signing secret. Receivers should verify the signature against the raw request body and reject invalid signatures. Do not log or expose signing secrets. The test receiver is for local development; it does not demonstrate receiver-side signature verification.

## Observability

The API, outbox publisher, and delivery worker emit OpenTelemetry traces. Trace context is persisted with each outbox record and propagated through ARQ, allowing an event's API span, publisher span, worker span, and outbound HTTPX span to be inspected as one trace.

- In the local Kubernetes setup, Jaeger receives OTLP traces. Follow the setup and port-forward steps indeploy/k8s/README.md, then open http://localhost:16686.
- Search for the webhook-api service and open a recent event trace.
- Metrics are exposed through Prometheus endpoints. Logs are structured JSON on standard output and include trace and span IDs for correlation.
- The development Jaeger instance uses in-memory storage; its traces disappear when it restarts. Kubernetes logs remain available through kubectl logs.

## Kubernetes deployment

The local kind deployment runs PostgreSQL, Redis, the API, ARQ workers, the outbox publisher, and Jaeger. The API and background processes run as separate Kubernetes workloads. This is a learning and demo setup, not a production-hardened deployment.

See [deploy/k8s/README.md](deploy/k8s/README.md) for cluster creation, secrets, image loading, migrations, health checks, scaling, WebSocket updates, tracing, and cleanup instructions.

## Data model

- Tenant and API key: isolate clients and authenticate their requests; only a key hash is stored.
- Endpoint: destination URL, event subscriptions, status, and encrypted signing secret.
- Event: accepted producer event and payload.
- Delivery: one event-to-endpoint work item and its current state.
- DeliveryAttempt: outcome, response status, timing, and error for each attempt.
- OutboxMessage: durable handoff from a committed PostgreSQL transaction to ARQ.

## Project structure

~~~text
app/
  api/                  FastAPI routes and authentication
  models.py             SQLAlchemy models
  observability.py      OpenTelemetry and structured logging setup
  outbox_publisher.py   PostgreSQL outbox to ARQ publisher
  worker.py             Async webhook delivery worker
  realtime.py           WebSocket delivery updates
migrations/             Alembic schema history
scripts/                Tenant setup and local failure receiver
deploy/k8s/            kind and Kubernetes manifests and guide
~~~

## Tradeoffs and guarantees

- **At-least-once delivery:** retries and worker recovery can cause duplicate requests; exactly-once delivery to arbitrary HTTP receivers is not promised.
- **PostgreSQL is authoritative:** Redis/ARQ moves work efficiently, while the outbox allows pending work to be republished after queue interruptions.
- **Local-first operations:** kind and in-memory Jaeger make the project easy to run, but are not substitutes for production secrets management, persistent telemetry storage, or cloud availability planning.
- **Outbound URL checks have limits:** DNS checks reduce SSRF exposure, but production should also enforce outbound network policy and prevent DNS rebinding.

## Future improvements

- Add measured load and recovery benchmarks, including duplicate-delivery behavior.
- Use persistent trace storage and an OpenTelemetry Collector for multi-signal routing.
- Add tenant quotas, operator tooling for dead-letter inspection, and stronger production network controls.
- Add deployment automation and verify behavior under multi-node and database-failure scenarios.
