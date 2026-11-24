
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
