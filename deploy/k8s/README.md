# Local Kubernetes deployment

This development deployment runs PostgreSQL and Redis in the local cluster. The API, ARQ worker, and outbox publisher run as separate workloads. It is intended for local learning and demos, not production hardening.

## Prerequisites

- Docker Engine available to WSL
- kubectl and kind installed in WSL
- A running kind cluster. The commands below use the current rate-limiter cluster name.
- Run commands from the repository root in WSL

## Build and deploy

~~~bash
docker build -t webhook-delivery-engine:dev .
kind load docker-image webhook-delivery-engine:dev --name rate-limiter
deploy/k8s/create-dev-secrets.sh
kubectl apply -f deploy/k8s/postgres.yaml -f deploy/k8s/redis.yaml
kubectl -n webhook-engine rollout status statefulset/postgres --timeout=180s
kubectl -n webhook-engine rollout status deployment/redis --timeout=120s
kubectl apply -f deploy/k8s/migration-job.yaml
kubectl -n webhook-engine wait --for=condition=complete job/webhook-migrations --timeout=180s
kubectl apply -f deploy/k8s/app.yaml
kubectl -n webhook-engine rollout status deployment/webhook-api --timeout=180s
kubectl -n webhook-engine rollout status deployment/webhook-worker --timeout=180s
kubectl -n webhook-engine rollout status deployment/webhook-outbox-publisher --timeout=180s
~~~

The script creates the database password and encryption key once in a Kubernetes Secret, without printing them. It keeps them when rerun so they stay consistent with the persisted PostgreSQL volume. WEBHOOK_ENCRYPTION_KEY encrypts endpoint signing secrets; keep the Secret if you want to keep existing endpoint data.

If your kind cluster has a different name, replace rate-limiter with the name shown by kind get clusters. Rebuild and reload the image each time you change application code. To rerun migrations later, first delete the completed Job with kubectl -n webhook-engine delete job webhook-migrations, then apply deploy/k8s/migration-job.yaml again.

## Verify and scale

~~~bash
kubectl -n webhook-engine get pods -o wide
kubectl -n webhook-engine get deployments
kubectl -n webhook-engine port-forward service/webhook-api 8000:8000
~~~

Leave port-forward running. In another WSL terminal:

~~~bash
curl -i http://127.0.0.1:8000/health/live
curl -i http://127.0.0.1:8000/health/ready
kubectl -n webhook-engine scale deployment/webhook-worker --replicas=2
kubectl -n webhook-engine rollout status deployment/webhook-worker
kubectl -n webhook-engine get pods -l app=webhook-worker
kubectl -n webhook-engine scale deployment/webhook-worker --replicas=1
~~~

/health/live checks that the API process responds. /health/ready checks PostgreSQL connectivity before Kubernetes sends traffic to the API. Uvicorn gets 30 seconds to stop requests cleanly; the worker gets 60 seconds and runs its ARQ shutdown hook. Worker readiness checks Redis connectivity. PostgreSQL and Redis have their own health probes.

To observe worker replacement and graceful termination, scale the worker to two replicas, delete one worker pod, and watch Kubernetes create a replacement:

~~~bash
kubectl -n webhook-engine delete pod -l app=webhook-worker --wait=false
kubectl -n webhook-engine get pods -w -l app=webhook-worker
~~~

## Cleanup

Deleting the namespace deletes this deployment and its PostgreSQL test data:

~~~bash
kubectl delete namespace webhook-engine
~~~

This local setup has single-instance PostgreSQL and Redis, no TLS/ingress, no autoscaler, and development secrets managed in a Kubernetes Secret. Production deployment needs durable backups, managed stateful services, external secret management, network policy, and resource tuning.


## Live delivery updates

The API exposes tenant-scoped WebSockets at /v1/ws/deliveries. Connect to ws://127.0.0.1:8000/v1/ws/deliveries after port-forwarding, then send an initial JSON frame containing type=authenticate and the tenant API key. The server validates the key, subscribes to that tenant's Redis Pub/Sub channel, and replies with an authenticated frame. Worker status changes are published after the database transaction commits and are delivered to all connected API replicas.

Keep the API key in the first WebSocket message; do not put it in the URL. Pub/Sub notifications are live best-effort updates, not a durable event log. After reconnecting, query the delivery status API to catch up on updates missed while disconnected.

## OpenTelemetry traces

The Kustomize resources include a local Jaeger all-in-one pod and service. The API, outbox publisher, and worker send OTLP/HTTP traces to `http://jaeger:4318`. The development ConfigMap script sets `OTEL_EXPORTER_OTLP_ENDPOINT` for new local clusters. For an existing cluster, update the ConfigMap and restart the three app deployments after applying the Jaeger resource:

```bash
kubectl apply -f deploy/k8s/jaeger.yaml
kubectl -n webhook-engine patch configmap webhook-engine-config --type merge -p '{"data":{"OTEL_EXPORTER_OTLP_ENDPOINT":"http://jaeger:4318"}}'
kubectl -n webhook-engine rollout restart deployment/webhook-api deployment/webhook-worker deployment/webhook-outbox-publisher
kubectl -n webhook-engine port-forward svc/jaeger 16686:16686
```

Open <http://localhost:16686>, select `webhook-api`, and search for a recent `POST /v1/events` trace. The trace should continue through `outbox.publish`, `webhook.delivery`, and an HTTPX client span. Jaeger uses transient in-memory storage for this development setup, so traces are cleared when the Jaeger pod restarts. Logs remain in pod stdout; correlate them by `trace_id` and `span_id`.
