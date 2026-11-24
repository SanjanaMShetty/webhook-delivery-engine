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
