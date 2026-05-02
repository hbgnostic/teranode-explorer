# SSE bridge

Cloud Run service that subscribes to the four Teranode Pub/Sub topics and fans messages out to browser clients via Server-Sent Events.

Single endpoint: `GET /stream` — emits `block`, `subtree`, `rejected_tx`, `node_status` SSE events. Auxiliary `GET /health` returns 200 with current client count.

## Architecture

Single-instance design (Cloud Run `min=1, max=1`). All SSE clients share the same instance and the same Pub/Sub subscription state. Horizontal scaling is intentionally disabled because multi-instance would require per-instance subscriptions and message deduplication.

## Local dev

```bash
export GCP_PROJECT_ID=traceport-production-20251021
npm install
npm run dev
```

Then in another terminal:

```bash
curl -N http://localhost:8080/stream
```

You should see `event: hello` immediately, then live events as they flow.

## Deploy

```bash
gcloud builds submit --tag us-east4-docker.pkg.dev/$PROJECT/traceport-images/teranode-explorer-api:latest .
gcloud run deploy teranode-explorer-api \
  --image us-east4-docker.pkg.dev/$PROJECT/traceport-images/teranode-explorer-api:latest \
  --region us-east4 \
  --allow-unauthenticated \
  --min-instances 1 \
  --max-instances 1 \
  --cpu 1 --memory 512Mi \
  --no-cpu-throttling \
  --set-env-vars GCP_PROJECT_ID=$PROJECT
```
