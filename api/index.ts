// SSE bridge: subscribe to four Pub/Sub topics and fan messages out to
// connected browser clients via Server-Sent Events.
//
// Designed to run as a single Cloud Run instance (min=1, max=1) so all SSE
// clients share the same Pub/Sub subscription state. Scaling beyond one
// instance would require per-instance subscriptions and message dedup.

import express, { Request, Response } from 'express';
import { PubSub, Message } from '@google-cloud/pubsub';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const PORT = parseInt(process.env.PORT ?? '8080', 10);
const PROJECT_ID = process.env.GCP_PROJECT_ID;
if (!PROJECT_ID) {
  console.error('GCP_PROJECT_ID env required');
  process.exit(1);
}

// Topic names match what the listener daemon publishes to.
const TOPICS = [
  'teranode-block',
  'teranode-subtree',
  'teranode-rejected-tx',
  'teranode-node-status',
] as const;

// Subscription naming convention. Subs are pre-created (gcloud pubsub
// subscriptions create) so the runtime SA only needs roles/pubsub.subscriber
// — which lacks subscriptions.get/create. We attach to subs by name and rely
// on consume permission; if a sub is missing, the error surfaces at message
// time and the service stays healthy on the others.
const SUB_NAME = (topic: string) => `${topic}-sse-bridge`;

const pubsub = new PubSub({ projectId: PROJECT_ID });

type SSEClient = { id: number; res: Response };
const clients: SSEClient[] = [];
let nextClientId = 1;

const broadcast = (eventType: string, data: string) => {
  const payload = `event: ${eventType}\ndata: ${data}\n\n`;
  // Iterate by index so removals during iteration are safe.
  for (let i = clients.length - 1; i >= 0; i--) {
    try {
      clients[i].res.write(payload);
    } catch {
      // Client write failed — let req:close handler clean it up.
    }
  }
};

const startSubscriber = (topic: string) => {
  const subName = SUB_NAME(topic);
  const sub = pubsub.subscription(subName);
  sub.on('message', (message: Message) => {
    const eventType = message.attributes?.type ?? 'unknown';
    broadcast(eventType, message.data.toString());
    message.ack();
  });
  sub.on('error', (err: Error) => console.error(`[${subName}] subscription error:`, err.message));
  console.log(`subscribed to ${subName}`);
};

const app = express();

const __dirname = dirname(fileURLToPath(import.meta.url));
app.use(express.static(resolve(__dirname, 'public')));

app.get('/health', (_req: Request, res: Response) => {
  res.json({ ok: true, clients: clients.length, topics: TOPICS });
});

app.get('/stream', (req: Request, res: Response) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  const id = nextClientId++;
  clients.push({ id, res });
  console.log(`[client ${id}] connect; total=${clients.length}`);

  // Initial hello so the client knows the stream is live.
  res.write(`event: hello\ndata: ${JSON.stringify({ id, ts: new Date().toISOString() })}\n\n`);

  // Comment line every 25s acts as keep-alive through proxies.
  const keepalive = setInterval(() => {
    try { res.write(': keepalive\n\n'); } catch {}
  }, 25_000);

  req.on('close', () => {
    clearInterval(keepalive);
    const idx = clients.findIndex((c) => c.id === id);
    if (idx !== -1) clients.splice(idx, 1);
    console.log(`[client ${id}] disconnect; total=${clients.length}`);
  });
});

for (const topic of TOPICS) startSubscriber(topic);
app.listen(PORT, () => console.log(`SSE bridge listening on :${PORT}`));
