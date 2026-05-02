// SSE bridge: subscribe to four Pub/Sub topics and fan messages out to
// connected browser clients via Server-Sent Events.
//
// Designed to run as a single Cloud Run instance (min=1, max=1) so all SSE
// clients share the same Pub/Sub subscription state. Scaling beyond one
// instance would require per-instance subscriptions and message dedup.

import express, { Request, Response } from 'express';
import { PubSub, Message } from '@google-cloud/pubsub';

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

// Subscriptions are created on first run; reused thereafter.
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

const ensureSubscription = async (topic: string) => {
  const subName = SUB_NAME(topic);
  const sub = pubsub.subscription(subName);
  const [exists] = await sub.exists();
  if (!exists) {
    console.log(`creating subscription ${subName}`);
    await pubsub.topic(topic).createSubscription(subName, {
      ackDeadlineSeconds: 20,
      messageRetentionDuration: { seconds: 600 }, // 10 min — short, this is a live feed
    });
  }
  return pubsub.subscription(subName);
};

const startSubscriber = async (topic: string) => {
  const sub = await ensureSubscription(topic);
  sub.on('message', (message: Message) => {
    const eventType = message.attributes?.type ?? 'unknown';
    broadcast(eventType, message.data.toString());
    message.ack();
  });
  sub.on('error', (err: Error) => console.error(`[${topic}] subscription error:`, err.message));
  console.log(`subscribed to ${topic}`);
};

const app = express();

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

(async () => {
  for (const topic of TOPICS) await startSubscriber(topic);
  app.listen(PORT, () => console.log(`SSE bridge listening on :${PORT}`));
})().catch((err) => {
  console.error('fatal startup error:', err);
  process.exit(1);
});
