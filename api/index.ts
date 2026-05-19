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

// If set, /operators requires an X-API-Key header matching this value.
// /stream remains open so the live UI keeps working. Unset → endpoint open.
const OPERATORS_API_KEY = process.env.OPERATORS_API_KEY;

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

// Operator state, built from incoming node_status events. Exposed at
// GET /operators for downstream consumers (e.g. the BSV Intel Report's
// daily n8n workflow) that want a snapshot of the current network
// composition without subscribing to Pub/Sub themselves.
type OperatorSnapshot = {
  peer_id: string;
  peer_label: string;
  client_name: string;
  base_url: string;
  version: string;
  fsm_state: string;
  listen_mode: string;
  storage: string;
  best_height: number;
  miner_name: string;
  connected_peers_count: number;
  is_relay: boolean;
  last_seen: string;
};
const operators = new Map<string, OperatorSnapshot>();

// Two operators whose libp2p peer IDs ALSO serve as BSVB's public
// gossip-forwarding endpoints. See daemon/listener.ts PEER_LABELS.
const RELAY_PEER_IDS = new Set([
  '12D3KooWH5JVqGdaw7JEizmysCfRRcPGTFfvRJF7Hkure7oQWYnb', // BSVB-US (op: bsva-mainnet-eu-2)
  '12D3KooW9z2JRV37TqsmU8sDQcSQDZGSgtPpvWUmVegYxYvXfW9H', // BSVB-EU (op: bsva-mainnet-eu-1)
]);

const updateOperatorFromEnvelope = (envelopeJson: string): void => {
  try {
    const env = JSON.parse(envelopeJson);
    const m = env?.data;
    const peerId: string | undefined = m?.peer_id;
    if (!peerId) return;
    operators.set(peerId, {
      peer_id: peerId,
      peer_label: env?.peer_label || m?.client_name || peerId,
      client_name: m?.client_name || '',
      base_url: m?.base_url || '',
      version: m?.version || '',
      fsm_state: m?.fsm_state || '',
      listen_mode: m?.listen_mode || '',
      storage: m?.storage || '',
      best_height: Number(m?.best_height) || 0,
      miner_name: m?.miner_name || '',
      connected_peers_count: Number(m?.connected_peers_count) || 0,
      is_relay: RELAY_PEER_IDS.has(peerId),
      last_seen: env?.receivedAt || new Date().toISOString(),
    });
  } catch {
    // ignore malformed envelopes
  }
};

const broadcast = (eventType: string, data: string) => {
  if (eventType === 'node_status') updateOperatorFromEnvelope(data);

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

// Snapshot of the current set of operators announcing on the mesh, built
// from incoming node_status events. Stale entries are kept in the map
// (an operator that stops broadcasting won't disappear automatically);
// consumers can use `last_seen` to filter as they see fit.
// Gated by OPERATORS_API_KEY env var when set.
app.get('/operators', (req: Request, res: Response) => {
  if (OPERATORS_API_KEY) {
    const provided = req.headers['x-api-key'];
    if (provided !== OPERATORS_API_KEY) {
      res.status(401).json({ error: 'unauthorized' });
      return;
    }
  }
  res.setHeader('Access-Control-Allow-Origin', '*');
  const sorted = [...operators.values()].sort((a, b) =>
    a.client_name.localeCompare(b.client_name) || a.peer_id.localeCompare(b.peer_id),
  );
  res.json({
    generated_at: new Date().toISOString(),
    count: sorted.length,
    operators: sorted,
  });
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
