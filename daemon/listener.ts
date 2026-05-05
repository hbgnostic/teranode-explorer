import { createLibp2p, Libp2p } from 'libp2p';
import { tcp } from '@libp2p/tcp';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { bootstrap } from '@libp2p/bootstrap';
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { identify } from '@libp2p/identify';
import { ping } from '@libp2p/ping';
import { multiaddr } from '@multiformats/multiaddr';
import { generateKeyPair } from '@libp2p/crypto/keys';
import { Publisher, EventType, EventEnvelope } from './publisher.js';

const LIVE_PEERS = [
  '/dns4/teranode-eks-mainnet-us-1-p2p.bsvb.tech/tcp/9905/p2p/12D3KooWH5JVqGdaw7JEizmysCfRRcPGTFfvRJF7Hkure7oQWYnb',
  '/dns4/teranode-eks-mainnet-eu-1-p2p.bsvb.tech/tcp/9905/p2p/12D3KooW9z2JRV37TqsmU8sDQcSQDZGSgtPpvWUmVegYxYvXfW9H',
];

const TOPIC_PREFIX = 'teranode/bitcoin/1.0.0/mainnet-';
const TOPIC_KINDS: EventType[] = ['block', 'subtree', 'rejected_tx', 'node_status'];
// Observe-only topics: subscribed and logged for evaluation, not yet routed to Pub/Sub.
// Enable by leaving in this list; remove or feature-flag once we know what they carry.
const OBSERVE_KINDS = ['mining_on', 'bestblock'] as const;
type ObserveKind = typeof OBSERVE_KINDS[number];
const ALL_TOPIC_KINDS: (EventType | ObserveKind)[] = [...TOPIC_KINDS, ...OBSERVE_KINDS];
const TOPICS = ALL_TOPIC_KINDS.map((k) => TOPIC_PREFIX + k);

const PEER_LABELS: Record<string, string> = {
  '12D3KooWH5JVqGdaw7JEizmysCfRRcPGTFfvRJF7Hkure7oQWYnb': 'BSVB-US',
  '12D3KooW9z2JRV37TqsmU8sDQcSQDZGSgtPpvWUmVegYxYvXfW9H': 'BSVB-EU',
};
const labelOf = (peerId: string) => PEER_LABELS[peerId] ?? peerId.slice(-12);

const DRY_RUN = process.env.DRY_RUN === '1';
const REJECTION_LOG = process.env.REJECTION_LOG === '1';
const PROJECT_ID = process.env.GCP_PROJECT_ID;
const HEARTBEAT_INTERVAL_MS = 30_000;

const publisher = new Publisher({ projectId: PROJECT_ID, dryRun: DRY_RUN });

const counts: Record<EventType, number> = {
  block: 0, subtree: 0, rejected_tx: 0, node_status: 0,
};
const counts_at_last_heartbeat: Record<EventType, number> = { ...counts };

const node: Libp2p = await createLibp2p({
  privateKey: await generateKeyPair('Ed25519'),
  addresses: { listen: ['/ip4/0.0.0.0/tcp/0'] },
  transports: [tcp()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  peerDiscovery: [
    bootstrap({ list: LIVE_PEERS }),
    pubsubPeerDiscovery({ topics: TOPICS, interval: 5000 }),
  ],
  services: {
    identify: identify(),
    ping: ping(),
    pubsub: gossipsub({ allowPublishToZeroTopicPeers: true, emitSelf: false, fallbackToFloodsub: true }),
  },
});

console.log(`[boot] peer_id=${node.peerId.toString()} dry_run=${DRY_RUN} project=${PROJECT_ID ?? '(none)'}`);

node.addEventListener('peer:connect', (e) =>
  console.log(`[peer] connect ${labelOf(e.detail.toString())} peers=${node.getPeers().length}`),
);
node.addEventListener('peer:disconnect', (e) =>
  console.log(`[peer] disconnect ${labelOf(e.detail.toString())} peers=${node.getPeers().length}`),
);

const dialAttempted = new Set<string>();
node.addEventListener('peer:discovery', (evt) => {
  const id = evt.detail.id.toString();
  if (dialAttempted.has(id)) return;
  dialAttempted.add(id);
  if (node.getPeers().some((p) => p.toString() === id)) return;
  node.dial(evt.detail.id).then(
    () => console.log(`[peer] harvested ${labelOf(id)}`),
    () => {}, // silent — most discovered peers are firewalled
  );
});

const pubsub: any = node.services.pubsub;

// Outer envelope: { name, data } where data is base64-encoded JSON of the inner payload.
const unwrap = (data: Uint8Array): { publisher: string; inner: any } | null => {
  try {
    const outer = JSON.parse(new TextDecoder().decode(data));
    if (typeof outer?.name !== 'string' || typeof outer?.data !== 'string') return null;
    const inner = JSON.parse(Buffer.from(outer.data, 'base64').toString('utf-8'));
    return { publisher: outer.name, inner };
  } catch {
    return null;
  }
};

const shortHash = (h: string | undefined) => (h && h.length > 16 ? `${h.slice(0, 8)}…${h.slice(-6)}` : h ?? '?');

pubsub.addEventListener('gossipsub:message', async (evt: any) => {
  const { topic, data } = evt.detail.msg;
  const kindStr = topic.replace(TOPIC_PREFIX, '');
  if (!ALL_TOPIC_KINDS.includes(kindStr as any)) return;

  const wrapped = unwrap(data);
  if (!wrapped) return; // peer-discovery / unparseable noise

  // Observe-only topics: log a structured line for offline evaluation, then return.
  // Not counted in heartbeat totals, not published to Pub/Sub.
  if ((OBSERVE_KINDS as readonly string[]).includes(kindStr)) {
    console.log(`[observe:${kindStr}] ${JSON.stringify({
      ts: new Date().toISOString(),
      publisher: wrapped.publisher,
      relay: labelOf(evt.detail.propagationSource.toString()),
      data: wrapped.inner,
    })}`);
    return;
  }

  const kind = kindStr as EventType;

  const envelope: EventEnvelope = {
    type: kind,
    topic,
    publisher: wrapped.publisher,
    relay: labelOf(evt.detail.propagationSource.toString()),
    receivedAt: new Date().toISOString(),
    data: wrapped.inner,
  };

  counts[kind]++;
  await publisher.publish(envelope);

  // Per-rejection structured log. Independent of DRY_RUN; opt-in via
  // REJECTION_LOG=1 so journal volume only grows when we want a sample
  // for offline reason-distribution analysis.
  if (REJECTION_LOG && kind === 'rejected_tx') {
    const m: any = wrapped.inner;
    console.log(`[reject] ${JSON.stringify({
      ts: envelope.receivedAt,
      txid: m.TxID,
      reason: m.Reason,
      publisher: envelope.publisher,
      relay: envelope.relay,
    })}`);
  }

  // In dry-run, log each event so you can see the stream. In production we rely
  // on the 30s heartbeat aggregate to keep journal volume sane.
  if (DRY_RUN) {
    const m: any = wrapped.inner;
    const relay = envelope.relay;
    const pub = envelope.publisher;
    switch (kind) {
      case 'block':
        console.log(`🟦 BLOCK   h=${m.Height} hash=${shortHash(m.Hash)} miner=${m.ClientName ?? pub} relay=${relay}`);
        break;
      case 'subtree':
        console.log(`🌿 SUBTREE hash=${shortHash(m.Hash)} producer=${m.ClientName ?? pub} relay=${relay}`);
        break;
      case 'rejected_tx':
        console.log(`🚫 REJECTED tx=${shortHash(m.TxID)} reason="${(m.Reason ?? '?').slice(0, 80)}" by=${m.ClientName ?? pub} relay=${relay}`);
        break;
      case 'node_status':
        console.log(
          `📊 STATUS  ${m.client_name ?? pub} h=${m.best_height} state=${m.fsm_state} ` +
          `listen=${m.listen_mode} peers=${m.connected_peers_count} v=${m.version} relay=${relay}`,
        );
        break;
    }
  }
});

for (const topic of TOPICS) pubsub.subscribe(topic);
console.log(`[boot] subscribed ${ALL_TOPIC_KINDS.join(',')}`);

for (const addr of LIVE_PEERS) {
  node.dial(multiaddr(addr)).catch((err) => console.log(`[boot] dial failed ${err.message}`));
}

setInterval(() => {
  const delta = {
    block: counts.block - counts_at_last_heartbeat.block,
    subtree: counts.subtree - counts_at_last_heartbeat.subtree,
    rejected_tx: counts.rejected_tx - counts_at_last_heartbeat.rejected_tx,
    node_status: counts.node_status - counts_at_last_heartbeat.node_status,
  };
  console.log(
    `[heartbeat] peers=${node.getPeers().length} ` +
    `total{block=${counts.block} subtree=${counts.subtree} rejected_tx=${counts.rejected_tx} node_status=${counts.node_status}} ` +
    `last_30s{block=${delta.block} subtree=${delta.subtree} rejected_tx=${delta.rejected_tx} node_status=${delta.node_status}} ` +
    `failures=${JSON.stringify(publisher.failures())}`
  );
  Object.assign(counts_at_last_heartbeat, counts);
}, HEARTBEAT_INTERVAL_MS);

const shutdown = async (sig: string) => {
  console.log(`[shutdown] ${sig} received, stopping libp2p`);
  try { await node.stop(); } catch (err) { console.error('[shutdown] error stopping node', err); }
  console.log('[shutdown] done');
  process.exit(0);
};
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
