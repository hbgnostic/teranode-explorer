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
import { peerIdFromString } from '@libp2p/peer-id';
import { Publisher, EventType, EventEnvelope } from './publisher.js';
import { startBroadcaster } from './broadcaster.js';

const LIVE_PEERS = [
  '/dns4/teranode-eks-mainnet-us-1-p2p.bsvb.tech/tcp/9905/p2p/12D3KooWH5JVqGdaw7JEizmysCfRRcPGTFfvRJF7Hkure7oQWYnb',
  '/dns4/teranode-eks-mainnet-eu-1-p2p.bsvb.tech/tcp/9905/p2p/12D3KooW9z2JRV37TqsmU8sDQcSQDZGSgtPpvWUmVegYxYvXfW9H',
];

// Direct peers bypass gossipsub mesh-management and scoring penalties. The two
// BSVB relays are trusted upstream infrastructure, not arbitrary peers. Marking
// them as direct prevents the GraftBackoff penalty death spiral (where their
// re-GRAFTs during our backoff window earned them -10 behaviour score and
// caused us to PRUNE-then-penalize them in a loop). The library handles the
// non-reciprocal case explicitly (we don't appear in their direct list).
const DIRECT_PEERS = LIVE_PEERS.map((addr) => {
  const ma = multiaddr(addr);
  const peerIdStr = ma.getPeerId();
  if (!peerIdStr) throw new Error(`LIVE_PEERS entry missing /p2p/<peerId>: ${addr}`);
  return { id: peerIdFromString(peerIdStr), addrs: [ma] };
});

const TOPIC_PREFIX = 'teranode/bitcoin/1.0.0/mainnet-';
const TOPIC_KINDS: EventType[] = ['block', 'subtree', 'rejected_tx', 'node_status'];
const TOPICS = TOPIC_KINDS.map((k) => TOPIC_PREFIX + k);

// Public-facing labels for the two BSVB relay endpoints. These are DNS-hostname
// labels, used in the [peer] connect/disconnect and [mesh] GRAFT/PRUNE log lines
// where the meaningful identity is "which BSVB door is this." The operators
// running BEHIND these peer IDs self-identify in their node_status broadcasts as
// `bsva-mainnet-eu-1` (the BSVB-EU peer ID) and `bsva-mainnet-eu-2` (the BSVB-US
// peer ID, despite the "US" hostname — BSVB's naming, not ours). Per-event
// publisher resolution uses operatorMap below, not these labels.
const PEER_LABELS: Record<string, string> = {
  '12D3KooWH5JVqGdaw7JEizmysCfRRcPGTFfvRJF7Hkure7oQWYnb': 'BSVB-US',
  '12D3KooW9z2JRV37TqsmU8sDQcSQDZGSgtPpvWUmVegYxYvXfW9H': 'BSVB-EU',
};
const labelOf = (peerId: string) => PEER_LABELS[peerId] ?? peerId.slice(-12);

// Runtime map of libp2p peer_id → operator self-identity, built from incoming
// node_status broadcasts. Used by resolvePeerLabel() to attach a friendly
// `peer_label` to every outgoing Pub/Sub envelope. New operators appearing on
// the mesh populate this automatically on their first heartbeat.
type OperatorInfo = { client_name: string; base_url: string; last_seen: number };
const operatorMap = new Map<string, OperatorInfo>();

const hostnameFromUrl = (url: string | undefined): string | null => {
  if (!url) return null;
  try { return new URL(url).hostname || null; } catch { return null; }
};

// Resolve the best-effort friendly identifier for whoever originated a message.
// Tried in order: (1) operator map lookup by peer_id, (2) in-message client_name,
// (3) outer-envelope publisher, (4) hostname from a URL inside the message, (5)
// short peer_id prefix. Three of eight operators currently broadcast the generic
// `"teranode"` client_name; the URL fallback gives them readable identifiers.
const resolvePeerLabel = (publisherName: string, data: any): string => {
  // node_status uses snake_case peer_id; block + rejected_tx use PascalCase PeerID.
  const peerId: string | undefined = data?.peer_id || data?.PeerID;
  if (peerId) {
    const op = operatorMap.get(peerId);
    if (op?.client_name && op.client_name !== 'teranode') return op.client_name;
    const opHost = hostnameFromUrl(op?.base_url);
    if (opHost) return opHost;
  }
  const inMsgName: string | undefined = data?.client_name || data?.ClientName;
  if (inMsgName && inMsgName !== 'teranode') return inMsgName;
  if (publisherName && publisherName !== 'teranode') return publisherName;
  const inMsgHost = hostnameFromUrl(data?.DataHubURL || data?.base_url);
  if (inMsgHost) return inMsgHost;
  if (peerId) return `peer:${peerId.slice(-12)}`;
  return 'unknown';
};

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
    pubsub: gossipsub({
      allowPublishToZeroTopicPeers: true,
      emitSelf: false,
      fallbackToFloodsub: true,
      directPeers: DIRECT_PEERS,
    }),
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
  if (!TOPIC_KINDS.includes(kindStr as EventType)) return;
  const kind = kindStr as EventType;

  const wrapped = unwrap(data);
  if (!wrapped) return; // peer-discovery / unparseable noise

  // Refresh the operator map first so resolvePeerLabel sees this broadcast.
  if (kind === 'node_status') {
    const peerId = wrapped.inner?.peer_id;
    if (typeof peerId === 'string' && peerId.length > 0) {
      operatorMap.set(peerId, {
        client_name: wrapped.inner?.client_name || '',
        base_url: wrapped.inner?.base_url || '',
        last_seen: Date.now(),
      });
    }
  }

  const envelope: EventEnvelope = {
    type: kind,
    topic,
    publisher: wrapped.publisher,
    relay: labelOf(evt.detail.propagationSource.toString()),
    receivedAt: new Date().toISOString(),
    data: wrapped.inner,
    peer_label: resolvePeerLabel(wrapped.publisher, wrapped.inner),
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
    const who = envelope.peer_label;
    switch (kind) {
      case 'block':
        console.log(`🟦 BLOCK   h=${m.Height} hash=${shortHash(m.Hash)} announced_by=${who} relay=${relay}`);
        break;
      case 'subtree':
        console.log(`🌿 SUBTREE hash=${shortHash(m.Hash)} producer=${who} relay=${relay}`);
        break;
      case 'rejected_tx':
        console.log(`🚫 REJECTED tx=${shortHash(m.TxID)} reason="${(m.Reason ?? '?').slice(0, 80)}" by=${who} relay=${relay}`);
        break;
      case 'node_status':
        console.log(
          `📊 STATUS  ${who} h=${m.best_height} state=${m.fsm_state} ` +
          `listen=${m.listen_mode} peers=${m.connected_peers_count} v=${m.version} relay=${relay}`,
        );
        break;
    }
  }
});

// Per-topic mesh membership events. PRUNE = remote (or local) removed us
// from that topic's delivery set; GRAFT = added back. Frequent PRUNE without
// corresponding GRAFT is the signature of being scored down or losing a
// mesh slot. Useful for diagnosing silent gossip-flow death while TCP stays up.
pubsub.addEventListener('gossipsub:graft', (evt: any) => {
  const { peerId, topic } = evt.detail;
  const kind = topic.replace(TOPIC_PREFIX, '');
  console.log(`[mesh] GRAFT ${labelOf(peerId)} topic=${kind}`);
});
pubsub.addEventListener('gossipsub:prune', (evt: any) => {
  const { peerId, topic } = evt.detail;
  const kind = topic.replace(TOPIC_PREFIX, '');
  console.log(`[mesh] PRUNE ${labelOf(peerId)} topic=${kind}`);
});

for (const topic of TOPICS) pubsub.subscribe(topic);
console.log(`[boot] subscribed ${TOPIC_KINDS.join(',')}`);

for (const addr of LIVE_PEERS) {
  node.dial(multiaddr(addr)).catch((err) => console.log(`[boot] dial failed ${err.message}`));
}

// Optional HTTP broadcast harness. Off by default. When enabled, exposes
// GET /datahubs (snapshot of operatorMap) and POST /broadcast (fan-out raw
// tx bytes to every datahub with a recent node_status). Localhost-only by
// default; use an SSH port-forward to drive it.
if (process.env.BROADCAST_ENABLED === '1') {
  startBroadcaster({
    operatorMap,
    port: parseInt(process.env.BROADCAST_PORT ?? '8080', 10),
    bindHost: process.env.BROADCAST_BIND ?? '127.0.0.1',
  });
}

setInterval(() => {
  const delta = {
    block: counts.block - counts_at_last_heartbeat.block,
    subtree: counts.subtree - counts_at_last_heartbeat.subtree,
    rejected_tx: counts.rejected_tx - counts_at_last_heartbeat.rejected_tx,
    node_status: counts.node_status - counts_at_last_heartbeat.node_status,
  };
  const meshStr = TOPIC_KINDS.map((k) => {
    try { return `${k}=${pubsub.getMeshPeers(TOPIC_PREFIX + k).length}`; }
    catch { return `${k}=?`; }
  }).join(' ');
  const scoreStr = Object.entries(PEER_LABELS).map(([peerId, label]) => {
    try {
      const s = pubsub.getScore(peerId);
      return `${label}=${typeof s === 'number' ? s.toFixed(1) : '?'}`;
    } catch { return `${label}=?`; }
  }).join(' ');
  console.log(
    `[heartbeat] peers=${node.getPeers().length} ` +
    `total{block=${counts.block} subtree=${counts.subtree} rejected_tx=${counts.rejected_tx} node_status=${counts.node_status}} ` +
    `last_30s{block=${delta.block} subtree=${delta.subtree} rejected_tx=${delta.rejected_tx} node_status=${delta.node_status}} ` +
    `mesh{${meshStr}} score{${scoreStr}} ` +
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
