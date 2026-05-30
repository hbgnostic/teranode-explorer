#!/usr/bin/env node
/**
 * Subscribe to teranode-subtree Pub/Sub topic, fetch each subtree's DataHubURL
 * body, and (a) report per-subtree stats or (b) loudly announce when a target
 * txid appears in one. Uses an ephemeral subscription (auto-deleted after 1h
 * idle, and explicitly on SIGINT) so it doesn't compete with the SSE bridge.
 *
 * Usage:
 *   node scripts/find-tx-in-subtree.mjs               # discovery mode
 *   node scripts/find-tx-in-subtree.mjs <txid_hex>    # watch mode
 *
 * Env:
 *   GCP_PROJECT_ID  (default: traceport-production-20251021)
 */
import { PubSub } from '@google-cloud/pubsub';
import { appendFileSync, openSync, closeSync, writeFileSync } from 'node:fs';

const PROJECT_ID = process.env.GCP_PROJECT_ID || 'traceport-production-20251021';
const TOPIC = 'teranode-subtree';
const TARGET_TXID = (process.argv[2] || '').toLowerCase().trim();
const FETCH_TIMEOUT_MS = 12_000;

// Optional: only fetch+log subtrees from this client_name. Set to '' to log all.
const PRODUCER_FILTER = process.env.PRODUCER_FILTER ?? 'bsva-mainnet-eu-1';

// Optional: append every fetched subtree's full txid list to this file. One
// line per txid: "<unix_ms> <producer> <subtree_hash_short> <txid_hex_display>"
const LOG_FILE = process.env.LOG_FILE || '/tmp/bsvb-eu-1-subtrees.log';
if (LOG_FILE) {
  writeFileSync(LOG_FILE, `# started ${new Date().toISOString()} producer_filter=${PRODUCER_FILTER || '(none)'}\n`);
}

const targetTxidBufBE = TARGET_TXID ? Buffer.from(TARGET_TXID, 'hex') : null;
const targetTxidBufLE = targetTxidBufBE ? Buffer.from(targetTxidBufBE).reverse() : null;

const shortHash = (h) =>
  !h ? '?' : h.length > 16 ? `${h.slice(0, 8)}…${h.slice(-6)}` : h;

function scanForTxid(buf) {
  if (!targetTxidBufBE) return false;
  // Try BOTH byte-orders at EVERY 32-byte aligned offset, and also at every
  // offset (handles the case where the subtree body has a small header).
  // Aligned-fast first:
  for (let i = 0; i + 32 <= buf.length; i += 32) {
    if (buf.subarray(i, i + 32).equals(targetTxidBufBE)) return { offset: i, order: 'big-endian' };
    if (buf.subarray(i, i + 32).equals(targetTxidBufLE)) return { offset: i, order: 'little-endian' };
  }
  // Slow unaligned fallback (covers small fixed header):
  for (let i = 0; i + 32 <= buf.length; i++) {
    if (buf.subarray(i, i + 32).equals(targetTxidBufBE)) return { offset: i, order: 'big-endian (unaligned)' };
    if (buf.subarray(i, i + 32).equals(targetTxidBufLE)) return { offset: i, order: 'little-endian (unaligned)' };
  }
  return false;
}

const subName = `tx-finder-${process.pid}-${Date.now()}`;
const pubsub = new PubSub({ projectId: PROJECT_ID });
const topic = pubsub.topic(TOPIC);

console.log(`[boot] project=${PROJECT_ID} topic=${TOPIC} target=${TARGET_TXID || '(none, discovery mode)'}`);
console.log(`[boot] creating ephemeral subscription ${subName}…`);

const [sub] = await topic.createSubscription(subName, {
  expirationPolicy: { ttl: { seconds: 86400 } },  // 24h minimum per Pub/Sub policy
  ackDeadlineSeconds: 60,
});

console.log(`[boot] listening. Ctrl-C to stop (subscription will be deleted).`);

let processed = 0;
let nextHeartbeat = Date.now() + 30_000;

const cleanup = async () => {
  console.log(`\n[shutdown] processed ${processed} subtrees. Deleting subscription…`);
  try { await sub.close(); } catch {}
  try { await sub.delete(); } catch (e) { console.log(`[shutdown] delete err: ${e.message}`); }
  process.exit(0);
};
process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

sub.on('error', (err) => console.error(`[sub error] ${err.message}`));

sub.on('message', async (msg) => {
  msg.ack();
  processed++;
  try {
    const envelope = JSON.parse(msg.data.toString('utf8'));
    const inner = envelope?.data || {};
    const subtreeHash = inner.Hash;
    const baseUrl = (inner.DataHubURL || inner.base_url || '').replace(/\/$/, '');
    const producer = envelope.peer_label || inner.ClientName || inner.client_name || '?';

    // Discovery: show all envelope fields on first messages
    if (processed <= 3) {
      console.log(`\n=== ENVELOPE #${processed} ===`);
      console.log(`  envelope keys: ${Object.keys(envelope).join(', ')}`);
      console.log(`  inner keys:    ${Object.keys(inner).join(', ')}`);
      console.log(`  inner JSON:    ${JSON.stringify(inner).slice(0, 400)}`);
    }

    if (!baseUrl || !subtreeHash) {
      return;
    }

    // Producer filter: skip subtrees from operators we don't care about.
    if (PRODUCER_FILTER && (inner.ClientName !== PRODUCER_FILTER && producer !== PRODUCER_FILTER)) {
      return;
    }

    const dataHubUrl = `${baseUrl}/subtree/${subtreeHash}`;

    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), FETCH_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(dataHubUrl, { signal: ac.signal });
    } catch (err) {
      if (processed <= 3 || (TARGET_TXID && processed % 100 === 1))
        console.log(`[fetch fail] subtree=${shortHash(subtreeHash)} producer=${producer} err=${err.message}`);
      return;
    } finally {
      clearTimeout(timer);
    }

    if (!res.ok) {
      if (processed <= 3 || (TARGET_TXID && processed % 100 === 1))
        console.log(`[fetch ${res.status}] subtree=${shortHash(subtreeHash)} producer=${producer} url=${dataHubUrl}`);
      return;
    }

    const contentType = res.headers.get('content-type') || '';
    const buf = Buffer.from(await res.arrayBuffer());

    // Discovery: dump format details for first 3 successful fetches
    if (processed <= 3) {
      console.log(`\n--- DISCOVERY #${processed} ---`);
      console.log(`  producer:     ${producer}`);
      console.log(`  url:          ${dataHubUrl}`);
      console.log(`  content-type: ${contentType}`);
      console.log(`  size:         ${buf.length} bytes`);
      console.log(`  size % 32:    ${buf.length % 32}`);
      console.log(`  head hex:     ${buf.subarray(0, 64).toString('hex')}`);
      if (buf.length > 64)
        console.log(`  tail hex:     ${buf.subarray(buf.length - 64).toString('hex')}`);
    }

    if (TARGET_TXID) {
      const hit = scanForTxid(buf);
      if (hit) {
        console.log(`\n🎯🎯🎯 FOUND ${TARGET_TXID} in subtree`);
        console.log(`     subtree_hash: ${subtreeHash}`);
        console.log(`     producer:     ${producer}`);
        console.log(`     data_hub_url: ${dataHubUrl}`);
        console.log(`     byte_offset:  ${hit.offset}`);
        console.log(`     byte_order:   ${hit.order}\n`);
      }
    } else {
      // No target: print per-subtree summary AND append all txids to LOG_FILE
      const approxTxCount = Math.floor(buf.length / 32);
      const sample = [];
      for (let i = 0; i < Math.min(3, approxTxCount); i++) {
        sample.push(buf.subarray(i * 32, (i + 1) * 32).reverse().toString('hex').slice(0, 16) + '…');
      }
      console.log(
        `subtree ${shortHash(subtreeHash)} producer=${producer} ` +
        `bytes=${buf.length} txs=${approxTxCount} sample=[${sample.join(', ')}]`
      );
      if (LOG_FILE) {
        const ts = Date.now();
        const lines = [];
        for (let i = 0; i < approxTxCount; i++) {
          const txid = buf.subarray(i * 32, (i + 1) * 32).reverse().toString('hex');
          // Log FULL subtree hash now (was shortened previously)
          lines.push(`${ts} ${producer} ${subtreeHash} ${txid}\n`);
        }
        appendFileSync(LOG_FILE, lines.join(''));
      }
    }

    if (Date.now() > nextHeartbeat) {
      console.log(`[heartbeat] processed=${processed}`);
      nextHeartbeat = Date.now() + 30_000;
    }
  } catch (err) {
    console.error(`[handler error] ${err.message}`);
  }
});
