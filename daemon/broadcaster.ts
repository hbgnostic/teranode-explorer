import { createServer, IncomingMessage, ServerResponse } from 'node:http';

// Structural — must stay in sync with operatorMap's value type in listener.ts.
// Kept local to avoid a circular import on a 3-field interface.
type OperatorInfo = { client_name: string; base_url: string; last_seen: number };

const READ_TIMEOUT_MS = 10_000;
const POST_TIMEOUT_MS = 15_000;
const MAX_BODY_BYTES = 1_048_576;
const DEFAULT_MAX_AGE_SEC = 120;

const DRY_RUN = process.env.BROADCAST_DRY_RUN === '1';

type DatahubResult = {
  base_url: string;
  client_name: string;
  status: number | null;
  body: string;
  latency_ms: number;
  error?: string;
};

const hexToBytes = (hex: string): Uint8Array => {
  const clean = hex.trim().replace(/^0x/, '');
  if (!/^[0-9a-fA-F]+$/.test(clean) || clean.length % 2 !== 0) {
    throw new Error('invalid hex');
  }
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  return out;
};

const readBody = (req: IncomingMessage): Promise<string> =>
  new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    const timer = setTimeout(() => reject(new Error('read timeout')), READ_TIMEOUT_MS);
    req.on('data', (c: Buffer) => {
      total += c.length;
      if (total > MAX_BODY_BYTES) {
        clearTimeout(timer);
        req.destroy();
        reject(new Error('body too large'));
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => { clearTimeout(timer); resolve(Buffer.concat(chunks).toString('utf8')); });
    req.on('error', (e) => { clearTimeout(timer); reject(e); });
  });

const postTx = async (
  baseUrl: string,
  txBytes: Uint8Array,
): Promise<Omit<DatahubResult, 'base_url' | 'client_name'>> => {
  const t0 = performance.now();
  if (DRY_RUN) {
    return { status: 0, body: '(dry-run, no post sent)', latency_ms: 0 };
  }
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), POST_TIMEOUT_MS);
  try {
    const res = await fetch(`${baseUrl.replace(/\/$/, '')}/tx`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: Buffer.from(txBytes),
      signal: ac.signal,
    });
    const text = (await res.text()).slice(0, 500);
    return { status: res.status, body: text, latency_ms: Math.round(performance.now() - t0) };
  } catch (err) {
    return {
      status: null,
      body: '',
      latency_ms: Math.round(performance.now() - t0),
      error: err instanceof Error ? err.message : String(err),
    };
  } finally {
    clearTimeout(timer);
  }
};

const sendJson = (res: ServerResponse, status: number, payload: unknown) => {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(payload, null, 2));
};

export function startBroadcaster(opts: {
  operatorMap: Map<string, OperatorInfo>;
  port: number;
  bindHost?: string;
}) {
  const { operatorMap, port, bindHost = '127.0.0.1' } = opts;

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    if (req.method === 'GET' && req.url === '/datahubs') {
      const now = Date.now();
      const list = Array.from(operatorMap.entries()).map(([peer_id, info]) => ({
        peer_id,
        client_name: info.client_name,
        base_url: info.base_url,
        age_sec: Math.round((now - info.last_seen) / 1000),
      }));
      sendJson(res, 200, { count: list.length, datahubs: list });
      return;
    }

    if (req.method === 'POST' && req.url === '/broadcast') {
      try {
        const raw = await readBody(req);
        const body = JSON.parse(raw) as { tx_hex: string; max_age_sec?: number };
        if (typeof body?.tx_hex !== 'string') throw new Error('missing tx_hex (string)');
        const txBytes = hexToBytes(body.tx_hex);

        const maxAgeMs = (body.max_age_sec ?? DEFAULT_MAX_AGE_SEC) * 1000;
        const now = Date.now();
        const targets = Array.from(operatorMap.values())
          .filter((op) => op.base_url && (now - op.last_seen) < maxAgeMs);

        if (targets.length === 0) {
          sendJson(res, 503, { error: 'no live datahubs in operatorMap' });
          return;
        }

        console.log(`[broadcaster] POST /broadcast tx_size=${txBytes.length} targets=${targets.length} dry_run=${DRY_RUN}`);

        const results: DatahubResult[] = await Promise.all(
          targets.map(async (op) => ({
            base_url: op.base_url,
            client_name: op.client_name,
            ...(await postTx(op.base_url, txBytes)),
          })),
        );

        const accepted = results.filter((r) => r.status === 200).length;
        console.log(`[broadcaster] done accepted=${accepted}/${results.length}`);
        sendJson(res, 200, {
          tx_size_bytes: txBytes.length,
          targets_attempted: results.length,
          accepted_200: accepted,
          dry_run: DRY_RUN,
          results,
        });
      } catch (err) {
        sendJson(res, 400, { error: err instanceof Error ? err.message : String(err) });
      }
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(port, bindHost, () => {
    console.log(`[broadcaster] listening on ${bindHost}:${port} dry_run=${DRY_RUN}`);
  });

  return server;
}
