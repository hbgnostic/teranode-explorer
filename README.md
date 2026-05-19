# teranode-explorer

Real-time observability for BSV's Teranode network — a live view of the libp2p / gossipsub mesh that operators use to announce blocks, subtrees, transaction rejections, and node status.

By **[Bridget Doran](https://x.com/HBGnostic)** ([HBGnostic](https://x.com/HBGnostic) on X / [UTXO Engineer](https://utxoengineer.com)) — author of the BSV Intel Report.
First published **May 2026**.

Live at [`explorer.utxoengineer.com`](https://explorer.utxoengineer.com).

---

## What this is

A web explorer that taps the BSV Teranode mesh directly via libp2p — no chain-explorer middleman, no third-party API. It subscribes to the four public gossip topics on Teranode mainnet, parses what flies past, and surfaces the network in motion:

- **Live block feed** — new blocks as they propagate, with miner, height, and propagation path.
- **Live subtree stream** — the continuous flow of transaction batches between blocks. Teranode's "no-mempool" architecture made visible.
- **Operator status grid** — every node currently announcing on the mesh: FSM state, version, listen mode, latest block miner. Refreshed every ~10s by each operator's heartbeat.
- **Rejected-transaction feed** — with reason codes from the operators that rejected them.
- **Network health row** — operator count, version diffusion, tip consensus across the mesh.

## Why it's interesting

Existing BSV explorers query a node and present chain data. This one taps the *mesh itself* and shows the gossip layer in motion. No existing tool surfaces:

- Subtrees in real time (operators broadcast many per minute between blocks; nobody displays them)
- Operator-level visibility (who's running Teranode, in what state, on what version)
- Real-time network composition (how many distinct organizations, version diffusion, FSM state distribution)

Built on the same libp2p / gossipsub primitives the operators themselves speak.

## Architecture

```
┌─ openclaw VM (long-running listener daemon) ───────────────┐
│   subscribes to Teranode mainnet libp2p gossipsub          │
│   parses block / subtree / rejected_tx / node_status       │
│   publishes events → Cloud Pub/Sub                         │
└──────────────────────────────────┬─────────────────────────┘
                                   │
                ┌──────────────────┴──────────────────┐
                │  Cloud Pub/Sub topics               │
                └──────────────────┬──────────────────┘
                                   │
                                   ▼
                        ┌──────────────────────┐
                        │ Cloud Run SSE bridge │
                        │ • subscribes to      │
                        │   Pub/Sub            │
                        │ • streams events to  │
                        │   browser via SSE    │
                        └──────────┬───────────┘
                                   │
                                   ▼
                          [browser: live UI]
```

Architecturally: announce on P2P, fetch on HTTP. The libp2p layer carries small notifications; the actual block/subtree data sits behind operator-served HTTP endpoints (`DataHubURL`).

## Players and protocol vocabulary

A reference for the terms this project uses, with each term mapped to the actors in the Teranode mainnet network it describes. "My listener" refers to the daemon in this repo.

**Categories:** 🔵 libp2p / gossipsub protocol · 🟠 Teranode application layer · 🟣 this project's vocabulary

<table>
  <thead>
    <tr>
      <th align="left">Term</th>
      <th align="left">What it means</th>
      <th align="left">Who in this network</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>🔵 <b>Peer</b></td>
      <td>Any node speaking the libp2p protocol that I'm connected to.</td>
      <td>Everyone here. Operators, relay peers, and my listener are all peers.</td>
    </tr>
    <tr>
      <td>🟠 <b>Operator</b></td>
      <td>A peer running Teranode that broadcasts <code>node_status</code> heartbeats. Operators produce blocks and subtrees. Some operators ALSO play the relay role (see below).</td>
      <td>The operators visible in my UI's right-side panel.</td>
    </tr>
    <tr>
      <td>🟣 <b>Relay peer</b></td>
      <td>A role some operators choose to play: they expose their libp2p endpoint publicly so listeners and observability tools can connect. Most operators do not — their endpoints are firewalled. Not a formal libp2p term; libp2p's <em>Circuit Relay v2</em> is a different protocol concept.</td>
      <td>Two operators currently play the relay role: <code>bsva-mainnet-eu-1</code> (exposed publicly as the BSVB-EU endpoint) and <code>bsva-mainnet-eu-2</code> (exposed as BSVB-US, despite the "US" in the hostname). The other operators are operator-only. My UI marks these two with a "relay" badge.</td>
    </tr>
    <tr>
      <td>🟠 <b>Producer</b></td>
      <td>The peer that originated a subtree announcement — the operator running the subtree microservice. Appears as <code>ClientName</code> on subtree messages.</td>
      <td>An operator, for subtree messages. The same field on block messages identifies the block announcer (see below).</td>
    </tr>
    <tr>
      <td>🟠 <b>Block announcer</b></td>
      <td>The operator that broadcast a block on the mesh. A single chain block typically generates several announcements from different operators within seconds — each one propagating the block they just processed. The on-the-wire <code>ClientName</code> field is always the literal string <code>"teranode"</code> on block messages and is not the announcer; the announcer is identified by <code>data.PeerID</code> (resolved by this project to the operator's <code>client_name</code> via their <code>node_status</code> broadcasts).</td>
      <td>An operator that received and propagated a block. Not necessarily the actual proof-of-work miner — the PoW miner is identified by the block header's <code>miner_name</code>, which surfaces separately on operator cards as <code>last_miner</code>.</td>
    </tr>
    <tr>
      <td>🟠 <b>Miner</b></td>
      <td>In strict Bitcoin terms, the entity that performed proof-of-work and found the block (e.g. <code>/taal.com_TERANODE/</code>, <code>/Mining-Dutch/</code>). Comes from the block header. Distinct from the operator that announced the block on the mesh.</td>
      <td>A small set of pools doing PoW on BSV mainnet at any given time. Reported by each operator on their <code>node_status</code> as <code>miner_name</code> (the miner of the latest block they have seen).</td>
    </tr>
    <tr>
      <td>🔵 <b>Subscriber</b></td>
      <td>A peer subscribed to a gossipsub topic, receiving messages on it.</td>
      <td>My listener subscribes to all four topics. Operators and relay peers also subscribe.</td>
    </tr>
    <tr>
      <td>🔵 <b>Publisher</b></td>
      <td>A peer that originates new messages on a topic. To publish, a peer has to be running the Teranode application logic that emits on that topic. Distinct from forwarding, which is what every gossipsub peer does to messages it receives.</td>
      <td>Only operators publish in this network: blocks, subtrees, <code>node_status</code>, and <code>rejected_tx</code>. Relay peers forward published messages but do not originate them. My listener does not publish.</td>
    </tr>
    <tr>
      <td>🔵 <b>Mesh peer</b></td>
      <td>A peer in my active push-delivery list for a topic. Mesh is the "fast lane" — messages get pushed to mesh peers without request. Target ~6 per topic.</td>
      <td>My listener's mesh is intentionally empty; the relay peers are managed as Direct peers instead.</td>
    </tr>
    <tr>
      <td>🔵 <b>Direct peer</b></td>
      <td>A peer flagged as trusted upstream infrastructure; receives and delivers messages outside mesh logic and bypasses scoring penalties.</td>
      <td>BSVB-US and BSVB-EU, by my listener's config.</td>
    </tr>
    <tr>
      <td>🔵 <b>Bootstrap peer</b></td>
      <td>A peer in my starter list, dialed at boot to make initial connections.</td>
      <td>BSVB-US and BSVB-EU.</td>
    </tr>
    <tr>
      <td>🔵 <b>GRAFT</b></td>
      <td>Control message a peer sends to ask: "add me to your mesh for this topic."</td>
      <td>Any peer can send GRAFT. My listener receives them from relay peers; due to direct-peer config, my listener replies with PRUNE rather than accepting.</td>
    </tr>
    <tr>
      <td>🔵 <b>PRUNE</b></td>
      <td>Control message a peer sends to say: "remove me from your mesh for this topic."</td>
      <td>Any peer can send PRUNE, including my listener.</td>
    </tr>
    <tr>
      <td>🔵 <b>IHAVE</b></td>
      <td>Lazy gossip: "I've seen these message IDs recently." Broadcast periodically to non-mesh peers.</td>
      <td>Any subscribed peer can send IHAVE. My listener receives them from relay peers.</td>
    </tr>
    <tr>
      <td>🔵 <b>IWANT</b></td>
      <td>Response to IHAVE: "send me message X." How a non-mesh peer pulls a message it heard about.</td>
      <td>My listener sends IWANTs when it hears about a message it doesn't have.</td>
    </tr>
    <tr>
      <td>🔵 <b>Floodsub fallback</b></td>
      <td>Backup delivery mode where messages flood to all subscribers when the mesh is empty.</td>
      <td>Enabled in my listener's gossipsub config.</td>
    </tr>
    <tr>
      <td>🔵 <b>Score</b></td>
      <td>A number each peer tracks per other peer, based on observed behavior. Below threshold → exclusion from mesh.</td>
      <td>My listener scores the relay peers; the relay peers presumably score my listener, but I can't read their score of me.</td>
    </tr>
    <tr>
      <td>🔵 <b>Behaviour penalty</b></td>
      <td>Counter that drives score down for protocol misbehaviors (e.g., re-GRAFT during backoff, broken IWANT promises). Score impact = counter² × weight.</td>
      <td>Applied by my listener against any peer that triggers it. Direct peers bypass the GRAFT-side penalty.</td>
    </tr>
    <tr>
      <td>🔵 <b>Broken promise</b></td>
      <td>When a peer sent IHAVE for a message, I sent IWANT, and they failed to deliver in time.</td>
      <td>Triggered when relay peers can't fulfill an IWANT my listener sent.</td>
    </tr>
  </tbody>
</table>

## Glossary

For terminology used in the explorer (FSM states, subtrees, operators, gossipsub topics, etc.), see the standalone [Teranode libp2p glossary](https://github.com/hbgnostic/teranode-listener-test/blob/master/GLOSSARY.md) — a reference document covering the gossip layer this explorer builds on.

## Repo structure

```
teranode-explorer/
├── daemon/                     # long-running libp2p listener + Pub/Sub publisher
│   ├── listener.ts             # main daemon
│   ├── publisher.ts            # Pub/Sub wrapper
│   └── systemd/                # systemd unit for openclaw VM deployment
├── api/                        # Cloud Run SSE bridge (TBD)
├── ui/                         # static web UI (TBD)
└── package.json
```

## Status

MVP shipped, public launch pending.

- [x] libp2p listener research (precursor: [teranode-listener-test](https://github.com/hbgnostic/teranode-listener-test))
- [x] Listener daemon publishing to Cloud Pub/Sub
- [x] SSE bridge on Cloud Run
- [x] Live block and subtree feed UI
- [x] Operator status grid UI
- [x] Derived feed events (state changes, new miners, tip advances, operator joins)
- [x] Domain wiring (`explorer.utxoengineer.com`)
- [x] Public launch

## License

[MIT](LICENSE) — free to use with attribution.

## Cite this repo

A `CITATION.cff` is included. GitHub will surface a "Cite this repository" button on the landing page. If you reference this work in an article or research paper, please cite the author.
