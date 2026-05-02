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
- [ ] Public launch

## License

[MIT](LICENSE) — free to use with attribution.

## Cite this repo

A `CITATION.cff` is included. GitHub will surface a "Cite this repository" button on the landing page. If you reference this work in an article or research paper, please cite the author.
