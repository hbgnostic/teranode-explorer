# Listener daemon

Long-running libp2p / gossipsub subscriber for Teranode mainnet. Parses the four public gossip topics and publishes each event to Cloud Pub/Sub. Designed to run as a systemd service on a Linux VM.

## Local testing

```bash
npm install
DRY_RUN=1 npm run daemon
```

Dry-run mode logs events to stdout without publishing to Pub/Sub. Useful for verifying connectivity and message parsing without GCP credentials.

## Running against Pub/Sub

Requires:
- A GCP project with Pub/Sub enabled
- Four topics created: `teranode-block`, `teranode-subtree`, `teranode-rejected-tx`, `teranode-node-status`
- Application Default Credentials (or `GOOGLE_APPLICATION_CREDENTIALS` pointing to a key file) with `roles/pubsub.publisher`

```bash
export GCP_PROJECT_ID=your-project-id
npm run daemon
```

## Event envelope

Each Pub/Sub message body is JSON:

```json
{
  "type": "block" | "subtree" | "rejected_tx" | "node_status",
  "topic": "teranode/bitcoin/1.0.0/mainnet-block",
  "publisher": "bsva-mainnet-eu-2",
  "relay": "BSVB-EU",
  "receivedAt": "2026-05-02T14:32:41.123Z",
  "data": { ... }
}
```

Pub/Sub message attributes (`type`, `relay`, `publisher`) are also set, so downstream subscribers can filter without parsing the body.

## Deployment to openclaw VM

See `systemd/teranode-explorer-listener.service`. Outline:

```bash
# As root on the VM:
sudo useradd --system --home /opt/teranode-explorer --shell /usr/sbin/nologin teranode-explorer
sudo mkdir -p /opt/teranode-explorer /etc/teranode-explorer /var/log/teranode-explorer
sudo chown -R teranode-explorer:teranode-explorer /opt/teranode-explorer /var/log/teranode-explorer

# Deploy code (rsync from dev machine, or git clone):
sudo -u teranode-explorer git clone https://github.com/hbgnostic/teranode-explorer.git /opt/teranode-explorer
sudo -u teranode-explorer npm --prefix /opt/teranode-explorer install

# Environment file:
sudo tee /etc/teranode-explorer/env <<EOF
GCP_PROJECT_ID=your-project-id
EOF
sudo chmod 600 /etc/teranode-explorer/env

# systemd unit:
sudo cp /opt/teranode-explorer/daemon/systemd/teranode-explorer-listener.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now teranode-explorer-listener.service
sudo systemctl status teranode-explorer-listener.service
journalctl -u teranode-explorer-listener.service -f
```
