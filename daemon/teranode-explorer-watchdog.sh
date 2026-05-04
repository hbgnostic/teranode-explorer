#!/usr/bin/env bash
# Watches teranode-explorer-listener for silent gossip-flow death.
#
# The listener daemon reports a heartbeat every 30s with last_30s counters
# for block/subtree/rejected_tx/node_status. In healthy operation, subtree
# and node_status are essentially never both zero (each operator emits a
# node_status every ~10s). A sustained zero across multiple consecutive
# heartbeats means the libp2p layer has silently degraded and the daemon
# is no longer receiving gossip even though the process is alive.
#
# This script samples recent heartbeats from journalctl, evaluates the
# flow, and restarts the listener service when the flow is dead.
# Sends ntfy notifications on restart and recovery. Cooldown prevents
# thrashing if a restart doesn't fix things.

set -uo pipefail

NTFY_TOPIC="${NTFY_TOPIC:-MISSING_TOPIC}"
NTFY_URL="https://ntfy.sh/${NTFY_TOPIC}"
SERVICE=teranode-explorer-listener.service
COOLDOWN_SEC=$((10 * 60))
STATE_DIR=/var/lib/teranode-explorer-watchdog
LOCK_FILE="${STATE_DIR}/lastrestart"
mkdir -p "$STATE_DIR"

notify() {
  local priority="$1" title="$2" message="$3"
  curl -fsS --max-time 10 \
    -H "Title: ${title}" \
    -H "Priority: ${priority}" \
    -H "Tags: teranode,explorer" \
    -d "${message}" \
    "${NTFY_URL}" >/dev/null 2>&1 || logger -t teranode-explorer-watchdog "ntfy POST failed"
}

# Pull last few heartbeats from a 3-minute window (~6 heartbeats expected at 30s interval)
heartbeats=$(journalctl -u "$SERVICE" --since '3 minutes ago' --no-pager 2>/dev/null | grep '\[heartbeat\]' | tail -3)
count=$(printf '%s\n' "$heartbeats" | grep -c '\[heartbeat\]' || echo 0)

if [ "$count" -lt 2 ]; then
  reason="only ${count} heartbeats in last 3 minutes (expected ~6)"
else
  # Sum subtree+node_status from the last_30s field across recent heartbeats.
  flow=$(printf '%s\n' "$heartbeats" \
    | grep -oE 'last_30s\{[^}]+\}' \
    | grep -oE 'subtree=[0-9]+|node_status=[0-9]+' \
    | grep -oE '[0-9]+' \
    | awk '{s+=$1} END {print s+0}')
  if [ "${flow:-0}" -gt 0 ]; then
    logger -t teranode-explorer-watchdog "healthy: flow=${flow} across ${count} heartbeats"
    exit 0
  fi
  reason="zero subtree+node_status flow across ${count} consecutive heartbeats"
fi

# Cooldown: don't thrash if a recent restart didn't fix things
if [ -f "$LOCK_FILE" ]; then
  last_restart=$(cat "$LOCK_FILE" 2>/dev/null || echo 0)
  now=$(date +%s)
  age=$((now - last_restart))
  if [ "$age" -lt "$COOLDOWN_SEC" ]; then
    notify 4 "teranode-explorer stale (in cooldown)" "${reason}; last restart ${age}s ago. Manual intervention may be needed."
    exit 0
  fi
fi

notify 4 "teranode-explorer stale, restarting" "${reason}"
date +%s > "$LOCK_FILE"
systemctl restart "$SERVICE"

# Wait for boot + first heartbeat cycle, then verify recovery
sleep 45
post_check=$(journalctl -u "$SERVICE" --since '60 seconds ago' --no-pager 2>/dev/null | grep -c '\[heartbeat\]' || echo 0)
if [ "$post_check" -gt 0 ]; then
  notify 3 "teranode-explorer recovered" "Listener restarted, heartbeats resumed (${post_check} cycle(s) seen)."
  logger -t teranode-explorer-watchdog "restart succeeded: ${reason}"
else
  notify 5 "teranode-explorer FAILED to recover" "Restarted but no heartbeats in 60s. ${reason}"
  logger -t teranode-explorer-watchdog "restart did not produce heartbeats: ${reason}"
fi
