import { PubSub, Topic } from '@google-cloud/pubsub';

export type EventType = 'block' | 'subtree' | 'rejected_tx' | 'node_status';

export type EventEnvelope = {
  type: EventType;
  topic: string;          // gossipsub topic the event came from
  publisher: string;      // outer envelope name (operator self-report)
  relay: string;          // peer that forwarded the message to us
  receivedAt: string;     // ISO timestamp
  data: unknown;          // parsed inner message
  peer_label?: string;    // best-effort friendly identifier for the originator,
                          // resolved from a runtime peer_id → client_name map
                          // built from node_status broadcasts; falls back to
                          // hostname, publisher, or short peer-id prefix
};

const TOPIC_NAMES: Record<EventType, string> = {
  block: 'teranode-block',
  subtree: 'teranode-subtree',
  rejected_tx: 'teranode-rejected-tx',
  node_status: 'teranode-node-status',
};

export class Publisher {
  private pubsub: PubSub | null;
  private topics: Partial<Record<EventType, Topic>> = {};
  private dryRun: boolean;
  private failureCounts: Record<EventType, number> = {
    block: 0, subtree: 0, rejected_tx: 0, node_status: 0,
  };

  constructor(opts: { projectId?: string; dryRun: boolean }) {
    this.dryRun = opts.dryRun;
    this.pubsub = opts.dryRun ? null : new PubSub({ projectId: opts.projectId });
  }

  private getTopic(type: EventType): Topic | null {
    if (!this.pubsub) return null;
    if (!this.topics[type]) this.topics[type] = this.pubsub.topic(TOPIC_NAMES[type]);
    return this.topics[type]!;
  }

  // Publish but never throw. Pub/Sub being down should not crash the listener;
  // we'd rather keep ingesting and miss messages than miss the whole stream.
  async publish(envelope: EventEnvelope): Promise<void> {
    if (this.dryRun) return;
    const topic = this.getTopic(envelope.type);
    if (!topic) return;
    try {
      await topic.publishMessage({
        data: Buffer.from(JSON.stringify(envelope)),
        attributes: {
          type: envelope.type,
          relay: envelope.relay,
          publisher: envelope.publisher,
        },
      });
    } catch (err) {
      this.failureCounts[envelope.type]++;
      // Log only every 100th failure to avoid log spam during sustained outages.
      if (this.failureCounts[envelope.type] % 100 === 1) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[publisher] ${envelope.type} publish failed (count=${this.failureCounts[envelope.type]}): ${msg}`);
      }
    }
  }

  failures(): Record<EventType, number> {
    return { ...this.failureCounts };
  }
}
