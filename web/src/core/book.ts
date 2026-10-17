// Order-book model fed by the public WebSocket protocol (docs/websocket.md):
// a snapshot, then deltas that each carry `prev`, the sequence number of the
// previous book message. A delta whose prev is not the last seen sequence means
// a message was missed and the book must be resynchronised.

import { addDecimal, cmpDecimal } from "./decimal";

export type Level = [price: string, qty: string];

export interface SnapshotMsg {
  type: "snapshot";
  seq: number;
  bids: Level[];
  asks: Level[];
}

export interface DeltaMsg {
  type: "delta";
  seq: number;
  prev: number;
  side: "buy" | "sell";
  price: string;
  qty: string;
}

export interface BookRow {
  price: string;
  qty: string;
  /** cumulative quantity from the best level down to this one */
  total: string;
}

export class BookModel {
  private bids = new Map<string, string>();
  private asks = new Map<string, string>();
  seq = 0;
  ready = false;

  applySnapshot(m: SnapshotMsg): void {
    this.bids = new Map(m.bids);
    this.asks = new Map(m.asks);
    this.seq = m.seq;
    this.ready = true;
  }

  /** Returns false when a gap is detected (the caller must resynchronise). */
  applyDelta(m: DeltaMsg): boolean {
    if (!this.ready || m.prev !== this.seq) return false;
    const side = m.side === "buy" ? this.bids : this.asks;
    if (m.qty === "0") side.delete(m.price);
    else side.set(m.price, m.qty);
    this.seq = m.seq;
    return true;
  }

  reset(): void {
    this.bids.clear();
    this.asks.clear();
    this.seq = 0;
    this.ready = false;
  }

  /** Bids best (highest) first. */
  bidRows(depth = 15): BookRow[] {
    return rows(this.bids, depth, (a, b) => cmpDecimal(b, a));
  }

  /** Asks best (lowest) first. */
  askRows(depth = 15): BookRow[] {
    return rows(this.asks, depth, (a, b) => cmpDecimal(a, b));
  }

  bestBid(): string | undefined {
    return this.bidRows(1)[0]?.price;
  }

  bestAsk(): string | undefined {
    return this.askRows(1)[0]?.price;
  }
}

function rows(side: Map<string, string>, depth: number, order: (a: string, b: string) => number): BookRow[] {
  const prices = [...side.keys()].sort(order).slice(0, depth);
  let total = "0";
  return prices.map((price) => {
    const qty = side.get(price)!;
    total = addDecimal(total, qty);
    return { price, qty, total };
  });
}
