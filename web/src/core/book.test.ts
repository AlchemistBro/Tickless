import { describe, expect, it } from "vitest";
import { BookModel, DeltaMsg, Level, SnapshotMsg } from "./book";

const snap = (seq: number, bids: Level[], asks: Level[]): SnapshotMsg => ({ type: "snapshot", seq, bids, asks });
const delta = (seq: number, prev: number, side: "buy" | "sell", price: string, qty: string): DeltaMsg => ({ type: "delta", seq, prev, side, price, qty });

describe("BookModel", () => {
  it("shows bids best-first, asks best-first, with cumulative totals", () => {
    const b = new BookModel();
    b.applySnapshot(snap(5, [["99", "1"], ["100", "2"], ["98.5", "0.5"]], [["102", "1"], ["101", "0.25"]]));
    expect(b.bidRows().map((r) => [r.price, r.qty, r.total])).toEqual([["100", "2", "2"], ["99", "1", "3"], ["98.5", "0.5", "3.5"]]);
    expect(b.askRows().map((r) => [r.price, r.total])).toEqual([["101", "0.25"], ["102", "1.25"]]);
    expect(b.bestBid()).toBe("100");
    expect(b.bestAsk()).toBe("101");
  });

  it("sorts prices numerically (10 > 9)", () => {
    const b = new BookModel();
    b.applySnapshot(snap(1, [["9", "1"], ["10", "1"], ["100", "1"]], []));
    expect(b.bidRows().map((r) => r.price)).toEqual(["100", "10", "9"]);
  });

  it("applies contiguous deltas: update, insert, remove", () => {
    const b = new BookModel();
    b.applySnapshot(snap(5, [["100", "2"]], [["101", "1"]]));
    // The engine sequence jumps when non-book events happen in between; prev links the book messages.
    expect(b.applyDelta(delta(8, 5, "buy", "100", "3"))).toBe(true);
    expect(b.applyDelta(delta(9, 8, "buy", "99", "1"))).toBe(true);
    expect(b.applyDelta(delta(11, 9, "sell", "101", "0"))).toBe(true);
    expect(b.bidRows().map((r) => [r.price, r.qty])).toEqual([["100", "3"], ["99", "1"]]);
    expect(b.askRows()).toEqual([]);
    expect(b.seq).toBe(11);
  });

  it("detects a missed delta and refuses to apply anything on top of the gap", () => {
    const b = new BookModel();
    b.applySnapshot(snap(5, [["100", "2"]], []));
    expect(b.applyDelta(delta(9, 8, "buy", "100", "9"))).toBe(false); // prev 8, but we are at 5
    expect(b.bidRows()[0].qty).toBe("2");
    expect(b.seq).toBe(5);
  });

  it("ignores deltas before the first snapshot", () => {
    const b = new BookModel();
    expect(b.applyDelta(delta(2, 1, "buy", "100", "1"))).toBe(false);
  });

  it("reset forgets everything until the next snapshot", () => {
    const b = new BookModel();
    b.applySnapshot(snap(5, [["100", "2"]], []));
    b.reset();
    expect(b.ready).toBe(false);
    expect(b.bidRows()).toEqual([]);
  });

  it("matches a naive reference on random streams", () => {
    let rnd = 12345;
    const next = (n: number) => (rnd = (rnd * 1103515245 + 12345) & 0x7fffffff) % n;
    const b = new BookModel();
    const ref = { buy: new Map<string, string>(), sell: new Map<string, string>() };
    b.applySnapshot(snap(0, [], []));
    let prev = 0;
    for (let seq = 1; seq <= 2000; seq++) {
      const side = next(2) ? "buy" : "sell";
      const price = String(90 + next(20));
      const qty = next(4) === 0 ? "0" : String(1 + next(9));
      expect(b.applyDelta(delta(seq, prev, side, price, qty))).toBe(true);
      prev = seq;
      if (qty === "0") ref[side].delete(price);
      else ref[side].set(price, qty);
    }
    const sorted = (m: Map<string, string>, dir: 1 | -1) => [...m].sort((x, y) => dir * (Number(x[0]) - Number(y[0]))).map(([p, q]) => [p, q]);
    expect(b.bidRows(100).map((r) => [r.price, r.qty])).toEqual(sorted(ref.buy, -1));
    expect(b.askRows(100).map((r) => [r.price, r.qty])).toEqual(sorted(ref.sell, 1));
  });
});
