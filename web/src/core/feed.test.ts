import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Feed } from "./feed";

class FakeSocket {
  static instances: FakeSocket[] = [];
  readyState = 0;
  sent: Record<string, unknown>[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  send(s: string) {
    this.sent.push(JSON.parse(s));
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
  open() {
    this.readyState = 1;
    return this.onopen?.();
  }
  receive(m: unknown) {
    this.onmessage?.({ data: JSON.stringify(m) });
  }
}

const connect = (u: string) => new FakeSocket(u) as unknown as WebSocket;

beforeEach(() => {
  FakeSocket.instances = [];
  vi.useFakeTimers();
  // The Feed compares readyState with WebSocket.OPEN.
  vi.stubGlobal("WebSocket", { OPEN: 1 });
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const last = () => FakeSocket.instances[FakeSocket.instances.length - 1];

describe("Feed", () => {
  it("routes messages by channel and ref-counts subscriptions", async () => {
    const f = new Feed("ws://x", { connect });
    f.start();
    await last().open();
    const a: unknown[] = [];
    const b: unknown[] = [];
    const offA = f.subscribe("book.X", (m) => a.push(m));
    const offB = f.subscribe("book.X", (m) => b.push(m));
    expect(last().sent.filter((s) => s.op === "subscribe")).toHaveLength(1); // second handler does not resubscribe

    last().receive({ channel: "book.X", n: 1 });
    last().receive({ channel: "book.Y", n: 2 });
    expect(a).toEqual([{ channel: "book.X", n: 1 }]);
    expect(b).toEqual(a);

    offA();
    expect(last().sent.some((s) => s.op === "unsubscribe")).toBe(false); // still one handler left
    offB();
    expect(last().sent.at(-1)).toEqual({ op: "unsubscribe", channels: ["book.X"] });
  });

  it("re-subscribes every channel after a reconnect (the server answers with fresh snapshots)", async () => {
    const f = new Feed("ws://x", { connect, backoffMs: [100, 1000] });
    f.start();
    await last().open();
    f.subscribe("book.X", () => undefined);
    f.subscribe("trades.X", () => undefined);

    last().close(); // connection lost
    expect(FakeSocket.instances).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1100);
    expect(FakeSocket.instances).toHaveLength(2);
    await last().open();
    const sub = last().sent.find((s) => s.op === "subscribe");
    expect(sub).toEqual({ op: "subscribe", channels: ["book.X", "trades.X"] });
  });

  it("backs off exponentially and stops reconnecting after stop()", async () => {
    const f = new Feed("ws://x", { connect, backoffMs: [100, 800] });
    f.start();
    last().close();
    await vi.advanceTimersByTimeAsync(110);
    expect(FakeSocket.instances).toHaveLength(2);
    last().close();
    await vi.advanceTimersByTimeAsync(110); // second attempt needs ~200 ms
    expect(FakeSocket.instances).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(200);
    expect(FakeSocket.instances).toHaveLength(3);
    f.stop();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(FakeSocket.instances).toHaveLength(3);
  });

  it("wildcard handlers see everything and never send a subscribe", async () => {
    const f = new Feed("ws://x", { connect });
    f.start();
    await last().open();
    const seen: unknown[] = [];
    f.subscribe("*", (m) => seen.push(m));
    last().receive({ type: "balance" });
    last().receive({ channel: "x", type: "y" });
    expect(seen).toHaveLength(2);
    expect(last().sent.filter((s) => s.op === "subscribe" || s.op === "unsubscribe")).toEqual([]);
  });

  it("runs onOpen (authentication) before re-subscribing", async () => {
    const order: string[] = [];
    const f = new Feed("ws://x", {
      connect,
      onOpen: async (send) => {
        order.push("auth");
        send({ op: "auth", token: "t" });
      },
    });
    f.subscribe("orders", () => order.push("never"));
    f.start();
    await last().open();
    expect(last().sent.map((s) => s.op)).toEqual(["auth", "subscribe"]);
  });

  it("resubscribe asks for a fresh snapshot", async () => {
    const f = new Feed("ws://x", { connect });
    f.start();
    await last().open();
    f.resubscribe("book.X");
    expect(last().sent.slice(-2)).toEqual([
      { op: "unsubscribe", channels: ["book.X"] },
      { op: "subscribe", channels: ["book.X"] },
    ]);
  });

  it("ignores malformed frames", async () => {
    const f = new Feed("ws://x", { connect });
    f.start();
    await last().open();
    const got: unknown[] = [];
    f.subscribe("*", (m) => got.push(m));
    last().onmessage?.({ data: "{not json" });
    expect(got).toEqual([]);
  });
});
