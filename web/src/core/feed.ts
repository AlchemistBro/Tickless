// WebSocket feeds with automatic reconnection. Subscriptions are reference
// counted per channel and replayed after every reconnect; because the server
// answers every subscription with a fresh snapshot, a reconnect needs no extra
// resynchronisation logic in the widgets.

import { accessToken } from "./api";

type Handler = (msg: Record<string, unknown>) => void;

interface Options {
  /** Builds the socket (injectable for tests). */
  connect?: (url: string) => WebSocket;
  /** Called after the socket opened, before channels are re-subscribed. */
  onOpen?: (send: (v: unknown) => void) => Promise<void> | void;
  backoffMs?: [min: number, max: number];
}

export type FeedStatus = "connecting" | "open" | "closed";

export class Feed {
  private ws: WebSocket | null = null;
  private handlers = new Map<string, Set<Handler>>();
  private statusListeners = new Set<(s: FeedStatus) => void>();
  private attempt = 0;
  private stopped = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private pingTimer: ReturnType<typeof setInterval> | undefined;
  status: FeedStatus = "closed";

  constructor(
    private url: string,
    private opts: Options = {},
  ) {}

  private setStatus(s: FeedStatus) {
    this.status = s;
    this.statusListeners.forEach((l) => l(s));
  }

  onStatus(l: (s: FeedStatus) => void): () => void {
    this.statusListeners.add(l);
    return () => this.statusListeners.delete(l);
  }

  start(): void {
    this.stopped = false;
    if (!this.ws) this.open();
  }

  stop(): void {
    this.stopped = true;
    clearTimeout(this.timer);
    clearInterval(this.pingTimer);
    this.ws?.close();
    this.ws = null;
    this.setStatus("closed");
  }

  private open() {
    this.setStatus("connecting");
    const ws = (this.opts.connect ?? ((u) => new WebSocket(u)))(this.url);
    this.ws = ws;
    ws.onopen = async () => {
      this.attempt = 0;
      const send = (v: unknown) => ws.readyState === WebSocket.OPEN && ws.send(JSON.stringify(v));
      await this.opts.onOpen?.(send);
      const channels = [...this.handlers.keys()].filter((c) => c !== "*");
      if (channels.length) send({ op: "subscribe", channels });
      clearInterval(this.pingTimer);
      this.pingTimer = setInterval(() => send({ op: "ping" }), 20_000);
      this.setStatus("open");
    };
    ws.onmessage = (ev) => {
      let m: Record<string, unknown>;
      try {
        m = JSON.parse(ev.data as string) as Record<string, unknown>;
      } catch {
        return;
      }
      const ch = typeof m.channel === "string" ? m.channel : "";
      this.handlers.get(ch)?.forEach((h) => h(m));
      this.handlers.get("*")?.forEach((h) => h(m));
    };
    ws.onclose = () => {
      clearInterval(this.pingTimer);
      this.ws = null;
      if (this.stopped) return;
      this.setStatus("connecting");
      const [min, max] = this.opts.backoffMs ?? [500, 15_000];
      const delay = Math.min(max, min * 2 ** this.attempt++) * (0.5 + Math.random() / 2);
      this.timer = setTimeout(() => this.open(), delay);
    };
    ws.onerror = () => ws.close();
  }

  /** Subscribes a handler to a channel ("*" = every message). Returns the unsubscribe function. */
  subscribe(channel: string, handler: Handler): () => void {
    let set = this.handlers.get(channel);
    const first = !set;
    if (!set) this.handlers.set(channel, (set = new Set()));
    set.add(handler);
    if (first && channel !== "*") this.send({ op: "subscribe", channels: [channel] });
    return () => {
      set!.delete(handler);
      if (set!.size === 0) {
        this.handlers.delete(channel);
        if (channel !== "*") this.send({ op: "unsubscribe", channels: [channel] });
      }
    };
  }

  /** Forces a fresh snapshot of a channel (used when a gap was detected). */
  resubscribe(channel: string): void {
    this.send({ op: "unsubscribe", channels: [channel] });
    this.send({ op: "subscribe", channels: [channel] });
  }

  send(v: unknown): void {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(v));
  }
}

const wsBase = (): string => `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}`;

let pub: Feed | null = null;
let priv: Feed | null = null;

/** Shared public market-data feed. */
export function publicFeed(): Feed {
  pub ??= new Feed(`${wsBase()}/ws/public`);
  pub.start();
  return pub;
}

/** Shared private feed (orders, fills, balances); authenticates with the access token on every (re)connect. */
export function privateFeed(): Feed {
  priv ??= new Feed(`${wsBase()}/ws/private`, {
    onOpen: async (send) => {
      const token = await accessToken();
      if (token) send({ op: "auth", token });
    },
  });
  priv.start();
  return priv;
}

export function stopPrivateFeed(): void {
  priv?.stop();
  priv = null;
}
