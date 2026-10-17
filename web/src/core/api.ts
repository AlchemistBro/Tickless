// REST client for the gateway (api/openapi/openapi.yaml). Money is always a
// decimal string. A session (access + refresh token) is kept in localStorage;
// the access token is refreshed shortly before it expires and once more if a
// request still comes back 401.

import { persistedStore, Store } from "./store";

export interface Session {
  email: string;
  accessToken: string;
  accessExpiresAt: number; // epoch ms
  refreshToken: string;
}

export const session: Store<Session | null> = persistedStore<Session | null>("tickless.session.v1", null);

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

export interface Market {
  symbol: string;
  base_asset: string;
  quote_asset: string;
  tick_size: string;
  lot_size: string;
  min_notional: string;
  maker_fee_bps: number;
  taker_fee_bps: number;
  status: "active" | "halted";
}

export interface Balance {
  asset: string;
  available: string;
  held: string;
}

export type OrderStatus = "pending_hold" | "new" | "partially_filled" | "filled" | "cancelled" | "rejected";

export interface Order {
  id: string;
  market: string;
  side: "buy" | "sell";
  type: "limit" | "market";
  time_in_force: string;
  post_only: boolean;
  price?: string;
  quantity: string;
  filled_quantity: string;
  status: OrderStatus;
  client_order_id?: string;
  reject_reason?: string;
  created_at: string;
}

export interface PlaceOrderInput {
  market: string;
  side: "buy" | "sell";
  type: "limit" | "market";
  price?: string;
  quantity: string;
  time_in_force?: "GTC" | "IOC" | "FOK";
  post_only?: boolean;
  client_order_id?: string;
}

export interface Ticker {
  market: string;
  last: string;
  open: string;
  high: string;
  low: string;
  volume: string;
  quote_volume: string;
  trades: number;
  change_pct: string;
}

export interface Candle {
  t: number;
  o: string;
  h: string;
  l: string;
  c: string;
  v: string;
}

export interface PublicTrade {
  id: string;
  price: string;
  qty: string;
  side: "buy" | "sell";
  time: string;
}

interface Tokens {
  access_token: string;
  access_expires_at: string;
  refresh_token: string;
}

function toSession(email: string, t: Tokens): Session {
  return {
    email,
    accessToken: t.access_token,
    accessExpiresAt: Date.parse(t.access_expires_at),
    refreshToken: t.refresh_token,
  };
}

async function parse<T>(res: Response): Promise<T> {
  if (res.status === 204) return undefined as T;
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const e = (body as { error?: { code?: string; message?: string } }).error;
    throw new ApiError(res.status, e?.code ?? "error", e?.message ?? res.statusText);
  }
  return body as T;
}

async function raw<T>(method: string, path: string, body?: unknown, token?: string): Promise<T> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  return parse<T>(res);
}

let refreshing: Promise<Session | null> | null = null;

/** Refreshes the session once, sharing the request between concurrent callers. */
export function refreshSession(): Promise<Session | null> {
  const cur = session.get();
  if (!cur) return Promise.resolve(null);
  refreshing ??= raw<Tokens>("POST", "/v1/auth/refresh", { refresh_token: cur.refreshToken })
    .then((t) => {
      const next = toSession(cur.email, t);
      session.set(next);
      return next;
    })
    .catch(() => {
      session.set(null); // refresh token rejected: the user must log in again
      return null;
    })
    .finally(() => {
      refreshing = null;
    });
  return refreshing;
}

/** Returns a valid access token, refreshing it when it is about to expire. */
export async function accessToken(): Promise<string | null> {
  const s = session.get();
  if (!s) return null;
  if (s.accessExpiresAt - Date.now() < 30_000) return (await refreshSession())?.accessToken ?? null;
  return s.accessToken;
}

async function authed<T>(method: string, path: string, body?: unknown): Promise<T> {
  let token = await accessToken();
  if (!token) throw new ApiError(401, "unauthenticated", "please log in");
  try {
    return await raw<T>(method, path, body, token);
  } catch (e) {
    if (!(e instanceof ApiError) || e.status !== 401) throw e;
    token = (await refreshSession())?.accessToken ?? null;
    if (!token) throw e;
    return raw<T>(method, path, body, token);
  }
}

export const api = {
  async signup(email: string, password: string): Promise<void> {
    await raw("POST", "/v1/auth/signup", { email, password });
  },
  async login(email: string, password: string): Promise<void> {
    session.set(toSession(email, await raw<Tokens>("POST", "/v1/auth/login", { email, password })));
  },
  async logout(): Promise<void> {
    const s = session.get();
    session.set(null);
    if (s) await raw("POST", "/v1/auth/logout", { refresh_token: s.refreshToken }).catch(() => undefined);
  },

  markets: () => raw<{ markets: Market[] }>("GET", "/v1/markets").then((r) => r.markets),
  tickers: () => raw<{ tickers: Ticker[] }>("GET", "/v1/market/tickers").then((r) => r.tickers),
  candles: (market: string, interval: string, limit = 500) =>
    raw<{ candles: Candle[] }>("GET", `/v1/market/${market}/candles?interval=${interval}&limit=${limit}`).then((r) => r.candles),
  trades: (market: string, limit = 50) =>
    raw<{ trades: PublicTrade[] }>("GET", `/v1/market/${market}/trades?limit=${limit}`).then((r) => r.trades),

  balances: () => authed<{ balances: Balance[] }>("GET", "/v1/balances").then((r) => r.balances),
  openOrders: (market?: string) =>
    authed<{ orders: Order[] }>("GET", `/v1/orders?open=true${market ? `&market=${market}` : ""}`).then((r) => r.orders),
  orders: (market?: string, limit = 50) =>
    authed<{ orders: Order[] }>("GET", `/v1/orders?limit=${limit}${market ? `&market=${market}` : ""}`).then((r) => r.orders),
  placeOrder: (o: PlaceOrderInput) => authed<Order>("POST", "/v1/orders", o),
  cancelOrder: (id: string) => authed<Order>("DELETE", `/v1/orders/${id}`),
  cancelAll: (market?: string) => authed<{ cancel_requested: number }>("DELETE", `/v1/orders${market ? `?market=${market}` : ""}`),
};

/** Random id for client_order_id (idempotent order submission). */
export function newClientOrderId(): string {
  const b = new Uint8Array(12);
  crypto.getRandomValues(b);
  return "ui-" + [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}
