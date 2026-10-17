// App-wide state shared by widgets. Widgets never talk to fetch/WebSocket
// directly; they use the hooks in hooks.ts, which are built on this file.

import { api, Balance, Market, Order, session } from "./api";
import { privateFeed, stopPrivateFeed } from "./feed";
import { persistedStore, Store } from "./store";

export const markets = new Store<Market[]>([]);
export const selectedMarket = persistedStore<string>("tickless.market.v1", "BTC-USDT");
export const balances = new Store<Balance[]>([]);
export const openOrders = new Store<Order[]>([]);
/** Price clicked in the order book, offered to the order-entry widget. */
export const draftPrice = new Store<string>("");
/** Last private events, newest first (for toasts / activity). */
export const activity = new Store<{ at: number; text: string }[]>([]);

export async function loadMarkets(): Promise<void> {
  const list = await api.markets();
  markets.set(list);
  if (!list.some((m) => m.symbol === selectedMarket.get()) && list[0]) selectedMarket.set(list[0].symbol);
}

async function refreshAccount(): Promise<void> {
  try {
    const [b, o] = await Promise.all([api.balances(), api.openOrders()]);
    balances.set(b);
    openOrders.set(o);
  } catch {
    /* transient: the next private event or reconnect refreshes again */
  }
}

let debounce: ReturnType<typeof setTimeout> | undefined;
export const scheduleAccountRefresh = (): void => {
  clearTimeout(debounce);
  debounce = setTimeout(() => void refreshAccount(), 120);
};

let unsub: (() => void) | null = null;

/** Starts the account state (REST load + private WebSocket) for the logged-in user. */
export function startAccount(): void {
  void refreshAccount();
  const feed = privateFeed();
  unsub?.();
  unsub = feed.subscribe("*", (m) => {
    switch (m.type) {
      case "order":
        activity.update((a) => [{ at: Date.now(), text: `order ${m.order_id} ${m.event}${m.reason ? ` (${String(m.reason).toLowerCase().replace(/^reason_/, "")})` : ""}` }, ...a].slice(0, 30));
        scheduleAccountRefresh();
        break;
      case "fill":
        activity.update((a) => [{ at: Date.now(), text: `fill ${m.side} ${m.qty} @ ${m.price} ${m.market} (${m.role})` }, ...a].slice(0, 30));
        scheduleAccountRefresh();
        break;
      case "balance":
        scheduleAccountRefresh();
        break;
      case "authenticated":
        scheduleAccountRefresh(); // anything missed while disconnected
        break;
    }
  });
}

export function stopAccount(): void {
  unsub?.();
  unsub = null;
  stopPrivateFeed();
  balances.set([]);
  openOrders.set([]);
  activity.set([]);
}

// Start/stop automatically with the session.
session.subscribe(() => {
  if (session.get()) startAccount();
  else stopAccount();
});
