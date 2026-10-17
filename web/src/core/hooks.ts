// Data hooks for widgets. A widget calls e.g. useBook(market) and renders the
// result; connection handling, snapshots, sequence checks and resync live here.

import { useEffect, useMemo, useRef, useState } from "react";
import { api, Candle, PublicTrade, Ticker } from "./api";
import { BookModel, BookRow, DeltaMsg, SnapshotMsg } from "./book";
import { publicFeed } from "./feed";
import { balances, markets, openOrders, selectedMarket } from "./state";
import { useStore } from "./store";

export const useMarkets = () => useStore(markets);
export const useSelectedMarket = () => useStore(selectedMarket);
export const useBalances = () => useStore(balances);
export const useOpenOrders = () => useStore(openOrders);

export interface BookView {
  ready: boolean;
  bids: BookRow[];
  asks: BookRow[];
  spread?: string;
}

/** Live order book: snapshot + deltas, with gap detection and automatic resync. */
export function useBook(market: string, depth = 15): BookView {
  const [view, setView] = useState<BookView>({ ready: false, bids: [], asks: [] });
  useEffect(() => {
    const feed = publicFeed();
    const channel = `book.${market}`;
    const model = new BookModel();
    let raf = 0;
    const publish = () => {
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() =>
        setView({ ready: model.ready, bids: model.bidRows(depth), asks: model.askRows(depth) }),
      );
    };
    setView({ ready: false, bids: [], asks: [] });
    const off = feed.subscribe(channel, (m) => {
      if (m.type === "snapshot") {
        model.applySnapshot(m as unknown as SnapshotMsg);
        publish();
      } else if (m.type === "delta") {
        if (!model.applyDelta(m as unknown as DeltaMsg)) {
          model.reset();
          feed.resubscribe(channel); // a message was missed: ask for a fresh snapshot
        } else publish();
      }
    });
    return () => {
      off();
      cancelAnimationFrame(raf);
    };
  }, [market, depth]);
  return view;
}

/** Recent public trades, newest first. */
export function useTrades(market: string, limit = 40): PublicTrade[] {
  const [trades, setTrades] = useState<PublicTrade[]>([]);
  useEffect(() => {
    let alive = true;
    setTrades([]);
    api.trades(market, limit).then((t) => alive && setTrades(t)).catch(() => undefined);
    const off = publicFeed().subscribe(`trades.${market}`, (m) => {
      if (m.type === "trade") {
        const t = m as unknown as PublicTrade;
        setTrades((cur) => (cur.some((x) => x.id === t.id) ? cur : [t, ...cur].slice(0, limit)));
      }
    });
    return () => {
      alive = false;
      off();
    };
  }, [market, limit]);
  return trades;
}

/** Rolling 24 h ticker (REST once, then live updates). */
export function useTicker(market: string): Ticker | null {
  const [t, setT] = useState<Ticker | null>(null);
  useEffect(() => {
    let alive = true;
    setT(null);
    api.tickers().then((all) => alive && setT(all.find((x) => x.market === market) ?? null)).catch(() => undefined);
    const off = publicFeed().subscribe(`ticker.${market}`, (m) => {
      if (m.type === "ticker") setT(m as unknown as Ticker);
    });
    return () => {
      alive = false;
      off();
    };
  }, [market]);
  return t;
}

/** Candles for charting: history over REST, then the live candle updates of the same interval. */
export function useCandles(market: string, interval: string, onLive: (c: Candle) => void): Candle[] | null {
  const [candles, setCandles] = useState<Candle[] | null>(null);
  const live = useRef(onLive);
  live.current = onLive;
  useEffect(() => {
    let alive = true;
    setCandles(null);
    api.candles(market, interval).then((c) => alive && setCandles(c)).catch(() => alive && setCandles([]));
    const off = publicFeed().subscribe(`candles.${market}.${interval}`, (m) => {
      if (m.type === "candle") live.current(m as unknown as Candle);
    });
    return () => {
      alive = false;
      off();
    };
  }, [market, interval]);
  return candles;
}

/** Available balance of an asset (decimal string, "0" if none). */
export function useAvailable(asset: string): string {
  const b = useBalances();
  return useMemo(() => b.find((x) => x.asset === asset)?.available ?? "0", [b, asset]);
}
