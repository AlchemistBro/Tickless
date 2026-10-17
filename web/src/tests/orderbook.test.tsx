// Pattern for testing a widget: mock the feed, render the widget, push messages.
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type Handler = (m: Record<string, unknown>) => void;
const feed = vi.hoisted(() => ({
  handlers: new Map<string, Handler>(),
  resubscribed: [] as string[],
  subscribe(ch: string, h: Handler) {
    feed.handlers.set(ch, h);
    return () => feed.handlers.delete(ch);
  },
  resubscribe(ch: string) {
    feed.resubscribed.push(ch);
  },
}));

vi.mock("../core/feed", () => ({ publicFeed: () => feed, privateFeed: () => feed, stopPrivateFeed: () => undefined }));

import { draftPrice } from "../core/state";
import OrderBook from "../widgets/orderbook";
import OrderEntry from "../widgets/order-entry";

const push = (m: Record<string, unknown>) => act(async () => feed.handlers.get("book.BTC-USDT")!(m));

beforeEach(() => {
  feed.handlers.clear();
  feed.resubscribed.length = 0;
  draftPrice.set("");
});
afterEach(cleanup);

describe("order book widget", () => {
  const C = OrderBook.component;

  it("renders a snapshot, applies deltas and shows asks above bids", async () => {
    render(<C market="BTC-USDT" />);
    expect(screen.getByText("connecting…")).toBeTruthy();
    await push({ type: "snapshot", seq: 3, bids: [["99", "1"], ["100", "2"]], asks: [["101", "0.5"], ["102", "1"]] });
    await act(async () => new Promise<void>((r) => requestAnimationFrame(() => r())));
    const prices = screen.getAllByRole("row").map((r) => r.textContent ?? "");
    const i = (p: string) => prices.findIndex((t) => t.startsWith(p));
    expect(i("102")).toBeLessThan(i("101"));
    expect(i("101")).toBeLessThan(i("100"));
    expect(i("100")).toBeLessThan(i("99"));

    await push({ type: "delta", seq: 5, prev: 3, side: "buy", price: "100", qty: "7" });
    await act(async () => new Promise<void>((r) => requestAnimationFrame(() => r())));
    expect(screen.getAllByText("7").length).toBeGreaterThan(0); // new quantity (and the cumulative total)
  });

  it("asks for a fresh snapshot when a delta was missed", async () => {
    render(<C market="BTC-USDT" />);
    await push({ type: "snapshot", seq: 3, bids: [["100", "2"]], asks: [] });
    await push({ type: "delta", seq: 9, prev: 8, side: "buy", price: "100", qty: "9" });
    expect(feed.resubscribed).toEqual(["book.BTC-USDT"]);
  });

  it("clicking a price copies it into the order form draft", async () => {
    render(<C market="BTC-USDT" />);
    await push({ type: "snapshot", seq: 1, bids: [["100", "2"]], asks: [["101", "1"]] });
    await act(async () => new Promise<void>((r) => requestAnimationFrame(() => r())));
    fireEvent.click(screen.getByText("101"));
    expect(draftPrice.get()).toBe("101");
  });
});

describe("order entry widget", () => {
  it("asks anonymous visitors to log in instead of showing a form", () => {
    const C = OrderEntry.component;
    render(<C market="BTC-USDT" />);
    expect(screen.getByText("Log in to trade.")).toBeTruthy();
  });
});
