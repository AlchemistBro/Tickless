# Widgets: how to add a panel to the terminal (by hand or by prompting an AI)

The terminal is a grid of **widgets**. A widget is **one file** in `src/widgets/`. Drop a file in, and it shows up in the **Widgets ▾** menu, can be dragged and resized, and its position is remembered. Nothing else needs editing: no router, no registry, no imports.

A broken widget cannot take the page down: it is wrapped in an error boundary, and a file that does not follow the contract is rejected with a message in the browser console (and a red warning in the toolbar) while everything else keeps working.

## The contract (`src/core/widget.ts`)

```tsx
import { defineWidget, WidgetProps } from "../core/widget";

function MyThing({ market }: WidgetProps) {          // market = selected market, e.g. "BTC-USDT"
  return <div>hello {market}</div>;
}

export default defineWidget({
  id: "my-thing",                                   // unique, kebab-case, never change it later
  title: "My thing",                                // header + menu label
  defaultLayout: { x: 0, y: 30, w: 8, h: 8 },       // 24-column grid, rows are 24 px; x/y optional
  component: MyThing,
});
```

Rules the loader enforces (see `src/core/registry.ts`): the file must be in `src/widgets/*.tsx`, its **default export** must be a `defineWidget(...)`, `id` must be kebab-case and unique, `w` is 1-24, `h` is a positive integer.

## What a widget may use

Widgets are pure views. Get data with these hooks from `../core/hooks`; they handle snapshots, sequence numbers, reconnects and resync for you.

| Hook | Returns |
|---|---|
| `useBook(market, depth?)` | `{ ready, bids, asks }` rows `{ price, qty, total }`, best first |
| `useTrades(market, limit?)` | recent public trades, newest first |
| `useTicker(market)` | 24 h ticker (`last`, `change_pct`, `high`, `low`, `volume`, ...) or `null` |
| `useCandles(market, interval, onLive)` | candle history (`null` while loading); `onLive(c)` is called for live updates |
| `useMarkets()` / `useSelectedMarket()` | market configuration list / selected symbol |
| `useBalances()` / `useAvailable(asset)` | your balances (needs login) |
| `useOpenOrders()` | your open orders (needs login) |

Actions and state: `api` from `../core/api` (REST, e.g. `api.placeOrder`, `api.cancelOrder`), `session` (is the user logged in; use `useStore(session)`), `draftPrice` / `activity` / `scheduleAccountRefresh` from `../core/state`.

**Money is always a decimal string** (`"50000.25"`). Never convert it to a `number` for arithmetic; use `../core/decimal` (`addDecimal`, `mulDecimal`, `cmpDecimal`, `toFixedDecimal`), which is exact. (Charts are the one exception: a charting library needs numbers for drawing only.)

Styling: use the existing CSS classes in `src/styles.css` (`table`, `.good`/`.bad`/`.muted`, `.kv`, `.form`, `.inline`, `.empty`, buttons `.primary` / `.buy` / `.sell`) and the CSS variables `--buy`, `--sell`, `--accent`, `--panel`, `--line`, `--muted`.

## Prompt template for an AI assistant

> Add a widget to the Tickless terminal. Read `web/WIDGETS.md` and look at `web/src/widgets/trades.tsx` as the pattern. Create exactly one new file `web/src/widgets/<id>.tsx` whose default export is `defineWidget({ id, title, defaultLayout, component })`. Use only the hooks and helpers listed in WIDGETS.md, keep money as decimal strings, show a sensible empty/loading state, and do not modify any other file. The widget should: **<describe what it shows or does>**.
> Then run `npm run typecheck && npm test` in `web/` and fix any failure.

Examples of things that make good widgets: a depth chart from `useBook`, a "spread and mid price" strip, a table of your recent fills from `activity`, a position/PnL view from `useBalances` + `useTicker`, a price-alert box, a market overview table from `api.tickers()`.

## Testing a widget

`npm test` already checks that **every file in `src/widgets/` is a valid, unique widget** (`src/core/registry.test.ts`), so a malformed vibe-coded file fails CI. For behaviour, copy the pattern of `src/tests/orderbook.test.tsx`: mock `../core/feed`, render `Widget.component`, push messages, assert on the DOM. Keep test files **outside** `src/widgets/` (every `.tsx` in that folder is treated as a widget).

## Commands

```
cd web
npm install
npm run dev        # http://localhost:5173, proxies /v1 and /ws to the gateway on :8080 (needs `make up`)
npm run typecheck
npm test
npm run build
```
