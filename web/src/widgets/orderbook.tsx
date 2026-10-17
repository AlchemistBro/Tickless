import { BookRow } from "../core/book";
import { parseDecimal } from "../core/decimal";
import { useBook } from "../core/hooks";
import { draftPrice } from "../core/state";
import { defineWidget, WidgetProps } from "../core/widget";

function Rows({ rows, side, max }: { rows: BookRow[]; side: "buy" | "sell"; max: bigint }) {
  return (
    <>
      {rows.map((r) => {
        const pct = max > 0n ? Number((parseDecimal(r.total) * 100n) / max) : 0;
        return (
          <tr key={r.price} className="row-bar clickable" onClick={() => draftPrice.set(r.price)} title="Click to use this price in the order form">
            <td className={side === "buy" ? "good" : "bad"}>
              <i className="bar" style={{ width: `${pct}%`, background: side === "buy" ? "var(--buy)" : "var(--sell)" }} />
              {r.price}
            </td>
            <td>{r.qty}</td>
            <td>{r.total}</td>
          </tr>
        );
      })}
    </>
  );
}

function OrderBook({ market }: WidgetProps) {
  const { ready, bids, asks } = useBook(market, 14);
  if (!ready) return <div className="empty">connecting…</div>;
  const asksTopDown = [...asks].reverse(); // highest ask at the top, best ask next to the spread
  const maxTotal = [bids.at(-1)?.total, asks.at(-1)?.total].reduce<bigint>(
    (m, t) => (t && parseDecimal(t) > m ? parseDecimal(t) : m),
    0n,
  );
  return (
    <table>
      <thead>
        <tr><th>Price</th><th>Qty</th><th>Total</th></tr>
      </thead>
      <tbody>
        <Rows rows={asksTopDown} side="sell" max={maxTotal} />
        <tr><td colSpan={3} className="muted" style={{ textAlign: "center" }}>{bids[0] && asks[0] ? `${bids[0].price} / ${asks[0].price}` : "—"}</td></tr>
        <Rows rows={bids} side="buy" max={maxTotal} />
      </tbody>
    </table>
  );
}

export default defineWidget({
  id: "orderbook",
  title: "Order book",
  defaultLayout: { x: 14, y: 3, w: 5, h: 16 },
  component: OrderBook,
});
