import { FormEvent, useEffect, useState } from "react";
import { ApiError, api, newClientOrderId, session } from "../core/api";
import { isDecimal, mulDecimal } from "../core/decimal";
import { useAvailable, useMarkets } from "../core/hooks";
import { draftPrice, scheduleAccountRefresh } from "../core/state";
import { useStore } from "../core/store";
import { defineWidget, WidgetProps } from "../core/widget";

function OrderEntry({ market }: WidgetProps) {
  const sess = useStore(session);
  const spec = useMarkets().find((m) => m.symbol === market);
  const draft = useStore(draftPrice);
  const [side, setSide] = useState<"buy" | "sell">("buy");
  const [type, setType] = useState<"limit" | "market">("limit");
  const [tif, setTif] = useState<"GTC" | "IOC" | "FOK">("GTC");
  const [postOnly, setPostOnly] = useState(false);
  const [price, setPrice] = useState("");
  const [qty, setQty] = useState("");
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [busy, setBusy] = useState(false);

  const [base, quote] = market.split("-");
  const available = useAvailable(side === "buy" ? quote : base);

  useEffect(() => {
    if (draft) setPrice(draft); // a price clicked in the order book
  }, [draft]);

  if (!sess) return <div className="empty">Log in to trade.</div>;

  const priceOk = price === "" ? type === "market" && side === "sell" : isDecimal(price);
  const qtyOk = isDecimal(qty) && qty !== "0";
  const total = priceOk && qtyOk && price !== "" ? mulDecimal(price, qty, 8) : "";

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setMsg(null);
    try {
      const o = await api.placeOrder({
        market,
        side,
        type,
        quantity: qty,
        price: price || undefined,
        time_in_force: type === "market" ? (tif === "GTC" ? "IOC" : tif) : tif,
        post_only: postOnly && type === "limit" && tif === "GTC",
        client_order_id: newClientOrderId(), // idempotent: a retry after a network error cannot double-submit
      });
      setMsg({ ok: true, text: `order ${o.id} ${o.status}` });
      scheduleAccountRefresh();
    } catch (x) {
      setMsg({ ok: false, text: x instanceof ApiError ? x.message : "cannot reach the server" });
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="form" onSubmit={submit}>
      <div className="tabs">
        <button type="button" className={`buy ${side === "buy" ? "on" : ""}`} onClick={() => setSide("buy")}>Buy {base}</button>
        <button type="button" className={`sell ${side === "sell" ? "on" : ""}`} onClick={() => setSide("sell")}>Sell {base}</button>
      </div>
      <div className="inline">
        <select value={type} onChange={(e) => setType(e.target.value as "limit" | "market")}>
          <option value="limit">Limit</option>
          <option value="market">Market</option>
        </select>
        <select value={tif} onChange={(e) => setTif(e.target.value as "GTC" | "IOC" | "FOK")}>
          <option value="GTC" disabled={type === "market"}>GTC</option>
          <option value="IOC">IOC</option>
          <option value="FOK">FOK</option>
        </select>
        {type === "limit" && tif === "GTC" && (
          <label className="inline"><input type="checkbox" checked={postOnly} onChange={(e) => setPostOnly(e.target.checked)} />post-only</label>
        )}
      </div>
      <label>
        {type === "market" ? (side === "buy" ? `Max price (${quote}) — protective bound` : `Min price (${quote}) — optional`) : `Price (${quote})`}
        <input value={price} onChange={(e) => setPrice(e.target.value)} placeholder={spec ? `step ${spec.tick_size}` : ""} inputMode="decimal" />
      </label>
      <label>
        Quantity ({base})
        <input value={qty} onChange={(e) => setQty(e.target.value)} placeholder={spec ? `step ${spec.lot_size}` : ""} inputMode="decimal" />
      </label>
      <div className="muted">
        available {available} {side === "buy" ? quote : base}
        {total && <> · est. {total} {quote}</>}
        {spec && <> · taker fee {spec.taker_fee_bps} bps</>}
      </div>
      <button className={side} disabled={busy || !priceOk || !qtyOk || spec?.status === "halted"}>
        {spec?.status === "halted" ? "Market halted" : `${side === "buy" ? "Buy" : "Sell"} ${base}`}
      </button>
      {msg && <div className={msg.ok ? "good" : "bad"}>{msg.text}</div>}
    </form>
  );
}

export default defineWidget({
  id: "order-entry",
  title: "Order entry",
  defaultLayout: { x: 6, y: 19, w: 6, h: 14 },
  component: OrderEntry,
});
