import { useState } from "react";
import { ApiError, api, session } from "../core/api";
import { useOpenOrders } from "../core/hooks";
import { scheduleAccountRefresh } from "../core/state";
import { useStore } from "../core/store";
import { defineWidget } from "../core/widget";

function OpenOrders() {
  const sess = useStore(session);
  const orders = useOpenOrders();
  const [err, setErr] = useState("");
  if (!sess) return <div className="empty">Log in to see your orders.</div>;

  async function run(f: () => Promise<unknown>) {
    setErr("");
    try {
      await f();
      scheduleAccountRefresh();
    } catch (x) {
      setErr(x instanceof ApiError ? x.message : "cannot reach the server");
    }
  }

  return (
    <>
      <div className="inline" style={{ marginBottom: 4 }}>
        <button disabled={orders.length === 0} onClick={() => void run(() => api.cancelAll())}>Cancel all</button>
        {err && <span className="bad">{err}</span>}
      </div>
      {orders.length === 0 ? (
        <div className="empty">No open orders.</div>
      ) : (
        <table>
          <thead>
            <tr><th>Market</th><th>Side</th><th>Price</th><th>Qty</th><th>Filled</th><th>Status</th><th /></tr>
          </thead>
          <tbody>
            {orders.map((o) => (
              <tr key={o.id}>
                <td>{o.market}</td>
                <td className={o.side === "buy" ? "good" : "bad"}>{o.side}</td>
                <td>{o.price ?? "market"}</td>
                <td>{o.quantity}</td>
                <td>{o.filled_quantity}</td>
                <td className="muted">{o.status.replace("_", " ")}</td>
                <td><button onClick={() => void run(() => api.cancelOrder(o.id))}>Cancel</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

export default defineWidget({
  id: "open-orders",
  title: "Open orders",
  defaultLayout: { x: 12, y: 19, w: 12, h: 9 },
  component: OpenOrders,
});
