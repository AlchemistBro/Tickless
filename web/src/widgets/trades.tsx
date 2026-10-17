import { useTrades } from "../core/hooks";
import { defineWidget, WidgetProps } from "../core/widget";

function Trades({ market }: WidgetProps) {
  const trades = useTrades(market, 40);
  if (trades.length === 0) return <div className="empty">no trades yet</div>;
  return (
    <table>
      <thead>
        <tr><th>Price</th><th>Qty</th><th>Time</th></tr>
      </thead>
      <tbody>
        {trades.map((t) => (
          <tr key={t.id}>
            <td className={t.side === "buy" ? "good" : "bad"}>{t.price}</td>
            <td>{t.qty}</td>
            <td className="muted">{new Date(t.time).toLocaleTimeString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default defineWidget({
  id: "trades",
  title: "Trades",
  defaultLayout: { x: 19, y: 3, w: 5, h: 16 },
  component: Trades,
});
