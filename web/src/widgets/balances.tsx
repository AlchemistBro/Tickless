import { session } from "../core/api";
import { useBalances } from "../core/hooks";
import { useStore } from "../core/store";
import { defineWidget } from "../core/widget";

function Balances() {
  const sess = useStore(session);
  const balances = useBalances();
  if (!sess) return <div className="empty">Log in to see your balances.</div>;
  if (balances.length === 0) return <div className="empty">No balances yet. Deposits arrive in a later phase; in development use `make seed`.</div>;
  return (
    <table>
      <thead>
        <tr><th>Asset</th><th>Available</th><th>On hold</th></tr>
      </thead>
      <tbody>
        {balances.map((b) => (
          <tr key={b.asset}>
            <td>{b.asset}</td>
            <td>{b.available}</td>
            <td className="muted">{b.held}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default defineWidget({
  id: "balances",
  title: "Balances",
  defaultLayout: { x: 0, y: 19, w: 6, h: 9 },
  component: Balances,
});
