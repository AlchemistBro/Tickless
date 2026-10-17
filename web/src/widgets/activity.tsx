import { activity } from "../core/state";
import { useStore } from "../core/store";
import { defineWidget } from "../core/widget";

function Activity() {
  const items = useStore(activity);
  if (items.length === 0) return <div className="empty">Order and fill events from your private feed appear here.</div>;
  return (
    <div>
      {items.map((a, i) => (
        <div key={a.at + ":" + i}>
          <span className="muted">{new Date(a.at).toLocaleTimeString()} </span>
          {a.text}
        </div>
      ))}
    </div>
  );
}

export default defineWidget({
  id: "activity",
  title: "Activity",
  defaultLayout: { x: 12, y: 28, w: 12, h: 5 },
  component: Activity,
});
