import { useTicker } from "../core/hooks";
import { defineWidget, WidgetProps } from "../core/widget";

function Ticker({ market }: WidgetProps) {
  const t = useTicker(market);
  if (!t) return <div className="empty">no trades in the last 24 h</div>;
  const up = !t.change_pct.startsWith("-");
  return (
    <div className="kv">
      <div><span>{market} last</span><b className="big">{t.last}</b></div>
      <div><span>24h change</span><b className={up ? "good" : "bad"}>{up ? "+" : ""}{t.change_pct}%</b></div>
      <div><span>24h high</span>{t.high}</div>
      <div><span>24h low</span>{t.low}</div>
      <div><span>24h volume</span>{t.volume}</div>
      <div><span>24h quote volume</span>{t.quote_volume}</div>
      <div><span>trades</span>{t.trades}</div>
    </div>
  );
}

export default defineWidget({
  id: "ticker",
  title: "Ticker",
  defaultLayout: { x: 0, y: 0, w: 24, h: 3 },
  component: Ticker,
});
