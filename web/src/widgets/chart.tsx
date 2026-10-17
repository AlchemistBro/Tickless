import { CandlestickData, createChart, IChartApi, ISeriesApi, UTCTimestamp } from "lightweight-charts";
import { useEffect, useRef, useState } from "react";
import { Candle } from "../core/api";
import { useCandles } from "../core/hooks";
import { defineWidget, WidgetProps } from "../core/widget";

const INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"];

// The charting library wants numbers; this is display only, never used for money maths.
const bar = (c: Candle): CandlestickData => ({
  time: c.t as UTCTimestamp,
  open: Number(c.o),
  high: Number(c.h),
  low: Number(c.l),
  close: Number(c.c),
});

function Chart({ market }: WidgetProps) {
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const [interval, setInterval_] = useState("1m");

  useEffect(() => {
    const c = createChart(box.current!, {
      layout: { background: { color: "transparent" }, textColor: "#7f8aa3" },
      grid: { vertLines: { color: "#1b2233" }, horzLines: { color: "#1b2233" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#232b3d" },
      rightPriceScale: { borderColor: "#232b3d" },
      autoSize: true,
    });
    chart.current = c;
    series.current = c.addCandlestickSeries({ upColor: "#26a69a", downColor: "#ef5350", wickUpColor: "#26a69a", wickDownColor: "#ef5350", borderVisible: false });
    return () => {
      c.remove();
      chart.current = series.current = null;
    };
  }, []);

  const candles = useCandles(market, interval, (c) => series.current?.update(bar(c)));
  useEffect(() => {
    if (!series.current || !candles) return;
    series.current.setData(candles.map(bar));
    chart.current?.timeScale().fitContent();
  }, [candles]);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div className="inline" style={{ marginBottom: 4 }}>
        {INTERVALS.map((i) => (
          <button key={i} className={i === interval ? "primary" : ""} onClick={() => setInterval_(i)}>
            {i}
          </button>
        ))}
        {candles && candles.length === 0 && <span className="muted">no trades yet</span>}
      </div>
      <div ref={box} style={{ flex: 1, minHeight: 0 }} />
    </div>
  );
}

export default defineWidget({
  id: "chart",
  title: "Chart",
  defaultLayout: { x: 0, y: 3, w: 14, h: 16 },
  component: Chart,
});
