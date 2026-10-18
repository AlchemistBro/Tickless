import { Component, ReactNode, useMemo, useState } from "react";
import GridLayout, { WidthProvider } from "react-grid-layout";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";
import { addWidget, COLS, Item, reconcile, removeWidget, sameLayout } from "./layout";
import { loaded } from "./registry";
import { persistedStore, useStore } from "./store";

const Grid = WidthProvider(GridLayout);

interface Saved {
  layout: Item[] | null;
  /** widget ids that existed when the layout was saved (to tell "hidden" from "new") */
  known: string[];
}
const saved = persistedStore<Saved>("tickless.layout.v2", { layout: null, known: [] });

/** A crashing widget shows its error instead of taking the whole dashboard down. */
class Boundary extends Component<{ id: string; children: ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  componentDidCatch(error: Error) {
    console.error(`[widget ${this.props.id}]`, error);
  }
  render() {
    return this.state.error ? (
      <div className="widget-error">
        <b>{this.props.id} crashed</b>
        <pre>{this.state.error.message}</pre>
        <button onClick={() => this.setState({ error: null })}>retry</button>
      </div>
    ) : (
      this.props.children
    );
  }
}

export function Dashboard({ market }: { market: string }) {
  const s = useStore(saved);
  const allIds = loaded.widgets.map((w) => w.id);
  const layout = useMemo(() => reconcile(s.layout, loaded.widgets, s.known), [s]);
  const [menu, setMenu] = useState(false);
  const byId = new Map(loaded.widgets.map((w) => [w.id, w]));
  const commit = (l: Item[]) => saved.set({ layout: l, known: allIds });

  return (
    <div className="dashboard">
      <div className="toolbar">
        <button onClick={() => setMenu(!menu)}>Widgets ▾</button>
        <button onClick={() => saved.set({ layout: null, known: [] })}>Reset layout</button>
        {loaded.errors.length > 0 && (
          <span className="bad" title={loaded.errors.join("\n")}>
            ⚠ {loaded.errors.length} widget file(s) rejected — see the browser console
          </span>
        )}
        {menu && (
          <div className="menu">
            {loaded.widgets.map((w) => {
              const on = layout.some((l) => l.i === w.id);
              return (
                <label key={w.id}>
                  <input type="checkbox" checked={on} onChange={() => commit(on ? removeWidget(layout, w.id) : addWidget(layout, w))} />
                  {w.title}
                </label>
              );
            })}
          </div>
        )}
      </div>
      <Grid
        className="grid"
        layout={layout}
        cols={COLS}
        rowHeight={24}
        margin={[8, 8]}
        draggableHandle=".widget-head"
        compactType="vertical"
        onLayoutChange={(l) => {
          // Fires on every re-render with the compacted result; only persist real changes
          // (otherwise commit -> new layout -> change event feeds back on itself).
          const next = l.map(({ i, x, y, w, h }) => ({ i, x, y, w, h }));
          if (!sameLayout(next, layout)) commit(next);
        }}
      >
        {layout.map((l) => {
          const w = byId.get(l.i)!;
          const C = w.component;
          return (
            <div key={w.id} className="widget">
              <div className="widget-head">
                <span>{w.title}</span>
                <button title="Hide" onMouseDown={(e) => e.stopPropagation()} onClick={() => commit(removeWidget(layout, w.id))}>
                  ×
                </button>
              </div>
              <div className="widget-body">
                <Boundary id={w.id}>
                  <C market={market} />
                </Boundary>
              </div>
            </div>
          );
        })}
      </Grid>
    </div>
  );
}
