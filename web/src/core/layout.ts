import type { WidgetDef } from "./widget";

export interface Item {
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

export const COLS = 24;

/** The layout a fresh user gets: every widget at its declared position. */
export function defaultLayout(widgets: WidgetDef[]): Item[] {
  return widgets.map((w) => ({
    i: w.id,
    x: w.defaultLayout.x ?? 0,
    y: w.defaultLayout.y ?? Number.MAX_SAFE_INTEGER, // react-grid-layout compacts this to the bottom
    w: Math.min(w.defaultLayout.w, COLS),
    h: w.defaultLayout.h,
  }));
}

/**
 * Reconciles a saved layout with the widgets that exist now: entries of
 * widgets that were deleted are dropped, malformed entries are ignored. A
 * widget missing from the saved layout stays hidden (the user hid it) unless
 * it is new since the layout was saved (`knownIds` tells which ones were known).
 */
export function reconcile(saved: unknown, widgets: WidgetDef[], knownIds: string[] = []): Item[] {
  if (!Array.isArray(saved)) return defaultLayout(widgets);
  const byId = new Map(widgets.map((w) => [w.id, w]));
  const out: Item[] = [];
  for (const raw of saved as Partial<Item>[]) {
    const w = raw && typeof raw.i === "string" ? byId.get(raw.i) : undefined;
    if (!w || ![raw.x, raw.y, raw.w, raw.h].every((n) => Number.isFinite(n))) continue;
    out.push({ i: w.id, x: raw.x!, y: raw.y!, w: Math.min(raw.w!, COLS), h: raw.h! });
  }
  const present = new Set(out.map((o) => o.i));
  const known = new Set(knownIds);
  return [...out, ...defaultLayout(widgets.filter((w) => !present.has(w.id) && !known.has(w.id)))];
}

/**
 * Shows a widget again at the left edge below everything else. Using the
 * widget's declared position here made it collide with whatever the user had
 * moved there meanwhile, pushing other panels around on every toggle.
 */
export function addWidget(layout: Item[], w: WidgetDef): Item[] {
  if (layout.some((l) => l.i === w.id)) return layout;
  const bottom = layout.reduce((m, l) => Math.max(m, l.y + l.h), 0);
  const d = defaultLayout([w])[0];
  return [...layout, { ...d, x: 0, y: bottom }];
}

export const sameLayout = (a: Item[], b: Item[]): boolean =>
  a.length === b.length &&
  a.every((p) => {
    const q = b.find((o) => o.i === p.i);
    return !!q && q.x === p.x && q.y === p.y && q.w === p.w && q.h === p.h;
  });

export const removeWidget = (layout: Item[], id: string): Item[] => layout.filter((l) => l.i !== id);
