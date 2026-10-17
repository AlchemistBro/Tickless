import type { ComponentType } from "react";

/** Props every widget receives. Everything else comes from the hooks in hooks.ts. */
export interface WidgetProps {
  /** Currently selected market symbol, e.g. "BTC-USDT". */
  market: string;
}

/**
 * The whole contract of a widget. A widget is ONE file in src/widgets/ whose
 * default export is a WidgetDef; it is discovered automatically (see
 * registry.ts) and appears in the "Widgets" menu. See WIDGETS.md.
 */
export interface WidgetDef {
  /** Unique, kebab-case, stable (saved layouts refer to it). */
  id: string;
  /** Title shown in the widget header and the menu. */
  title: string;
  /** Initial size/position on the 24-column grid (row height 24px). */
  defaultLayout: { w: number; h: number; x?: number; y?: number };
  component: ComponentType<WidgetProps>;
}

/** Identity helper that gives the object literal full type checking. */
export const defineWidget = (w: WidgetDef): WidgetDef => w;
