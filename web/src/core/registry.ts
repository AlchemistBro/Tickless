import type { WidgetDef } from "./widget";

type Modules = Record<string, { default?: unknown }>;

const ID = /^[a-z][a-z0-9]*(-[a-z0-9]+)*$/;

/** Returns an error message, or null when `x` is a valid WidgetDef. */
export function validateWidget(x: unknown): string | null {
  if (typeof x !== "object" || x === null) return "default export is not an object (use defineWidget({...}))";
  const w = x as Partial<WidgetDef>;
  if (typeof w.id !== "string" || !ID.test(w.id)) return `id must be kebab-case, got ${JSON.stringify(w.id)}`;
  if (typeof w.title !== "string" || !w.title.trim()) return "title is required";
  const l = w.defaultLayout;
  if (!l || !Number.isInteger(l.w) || !Number.isInteger(l.h) || l.w < 1 || l.h < 1 || l.w > 24) {
    return "defaultLayout needs integer w (1-24) and h (>=1)";
  }
  if (typeof w.component !== "function") return "component must be a React component";
  return null;
}

export interface Loaded {
  widgets: WidgetDef[];
  /** One message per rejected file; the rest of the dashboard keeps working. */
  errors: string[];
}

/** Collects the valid widgets of a set of modules, rejecting duplicates and malformed ones. */
export function collectWidgets(mods: Modules): Loaded {
  const widgets: WidgetDef[] = [];
  const errors: string[] = [];
  const seen = new Map<string, string>();
  for (const file of Object.keys(mods).sort()) {
    const def = mods[file].default;
    const problem = validateWidget(def);
    if (problem) {
      errors.push(`${file}: ${problem}`);
      continue;
    }
    const w = def as WidgetDef;
    if (seen.has(w.id)) {
      errors.push(`${file}: id "${w.id}" is already used by ${seen.get(w.id)}`);
      continue;
    }
    seen.set(w.id, file);
    widgets.push(w);
  }
  return { widgets, errors };
}

/** Every widget file in src/widgets is picked up here: adding a file adds a widget. */
export const loaded: Loaded = collectWidgets(import.meta.glob("../widgets/*.tsx", { eager: true }) as Modules);
loaded.errors.forEach((e) => console.error(`[widgets] ${e}`));
