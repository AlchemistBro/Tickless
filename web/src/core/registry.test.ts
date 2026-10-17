import { describe, expect, it } from "vitest";
import { collectWidgets, loaded, validateWidget } from "./registry";
import { defineWidget } from "./widget";

const ok = defineWidget({ id: "my-widget", title: "Mine", defaultLayout: { w: 6, h: 6 }, component: () => null });

describe("widget validation (guards vibe-coded widgets)", () => {
  it("accepts a well-formed widget", () => {
    expect(validateWidget(ok)).toBeNull();
  });

  it.each([
    ["not an object", 42],
    ["null", null],
    ["missing id", { ...ok, id: undefined }],
    ["uppercase id", { ...ok, id: "MyWidget" }],
    ["id with spaces", { ...ok, id: "my widget" }],
    ["empty title", { ...ok, title: " " }],
    ["layout missing", { ...ok, defaultLayout: undefined }],
    ["layout width over 24", { ...ok, defaultLayout: { w: 25, h: 3 } }],
    ["fractional height", { ...ok, defaultLayout: { w: 3, h: 2.5 } }],
    ["component missing", { ...ok, component: undefined }],
  ])("rejects: %s", (_name, bad) => {
    expect(validateWidget(bad)).not.toBeNull();
  });

  it("rejects duplicates and keeps the rest working", () => {
    const { widgets, errors } = collectWidgets({
      "../widgets/a.tsx": { default: ok },
      "../widgets/b.tsx": { default: { ...ok, title: "Copy" } },
      "../widgets/c.tsx": { default: { nonsense: true } },
      "../widgets/d.tsx": { default: { ...ok, id: "other" } },
    });
    expect(widgets.map((w) => w.id)).toEqual(["my-widget", "other"]);
    expect(errors).toHaveLength(2);
    expect(errors[0]).toMatch(/b\.tsx.*already used by \.\.\/widgets\/a\.tsx/);
  });
});

describe("the widgets shipped in src/widgets", () => {
  it("are all valid, unique and discovered", () => {
    expect(loaded.errors).toEqual([]);
    const ids = loaded.widgets.map((w) => w.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toEqual(expect.arrayContaining(["ticker", "chart", "orderbook", "trades", "balances", "order-entry", "open-orders", "activity"]));
  });
});
