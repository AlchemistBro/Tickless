import { describe, expect, it } from "vitest";
import { addWidget, COLS, defaultLayout, reconcile, removeWidget } from "./layout";
import { defineWidget } from "./widget";

const w = (id: string, layout: { w: number; h: number; x?: number; y?: number } = { w: 6, h: 4, x: 0, y: 0 }) =>
  defineWidget({ id, title: id, defaultLayout: layout, component: () => null });
const A = w("alpha");
const B = w("beta", { w: 30, h: 4, x: 3, y: 4 });
const C = w("gamma");

describe("layout", () => {
  it("starts from the declared positions and clamps the width to the grid", () => {
    expect(defaultLayout([A, B])).toEqual([
      { i: "alpha", x: 0, y: 0, w: 6, h: 4 },
      { i: "beta", x: 3, y: 4, w: COLS, h: 4 },
    ]);
  });

  it("uses the default layout when nothing valid is saved", () => {
    expect(reconcile(null, [A])).toEqual(defaultLayout([A]));
    expect(reconcile("garbage", [A])).toEqual(defaultLayout([A]));
  });

  it("drops widgets that no longer exist and ignores malformed entries", () => {
    const saved = [
      { i: "alpha", x: 1, y: 2, w: 3, h: 4 },
      { i: "deleted", x: 0, y: 0, w: 1, h: 1 },
      { i: "beta", x: "x", y: 0, w: 1, h: 1 },
      null,
    ];
    expect(reconcile(saved, [A, B], ["alpha", "beta"])).toEqual([{ i: "alpha", x: 1, y: 2, w: 3, h: 4 }]);
  });

  it("keeps a widget the user hid hidden, but shows one that is new since the layout was saved", () => {
    const saved = [{ i: "alpha", x: 0, y: 0, w: 6, h: 4 }];
    const l = reconcile(saved, [A, B, C], ["alpha", "beta"]); // beta was known and hidden; gamma is new
    expect(l.map((x) => x.i)).toEqual(["alpha", "gamma"]);
  });

  it("adds and removes widgets without duplicates", () => {
    let l = defaultLayout([A]);
    l = addWidget(l, A);
    expect(l).toHaveLength(1);
    l = addWidget(l, C);
    expect(l.map((x) => x.i)).toEqual(["alpha", "gamma"]);
    expect(removeWidget(l, "alpha").map((x) => x.i)).toEqual(["gamma"]);
  });
});
