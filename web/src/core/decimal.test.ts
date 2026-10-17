import { describe, expect, it } from "vitest";
import { addDecimal, cmpDecimal, formatDecimal, isDecimal, mulDecimal, parseDecimal, toFixedDecimal } from "./decimal";

describe("decimal", () => {
  it("round-trips canonical decimals exactly", () => {
    for (const s of ["0", "1", "0.5", "50000.25", "0.000000000000000001", "123456789012345678901234567890.123456789012345678", "-0.1", "-42"]) {
      expect(formatDecimal(parseDecimal(s))).toBe(s);
    }
  });

  it("normalises trailing zeros and leading zeros", () => {
    expect(formatDecimal(parseDecimal("0001.500"))).toBe("1.5");
    expect(formatDecimal(parseDecimal("-0.0"))).toBe("0");
  });

  it("adds without the floating point errors of Number", () => {
    expect(0.1 + 0.2).not.toBe(0.3); // the reason this module exists
    expect(addDecimal("0.1", "0.2")).toBe("0.3");
    expect(addDecimal("0.000000000000000001", "0.000000000000000002")).toBe("0.000000000000000003");
    expect(addDecimal("9007199254740993", "1")).toBe("9007199254740994"); // beyond 2^53
  });

  it("multiplies and truncates (never rounds up)", () => {
    expect(mulDecimal("50000.25", "0.4")).toBe("20000.1");
    expect(mulDecimal("0.00000003", "0.5", 8)).toBe("0.00000001");
    expect(mulDecimal("0.00000001", "0.5", 8)).toBe("0");
  });

  it("compares numerically, not lexically", () => {
    expect(cmpDecimal("9", "10")).toBe(-1);
    expect(cmpDecimal("10.5", "10.50")).toBe(0);
    expect(cmpDecimal("0.1", "-0.1")).toBe(1);
  });

  it("formats a fixed number of digits by truncation", () => {
    expect(toFixedDecimal("1.999", 2)).toBe("1.99");
    expect(toFixedDecimal("3", 2)).toBe("3.00");
    expect(toFixedDecimal("-1.5", 0)).toBe("-1");
  });

  it("rejects what is not a plain decimal", () => {
    for (const s of ["", "abc", "1e5", "1,5", ".5", "5.", "0x10", " ", "--1"]) {
      expect(isDecimal(s)).toBe(false);
      expect(() => parseDecimal(s)).toThrow();
    }
    expect(() => parseDecimal("1." + "0".repeat(19))).toThrow(/too many/);
  });
});
