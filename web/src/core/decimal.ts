// Exact decimal arithmetic on strings. Money is never a JavaScript number:
// the API sends decimal strings and every sum shown in the UI goes through
// BigInt here. (Numbers are used only where a chart library demands them.)

const SCALE = 18;
const ONE = 10n ** BigInt(SCALE);

export function parseDecimal(s: string): bigint {
  const m = /^(-?)(\d+)(?:\.(\d+))?$/.exec(s.trim());
  if (!m) throw new Error(`invalid decimal: ${JSON.stringify(s)}`);
  const frac = (m[3] ?? "").padEnd(SCALE, "0");
  if (frac.length > SCALE) throw new Error("too many fractional digits");
  const v = BigInt(m[2]) * ONE + BigInt(frac || "0");
  return m[1] === "-" ? -v : v;
}

export function isDecimal(s: string): boolean {
  return /^-?\d+(\.\d+)?$/.test(s.trim());
}

export function formatDecimal(v: bigint, maxFrac = SCALE): string {
  const neg = v < 0n;
  const abs = neg ? -v : v;
  const int = abs / ONE;
  const frac = (abs % ONE).toString().padStart(SCALE, "0").slice(0, maxFrac).replace(/0+$/, "");
  const out = frac ? `${int}.${frac}` : `${int}`;
  return neg ? `-${out}` : out;
}

export const addDecimal = (a: string, b: string): string => formatDecimal(parseDecimal(a) + parseDecimal(b));

/** price * quantity, rounded down to `maxFrac` fractional digits. */
export function mulDecimal(a: string, b: string, maxFrac = 8): string {
  return formatDecimal((parseDecimal(a) * parseDecimal(b)) / ONE, maxFrac);
}

export const cmpDecimal = (a: string, b: string): number => {
  const x = parseDecimal(a);
  const y = parseDecimal(b);
  return x < y ? -1 : x > y ? 1 : 0;
};

/** Fixed number of fractional digits for display (truncates, never rounds up). */
export function toFixedDecimal(s: string, digits: number): string {
  const v = parseDecimal(s);
  const neg = v < 0n;
  const abs = neg ? -v : v;
  const scaled = abs / 10n ** BigInt(SCALE - digits);
  const str = scaled.toString().padStart(digits + 1, "0");
  const out = digits === 0 ? str : `${str.slice(0, -digits)}.${str.slice(-digits)}`;
  return neg ? `-${out}` : out;
}
