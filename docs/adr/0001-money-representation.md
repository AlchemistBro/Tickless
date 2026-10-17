# 0001 — Money representation

Status: accepted (Phase 1)

## Context
An exchange must never lose or invent a unit of value. Floats are out. Assets differ wildly in scale: BTC has 8 decimals, USDT 6, ETH/ERC-20 18 (wei), so a single `int64` overflows for large wei balances (2^63 ≈ 9.2 ETH).
The matching engine needs the fastest possible arithmetic (SPEC §5: 0 allocs/op, ≥ 1M ops/s); the ledger needs range and exactness.

## Decision
Two representations, with a defined bridge:

1. **Engine domain: `int64` price ticks and `int64` quantity lots.** Each market declares `TickSize` (quote units per tick) and `LotSize` (base units per lot). Orders are rejected at the `orders` service unless on-grid, so the engine only does integer compare/add/sub. `price*qty` is never computed inside the engine.
2. **Ledger/chain domain: `money.Int128`** (`pkg/money`), a signed two's-complement 128-bit integer holding the asset's smallest unit. All operations are overflow-checked and return an error; there is no wrapping API. Stored in PostgreSQL as `NUMERIC(39,0)`, in protobuf as a decimal string.
3. **Bridge: `MarketSpec.QuoteAmount`** converts `(priceTicks, qtyLots)` into quote units as `priceTicks*TickSize*qtyLots*LotSize / 10^BaseDecimals` using a wide intermediate (`math/big`, off the engine hot path) and an explicit `Rounding` mode. Holds round **up** (never under-reserve); a trade's two legs use the same computed amount so the journal entry balances.
4. **Fees** are `ceil(amount*bps/10000)` (`money.Fee`): deterministic rounding toward the exchange (SPEC §6 invariant 8). A fuzz test proves the result is the minimal value that never under-collects.
5. **User-facing text** (`Asset.Parse`/`Format`) accepts only plain decimals — no exponents, separators or excess precision; excess precision is an error, never truncation.

`Int128` add/sub use `math/bits` carries; multiplication and `MulDiv` go through `math/big` for obvious correctness. They are not used per-order in the engine.

## Alternatives considered
- **`math/big` everywhere:** simple and exact, but allocates; unacceptable for the engine, unnecessary for most ledger ops.
- **`shopspring/decimal`:** arbitrary precision decimals invite scale mismatches (summing 1.10 and 1.1 of different scales); integer smallest-units make scale part of the asset, not the value.
- **`int64` everywhere with per-asset scaling:** overflows for wei.
- **`uint128` libraries:** unsigned values make signed postings awkward (postings are signed).

## Consequences
- Every amount crossing a service boundary is a decimal string of smallest units; no float ever appears in protobuf, JSON money fields, SQL or Go money code (`float64` in money paths fails review).
- Correctness is guarded by differential fuzz tests against `math/big` (`pkg/money/fuzz_test.go`).
- Market grids must be chosen so typical notionals are exact; inexact cases are handled by an explicit rounding mode rather than silently.
- Amounts above 2^127−1 (≈1.7e38) are unrepresentable; this exceeds any real supply by 11 orders of magnitude at 18 decimals.
