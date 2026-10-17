# 0006 — Order placement saga, holds and rounding

Status: accepted (Phase 3)

## Context
An order must never reach the matching engine without funds behind it, funds must never be double-spent between concurrent orders, and the system must recover from a crash at any step. Orders (own database) and ledger (own database) cannot share a transaction.

## Decision — placement saga (`services/orders/internal/orders`)
1. **Persist** the order as `PENDING_HOLD`, deduplicated on `(user, client_order_id)` (`order_keys`); a repeated client id returns the original order with `duplicate=true` (SPEC §6.7).
2. **Hold** funds with `ledger.PlaceHold(order_id, ...)` - idempotent by order id; insufficient funds mark the order `REJECTED` and nothing is sent to the engine.
3. In **one transaction**: `PENDING_HOLD -> NEW` and the `PlaceOrder` command into the outbox. The command therefore exists only after the hold.
A sweeper re-drives `PENDING_HOLD` orders older than 5 s, so a crash after step 1 or 2 is repaired; a client retry with the same `client_order_id` also resumes the saga. All steps are idempotent.

## Decision — holds and settlement (`services/ledger`)
- A hold moves `AVAILABLE -> HELD` with a `holds` row (`remaining`). Trades consume `remaining`; when an order is finished (`TakerLeaves/MakerLeaves == 0`, `Cancelled`, or a rejection that is not `DUPLICATE/UNKNOWN`) the **rest is released** exactly once (`release:<order>`).
- Buyers hold the **quote** amount at their limit price (market buys: at their mandatory protective price bound; sellers hold **base**.
- **Rounding:** holds round **up** (never under-reserve); a trade's quote amount rounds **down** at the maker price. Because price <= limit and rounding down never increases the amount, the sum of trade amounts never exceeds the hold (`consumeHold` fails loudly otherwise). Fees are `ceil(amount * bps / 10000)` charged **in the received asset** (buyer pays base, seller pays quote), i.e. rounded toward the exchange (SPEC §6.8).
- One trade = one balanced journal entry spanning both assets; the database enforces per-asset balance with a deferred constraint trigger and non-negativity with `CHECK`.

## Decision — market orders and price bands
- Market orders are IOC/FOK limit orders with a **required protective bound for buys** so the quote hold is bounded; they are executed by the engine with that bound.
- `cancel-all` is expanded in the orders service into single cancel commands; the engine has no per-user index.
- Limit prices must be within +-50% (configurable per market) of the last trade price and each user may have at most 200 open orders per market. Together with the min notional, this bounds the number of live price levels an attacker can create (ADR 0002's sorted-slice worst case).

## Alternatives considered
- Two-phase commit between orders and ledger: operationally heavy and unavailable across PostgreSQL databases behind pgbouncer.
- Placing the order first and holding asynchronously: the engine could match an unfunded order.
- Fees charged in quote for both sides: simpler, but buyers would pay quote beyond their hold; charging in the received asset keeps holds exact.

## Consequences
- An accepted order can briefly be `PENDING_HOLD` (milliseconds normally); ledger downtime degrades order placement to `503` (retryable) without corrupting anything.
- Fee dust: fees round up by < 1 smallest unit per trade in the exchange's favour.
