# SPEC §6 invariants and the tests that prove them

| # | Invariant | Enforced by | Proven by |
|---|---|---|---|
| 1 | Every journal entry sums to zero per asset | Application check in `session.post` **and** a deferred constraint trigger on `postings` (`ledger/migrations/0001_init.sql`) | `TestDatabaseRejectsUnbalancedEntriesAndNegativeBalances` (direct SQL cannot commit an unbalanced entry); `TestRandomOperationsPreserveInvariants` (checked after each of 150 random operations); e2e `assertLedgerInvariants` (SQL on the live ledger after every trading scenario) |
| 2 | No available balance is ever negative | `CHECK (kind = 'EXTERNAL' OR amount >= 0)` on `balances` | same DB test (direct `UPDATE` to -1 fails); `TestHoldReleaseIdempotentAndInsufficientFunds`; e2e `TestInsufficientFundsAndValidation` |
| 3 | Sum of user + fee + hot-wallet accounts per asset equals deposits minus withdrawals | `ledger_verify()` + per-asset zero-sum check (`Store.Verify`) | `Store.Verify` is asserted in the ledger tests; the scheduled reconciler (Phase 7) runs it on every cycle |
| 4 | A deposit `(chain, txid, index)` is credited at most once | `credit:<key>` business key (idempotent journal entry) | `TestCreditIdempotentByBusinessKey` (ledger side); chain-side tests in Phase 5 |
| 5 | A reorg that removes an unconfirmed deposit never credits it | Phase 5 | Phase 5 |
| 6 | Replaying the command log from any snapshot yields the same events and book hash | Deterministic engine, canonical snapshots (ADR 0003) | `TestSnapshotReplayEqualsUninterruptedRun` (~60 cut points), `FuzzEngineAgainstModel` (midpoint snapshot), `TestCheckpointRestoreReplayEqualsUninterrupted`, `TestRestartMidStreamLosesNothingAndReplaysIdentically` (real Kafka, newest checkpoint discarded) |
| 7 | Order submission with the same `client_order_id` is idempotent | `order_keys` primary key `(user_id, client_order_id)` | `TestClientOrderIDIsIdempotent`, `TestDuplicateRetryResumesPendingHold`, e2e `TestTradeFlow` |
| 8 | Filled quantity never exceeds order quantity; fees round toward the exchange | Engine arithmetic; `orders.filled_lots <= qty_lots` CHECK; `money.Fee` rounds up | `TestFilledNeverExceedsQuantityAndConservation` (filled+cancelled+resting == qty for every order), `FuzzFeeNeverUnderCollects`, `TestTradeSettlementFeesAndHoldRelease` |

## Delivery guarantees

| Scenario | Test |
|---|---|
| Every Kafka message delivered twice | `make chaos-dup` (all producers publish each record twice; trade scenarios end with identical balances, ledger invariants hold, duplicates observed on `matching.events`) and `TestApplyEventsIsIdempotentUnderDuplicateDelivery` |
| Outbox relay dies between Kafka ack and commit | `TestRelayRepublishesAfterCrashBeforeCommit` |
| Matching killed mid-stream | `TestRestartMidStreamLosesNothingAndReplaysIdentically` |
| Duplicate `PlaceOrder` of an already finished order | `TestDuplicatePlaceOfFinishedOrderIsRejected`, `TestRejectedAndCancelledReleaseButDuplicateAndUnknownDoNot` |
| Poison event in a consumer | `TestPermanentErrorStallsOnlyThatPartition`, `TestUnsettleableEventIsPermanentAndRecorded` (the ledger stalls that partition, records `settlement_failures`, never skips) |
| Matching log truncated / checkpoints lost / partition count changed | `TestRefusesToStart*` (the service refuses to start) |
| Ledger history edited | `TestLedgerTablesAreAppendOnly` (UPDATE/DELETE/TRUNCATE on postings and journal entries fail) |
| Replay of a signed API request | `TestSignedOrderReplayCreatesOneOrder` (client_order_id required, one order) |
