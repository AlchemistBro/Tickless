#!/bin/bash
# Creates all Tickless topics (documented in docs/events.md). Idempotent.
# Matching topics are partitioned by market, the rest by user id.
#
# matching.commands / matching.events use retention.ms=-1 (keep forever): the
# command log is the source of truth the matching engine replays on recovery and
# the event log is the audit trail. With finite retention a lost checkpoint
# could not be rebuilt (ADR 0003); the service refuses to start in that case.
set -euo pipefail
BOOT=kafka:29092
KT=/opt/kafka/bin/kafka-topics.sh
create() { # name partitions [extra --config args...]
  local name=$1 parts=$2; shift 2
  $KT --bootstrap-server "$BOOT" --create --if-not-exists --topic "$name" --partitions "$parts" --replication-factor 1 "$@"
}
create matching.commands  6 --config retention.ms=-1
create matching.events    6 --config retention.ms=-1
create ledger.events      6
create deposits.events    6
create withdrawals.events 6
create users.events       6
create candles.live       6
$KT --bootstrap-server "$BOOT" --list
