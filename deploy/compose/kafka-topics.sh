#!/bin/bash
# Creates all Tickless topics. Matching topics are partitioned by market, the
# rest by user id (see docs/events.md). Idempotent via --if-not-exists.
set -euo pipefail
BOOT=kafka:29092
KT=/opt/kafka/bin/kafka-topics.sh
create() { $KT --bootstrap-server "$BOOT" --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1; }
create matching.commands 6
create matching.events   6
create ledger.events     6
create deposits.events   6
create withdrawals.events 6
create users.events      6
create candles.live      6
$KT --bootstrap-server "$BOOT" --list
