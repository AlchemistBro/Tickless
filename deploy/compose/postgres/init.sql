-- One database per service: no cross-service joins, no shared tables (SPEC §4).
-- Local development credentials only.
CREATE DATABASE identity;
CREATE DATABASE orders;
CREATE DATABASE ledger;
CREATE DATABASE chainwatcher;
CREATE DATABASE wallet;
CREATE DATABASE withdrawals;
CREATE DATABASE reconciler;
