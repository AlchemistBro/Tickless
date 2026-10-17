// Package pg provides PostgreSQL connectivity (pgx v5) and goose migrations.
package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Connect opens a pool. QueryExecModeExec avoids server-side prepared
// statements so the pool also works behind pgbouncer in transaction mode.
func Connect(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// Migrate applies the embedded goose migrations found under dir in fsys.
// dsn must point at PostgreSQL directly (goose takes advisory locks, which do
// not work through a transaction-mode pooler).
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, mustSub(fsys, dir))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

func mustSub(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

var _ = stdlib.GetDefaultDriver // registers the "pgx" database/sql driver

// InTx runs f in a transaction, committing on nil and rolling back otherwise.
func InTx(ctx context.Context, pool *pgxpool.Pool, f func(tx pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// Postgres error codes used by services.
const (
	CodeUniqueViolation = "23505"
	CodeCheckViolation  = "23514"
)

// IsCode reports whether err is a PostgreSQL error with the given SQLSTATE.
func IsCode(err error, code string) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == code
}
