// Package migrations embeds the ledger SQL migrations.
package migrations

import "embed"

// FS holds the goose migrations.
//
//go:embed *.sql
var FS embed.FS
