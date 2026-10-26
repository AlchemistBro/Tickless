// Package migrations embeds the withdrawals SQL migrations.
package migrations

import "embed"

// FS holds the goose migrations.
//
//go:embed *.sql
var FS embed.FS
