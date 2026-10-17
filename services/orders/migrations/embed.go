// Package migrations embeds the orders SQL migrations.
package migrations

import "embed"

// FS holds the goose migrations.
//
//go:embed *.sql
var FS embed.FS
