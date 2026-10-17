// Package migrations embeds the identity SQL migrations.
package migrations

import "embed"

// FS holds the goose migrations.
//
//go:embed *.sql
var FS embed.FS
