// Package migrations contains the SQL migrations shared by the CLI and tests.
package migrations

import "embed"

// Files embeds migrations so they can be used from any working directory.
//
//go:embed *.sql
var Files embed.FS
