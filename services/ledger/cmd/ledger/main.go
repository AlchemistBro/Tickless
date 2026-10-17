// Command ledger runs the ledger service.
package main

import (
	"context"

	"github.com/tickless/ledger/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("ledger", ":9201", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
