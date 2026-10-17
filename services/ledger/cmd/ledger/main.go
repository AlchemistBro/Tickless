// Command ledger runs the double-entry ledger service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/tickless/ledger/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	ctx, stop := svc.SignalContext()
	defer stop()
	if err := app.Run(ctx, app.ConfigFromEnv()); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("ledger stopped", "err", err)
		os.Exit(1)
	}
}
