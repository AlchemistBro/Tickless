// Command identity runs the identity service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/tickless/identity/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	ctx, stop := svc.SignalContext()
	defer stop()
	if err := app.Run(ctx, app.ConfigFromEnv()); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("identity stopped", "err", err)
		os.Exit(1)
	}
}
