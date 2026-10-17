// Command orders runs the orders service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/tickless/orders/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	ctx, stop := svc.SignalContext()
	defer stop()
	if err := app.Run(ctx, app.ConfigFromEnv()); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("orders stopped", "err", err)
		os.Exit(1)
	}
}
