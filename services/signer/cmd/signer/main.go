// Command signer runs the hot-wallet signer. `signer init` prepares the
// development PKI and keys and publishes the hot-wallet addresses.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/tickless/pkg/svc"
	"github.com/tickless/signer/app"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		if err := app.Init(app.ConfigFromEnv()); err != nil {
			slog.Error("signer init failed", "err", err)
			os.Exit(1)
		}
		return
	}
	svc.Main("signer", ":9210", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
