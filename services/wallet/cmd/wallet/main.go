// Command wallet runs the wallet service (deposit address derivation).
package main

import (
	"context"

	"github.com/tickless/pkg/svc"
	"github.com/tickless/wallet/app"
)

func main() {
	svc.Main("wallet", ":9209", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
