// Command marketdata runs the public market-data service.
package main

import (
	"context"

	"github.com/tickless/marketdata/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("marketdata", ":9207", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
