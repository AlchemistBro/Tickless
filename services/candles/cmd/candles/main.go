// Command candles runs the candles service.
package main

import (
	"context"

	"github.com/tickless/candles/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("candles", ":9206", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
