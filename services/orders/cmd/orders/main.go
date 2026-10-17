// Command orders runs the orders service.
package main

import (
	"context"

	"github.com/tickless/orders/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("orders", ":9202", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
