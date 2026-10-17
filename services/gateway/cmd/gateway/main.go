// Command gateway runs the gateway service.
package main

import (
	"context"

	"github.com/tickless/gateway/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("gateway", ":9205", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
