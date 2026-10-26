// Command chainwatcher runs the chainwatcher service.
package main

import (
	"context"

	"github.com/tickless/chainwatcher/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("chainwatcher", ":9208", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
