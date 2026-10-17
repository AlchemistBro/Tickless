// Command matching runs the matching service.
package main

import (
	"context"

	"github.com/tickless/matching/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("matching", ":9204", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
