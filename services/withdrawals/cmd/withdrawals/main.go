// Command withdrawals runs the withdrawals service.
package main

import (
	"context"

	"github.com/tickless/pkg/svc"
	"github.com/tickless/withdrawals/app"
)

func main() {
	svc.Main("withdrawals", ":9211", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
