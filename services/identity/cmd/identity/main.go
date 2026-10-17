// Command identity runs the identity service.
package main

import (
	"context"

	"github.com/tickless/identity/app"
	"github.com/tickless/pkg/svc"
)

func main() {
	svc.Main("identity", ":9203", func(ctx context.Context) error { return app.Run(ctx, app.ConfigFromEnv()) })
}
