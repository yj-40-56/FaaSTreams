package windower

import (
	"context"
	"net/http"

	"github.com/faastreams/windower/internal/windowercore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var manager *windowercore.Manager

func init() {
	manager = windowercore.SetupFromEnv(context.Background())
	functions.HTTP("Handler", Handler)
}

// Handler is the Cloud Functions entry point, invoked periodically by Cloud Scheduler.
func Handler(w http.ResponseWriter, r *http.Request) {
	manager.ServeHTTP(w, r)
}
