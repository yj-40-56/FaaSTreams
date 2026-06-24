package windower

import (
	"context"
	"net/http"

	"github.com/faastreams/windower/internal/windowercore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var windowers []*windowercore.Windower

func init() {
	windowers = windowercore.SetupFromEnv(context.Background())
	functions.HTTP("Handler", Handler)
}

// Handler is the Cloud Functions entry point, invoked periodically by Cloud Scheduler.
func Handler(w http.ResponseWriter, r *http.Request) {
	for _, windower := range windowers {
		windower.Tick(r.Context())
	}
	w.WriteHeader(http.StatusOK)
}
