package coordinatorfn

import (
	"context"
	"net/http"

	"github.com/faastreams/coordinator/internal/coordinatorcore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var coordinatorInstance *coordinatorcore.Coordinator

func init() {
	coordinatorInstance = coordinatorcore.SetupFromEnv(context.Background())
	functions.HTTP("Handler", Handler)
}

// Handler is the Cloud Functions entry point, receiving Pub/Sub push messages over HTTP.
func Handler(w http.ResponseWriter, r *http.Request) {
	coordinatorInstance.ServeHTTP(w, r)
}
