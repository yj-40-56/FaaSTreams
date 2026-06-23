package coordinator

import (
	"context"
	"io"
	"net/http"

	"github.com/faastreams/coordinator/internal/coordinatorcore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var coordinators []*coordinatorcore.Coordinator

func init() {
	coordinators = coordinatorcore.SetupFromEnv(context.Background())
	functions.HTTP("Handler", Handler)
}

// Handler is the Cloud Functions entry point, receiving Pub/Sub push messages over HTTP.
func Handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	for _, c := range coordinators {
		c.HandleMessage(r.Context(), body)
	}
	w.WriteHeader(http.StatusOK)
}
