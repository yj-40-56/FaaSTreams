package ingestor

import (
	"context"
	"net/http"

	"github.com/faastreams/ingestor/internal/ingestorcore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var ingestor *ingestorcore.Ingestor

func init() {
	ingestor = ingestorcore.SetupFromEnv(context.Background())
	functions.HTTP("Handler", Handler)
}

// Handler is the Cloud Functions entry point, receiving Pub/Sub push messages over HTTP.
func Handler(w http.ResponseWriter, r *http.Request) {
	ingestor.ServeHTTP(w, r)
}
