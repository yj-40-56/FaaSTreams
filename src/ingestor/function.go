package ingestor

import (
	"context"

	"github.com/faastreams/ingestor/internal/ingestorcore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
)

var ingestor *ingestorcore.Ingestor

func init() {
	ingestor = ingestorcore.SetupFromEnv(context.Background())
	functions.CloudEvent("Handler", Handler)
}

// Handler is the Cloud Functions entry point for a Pub/Sub-triggered (Eventarc) deployment.
// The CloudEvent payload for a Pub/Sub message is the same {"message":{"data": ...}} envelope
// used by push-HTTP subscriptions, so it can go straight through HandleMessage unchanged.
func Handler(ctx context.Context, e event.Event) error {
	ingestor.HandleMessage(ctx, e.Data())
	return nil
}
