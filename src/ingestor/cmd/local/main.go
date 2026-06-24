package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/faastreams/ingestor/internal/ingestorcore"
)

func main() {
	ctx := context.Background()

	ingestor := ingestorcore.SetupFromEnv(ctx)

	mode := os.Getenv("RUN_MODE")
	if mode == "http" {
		_, _, _ = ingestorcore.SetupPubSub(ctx)
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		log.Println("[Main] Starting HTTP server...")
		http.Handle("/", ingestor)
		http.ListenAndServe(":"+port, nil)
	} else {
		_, _, subscription := ingestorcore.SetupPubSub(ctx)
		log.Println("[Main] Starting subscription receiver...")
		ingestor.Run(ctx, subscription)
	}
}
