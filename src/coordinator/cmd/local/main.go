package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/faastreams/coordinator/internal/coordinatorcore"
)

func main() {
	ctx := context.Background()

	coordinators := coordinatorcore.SetupFromEnv(ctx)

	_, _, subscription := coordinatorcore.SetupPubSub(ctx)

	mode := os.Getenv("RUN_MODE")
	if mode == "http" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		log.Println("[Main] Starting HTTP server...")
		for _, coordinator := range coordinators {
			http.Handle("/", coordinator)
		}
		http.ListenAndServe(":"+port, nil)
	} else {
		log.Println("[Main] Starting subscription receiver...")
		for _, coordinator := range coordinators {
			coordinator.Run(ctx, subscription)
		}
	}
}
