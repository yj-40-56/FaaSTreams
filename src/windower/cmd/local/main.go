package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/faastreams/windower/internal/windowercore"
)

func main() {
	ctx := context.Background()

	manager := windowercore.SetupFromEnv(ctx)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("[Main] Starting HTTP server...")
	http.Handle("/", manager)
	http.ListenAndServe(":"+port, nil)
}
