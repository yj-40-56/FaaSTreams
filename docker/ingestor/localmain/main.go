// Local-only entry point. In production Cloud Functions supplies this wrapper
// itself and selects the entry point via build_config.entry_point, so this file
// deliberately lives outside src/ingestor: the Terraform archive_file zips that
// directory verbatim, and a second main package inside it would reach the
// buildpack. The Dockerfile copies this in at build time instead.
package main

import (
	"log"
	"os"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"

	// Imported for its init(), which registers IngestPull/IngestEvent with the
	// functions framework exactly as the deployed function does.
	_ "github.com/mardentub/ingestor"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	if err := funcframework.Start(port); err != nil {
		log.Fatalf("funcframework.Start: %v", err)
	}
}
