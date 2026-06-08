package config

import (
	"embed"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

//go:embed test.yaml
var embeddedConfigs embed.FS

type SQLQuery struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"`
}

// QueryConfigs with several queries, gets defined by user/program
type QueryConfig struct {
	Name                string     `yaml:"name"`
	WindowType          string     `yaml:"window_type"`
	WindowSizeInSeconds int        `yaml:"window_size"`
	SQLQueries          []SQLQuery `yaml:"sql"`
}

// YAML with several QueryConfigs
type Config struct {
	Queries []QueryConfig `yaml:"queries"`
}

func LoadConfig(path string) Config {
	file, err := os.ReadFile(path)
	if err != nil {
		// Working directory at runtime varies across local, container, and Cloud Functions
		// deployments, so fall back to the config bundled into the binary via go:embed.
		file, err = embeddedConfigs.ReadFile(filepath.Base(path))
		if err != nil {
			log.Fatalf("[Config] Failed to read config file: %v", err)
		}
	}

	var config Config
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		log.Fatalf("[Config] Failed to parse config file: %v", err)
	}

	for i := 0; i < len(config.Queries); i++ {
		query := config.Queries[i]
		log.Printf("[Config] Loaded query config: %s with %d SQL queries\n", query.Name, len(query.SQLQueries))
	}

	return config
}
