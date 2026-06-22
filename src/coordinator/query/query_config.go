package query

import (
	"embed"
	"log"

	"gopkg.in/yaml.v3"
)

//go:embed query_config.yaml
var embeddedConfig embed.FS

// A config has several Queries, each with a name, SQL query and return type (aggregate, spatial, etc.)
// This flat structure allows for more flexible query definitions, e.g. we can easily add more window types or other parameters later without changing the config structure
type Query struct {
	Name       string `yaml:"name"`
	WindowType string `yaml:"window_type"`
	WindowSize int    `yaml:"window_size"`
	Query      string `yaml:"query"`
	ReturnType string `yaml:"return_type"`
}

type Config struct {
	Queries []Query `yaml:"queries"`
}

// LoadConfig reads the query/window configuration bundled into the binary via go:embed.
func LoadConfig() Config {
	file, err := embeddedConfig.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("[Config] Failed to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		log.Fatalf("[Config] Failed to parse config file: %v", err)
	}

	for i := 0; i < len(config.Queries); i++ {
		query := config.Queries[i]
		log.Printf("[Config] Loaded query config: %s with %s SQL queries\n", query.Name, query.ReturnType)
	}

	return config
}
