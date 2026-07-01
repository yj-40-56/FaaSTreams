package config

import (
	"context"
	"io"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"gopkg.in/yaml.v3"
)

// Query A config has several Queries, each with a name, SQL query and return type (aggregate, spatial, etc.)
// This flat structure allows for more flexible query definitions, e.g. we can easily add more window types or other parameters later without changing the config structure
type Query struct {
	Name        string `yaml:"name"`
	DataSource  string `yaml:"data_source"`
	WindowType  string `yaml:"window_type"`
	WindowSize  int    `yaml:"window_size"`
	Query       string `yaml:"query"`
	ReturnType  string `yaml:"return_type"`
	IsAlert     bool   `yaml:"is_alert"`
	AlertFormat string `yaml:"alert_format"`
}

type ColumnDef struct {
	FromField string `yaml:"from_field" json:"from_field"`
	Type      string `yaml:"type"       json:"type"`
	Required  bool   `yaml:"required"   json:"required"`
}

type ReferenceTable struct {
	Columns map[string]string        `yaml:"columns" json:"columns"`
	Rows    []map[string]interface{} `yaml:"rows"    json:"rows"`
}

// Source scaleFactor compresses CSV event timestamps so data plays back faster than it was recorded.
// Formula: scaleFactor = CSV duration / desired real duration
//
// | CSV data | Real time | scaleFactor |
// |----------|-----------|-------------|
// | 24h      | 24h       | 1.0         |
// | 24h      | 1h        | 24.0        |
// | 24h      | 30min     | 48.0        |
// | 24h      | 10min     | 144.0       |
// | 1h       | 1min      | 60.0        |
type Source struct {
	Version         string                    `yaml:"version"          json:"version"`
	Description     string                    `yaml:"description"      json:"description"`
	CsvPath         string                    `yaml:"csv_path"         json:"csv_path"`
	CsvDelimiter    string                    `yaml:"csv_delimiter"    json:"csv_delimiter"`
	ScaleFactor     float64                   `yaml:"scale_factor"     json:"scale_factor"`
	IDField         string                    `yaml:"id_field"         json:"id_field"`
	TimestampField  string                    `yaml:"timestamp_field"  json:"timestamp_field"`
	TimestampFormat string                    `yaml:"timestamp_format" json:"timestamp_format"`
	LatField        string                    `yaml:"lat_field"        json:"lat_field"`
	LonField        string                    `yaml:"lon_field"        json:"lon_field"`
	Columns         map[string]ColumnDef      `yaml:"columns"          json:"columns"`
	ReferenceTables map[string]ReferenceTable `yaml:"reference_tables" json:"reference_tables"`
}

type Config struct {
	Queries []Query           `yaml:"queries"`
	Sources map[string]Source `yaml:"sources"`
}

// LoadConfig reads the query/window configuration from a GCS bucket.
func LoadConfig() Config {
	bucket := os.Getenv("CONFIG_BUCKET")
	object := os.Getenv("CONFIG_OBJECT")
	if bucket == "" || object == "" {
		log.Fatal("[Config] CONFIG_BUCKET and CONFIG_OBJECT env vars required")
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("[Config] Failed to create GCS client: %v", err)
	}
	defer client.Close()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		log.Fatalf("[Config] Failed to read config from GCS: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		log.Fatalf("[Config] Failed to read config file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("[Config] Failed to parse config file: %v", err)
	}

	for name := range config.Sources {
		log.Printf("[Config] Loaded source: %s", name)
	}
	for _, q := range config.Queries {
		log.Printf("[Config] Loaded query: %s (%s)", q.Name, q.ReturnType)
	}

	return config
}
