// This file is duplicated verbatim in src/ingestor/config
// (each is a separately deployed module, so it can't be a shared import). Keep both in sync.
package config

import (
	"context"
	"fmt"
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
	Slide       int    `yaml:"slide"`
	Query       string `yaml:"query"`
	ReturnType  string `yaml:"return_type"`
	IsAlert     bool   `yaml:"is_alert"`
	AlertFormat string `yaml:"alert_format"`
}

// SlideSeconds is how far a window's start advances between emissions.
// Unset means tumbling: the window advances by its own length.
func (q Query) SlideSeconds() int {
	if q.Slide == 0 {
		return q.WindowSize
	}
	return q.Slide
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

// Source describes one input stream: how to read its timestamps and what
// columns it carries. CSV playback settings are simulator-only and come from
// the SIM_CSV_PATH / SIM_CSV_DELIMITER / SIM_SCALE_FACTOR env vars instead.
type Source struct {
	Version         string                    `yaml:"version"          json:"version"`
	Description     string                    `yaml:"description"      json:"description"`
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

	for _, q := range config.Queries {
		if err := q.validate(); err != nil {
			log.Fatalf("[Config] Invalid query %q: %v", q.Name, err)
		}
	}

	for name := range config.Sources {
		log.Printf("[Config] Loaded source: %s", name)
	}
	for _, q := range config.Queries {
		log.Printf("[Config] Loaded query: %s (%s, %s %ds slide %ds)",
			q.Name, q.ReturnType, q.WindowType, q.WindowSize, q.SlideSeconds())
	}

	return config
}

// A slide wider than the window would leave gaps between windows, so the events
// falling in them would never reach a worker -- silent loss, hence fatal.
func (q Query) validate() error {
	if q.WindowSize <= 0 {
		return fmt.Errorf("window_size must be positive, got %d", q.WindowSize)
	}
	if q.Slide < 0 {
		return fmt.Errorf("slide must not be negative, got %d", q.Slide)
	}
	if q.SlideSeconds() > q.WindowSize {
		return fmt.Errorf("slide %ds exceeds window_size %ds, which would leave gaps between windows",
			q.SlideSeconds(), q.WindowSize)
	}
	if q.WindowType == "tumbling" && q.SlideSeconds() != q.WindowSize {
		return fmt.Errorf("tumbling windows cannot slide: drop slide, or set window_type: sliding")
	}
	// Defaulting to the window size would quietly make this tumbling, which is
	// not what the author asked for.
	if q.WindowType == "sliding" && q.Slide == 0 {
		return fmt.Errorf("sliding windows need an explicit slide; window_size %ds on its own means tumbling", q.WindowSize)
	}
	return nil
}
