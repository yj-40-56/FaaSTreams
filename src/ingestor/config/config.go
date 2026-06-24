package config

import (
	"embed"
	"log"

	"gopkg.in/yaml.v3"
)

//go:embed config.yaml
var embeddedConfig embed.FS

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
	Sources map[string]Source `yaml:"sources"`
}

// LoadConfig reads the source configuration bundled into the binary via go:embed.
func LoadConfig() Config {
	file, err := embeddedConfig.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("[Config] Failed to read config file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(file, &config); err != nil {
		log.Fatalf("[Config] Failed to parse config file: %v", err)
	}

	for name := range config.Sources {
		log.Printf("[Config] Loaded source: %s", name)
	}

	return config
}
