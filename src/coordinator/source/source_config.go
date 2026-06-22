package source

import (
	"os"

	"gopkg.in/yaml.v3"
)

type SourceConfig struct {
	CsvPath         string  `yaml:"csv_path"`
	CsvDelimiter    string  `yaml:"csv_delimiter"`
	TimestampField  string  `yaml:"timestamp_field"`
	TimestampFormat string  `yaml:"timestamp_format"`
	ScaleFactor     float64 `yaml:"scale_factor"`
	IDField         string  `yaml:"id_field"`
	LatField        string  `yaml:"lat_field"`
	LonField        string  `yaml:"lon_field"`
}

type Config struct {
	Sources map[string]SourceConfig `yaml:"sources"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	return &cfg, yaml.Unmarshal(data, &cfg)
}
