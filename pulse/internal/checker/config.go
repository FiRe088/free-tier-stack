package checker

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Target is one HTTP endpoint Pulse monitors.
type Target struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// Config is the top-level shape of targets.yml.
type Config struct {
	Targets []Target `yaml:"targets"`
}

// LoadConfig reads and parses a targets.yml file at path.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("checker: reading config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("checker: parsing config %s: %w", path, err)
	}

	if len(cfg.Targets) == 0 {
		return Config{}, fmt.Errorf("checker: config %s has no targets", path)
	}

	for i, t := range cfg.Targets {
		if t.Name == "" {
			return Config{}, fmt.Errorf("checker: target at index %d has empty name", i)
		}
		if t.URL == "" {
			return Config{}, fmt.Errorf("checker: target %q has empty url", t.Name)
		}
	}

	return cfg, nil
}
