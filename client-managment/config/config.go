package config

import (
	"encoding/json"
	"os"
)

// LoadConfigFromJSON reads key/value JSON and sets them into env if not present.
func LoadConfigFromJSON(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var data map[string]string
	if err := json.NewDecoder(f).Decode(&data); err != nil {
		return err
	}

	for k, v := range data {
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	return nil
}
