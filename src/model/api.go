package model

import (
	"fmt"
	"keboola-as-code/src/json"
	"strconv"
)

func (r *ConfigRow) ToApiValues() (map[string]string, error) {
	// Encode config
	configJson, err := json.Encode(r.Config, false)
	if err != nil {
		return nil, fmt.Errorf(`cannot JSON encode config configuration: %s`, err)
	}

	return map[string]string{
		"name":              r.Name,
		"description":       r.Description,
		"changeDescription": r.ChangeDescription,
		"isDisabled":        strconv.FormatBool(r.IsDisabled),
		"configuration":     string(configJson),
	}, nil
}

func (c *Config) ToApiValues() (map[string]string, error) {
	// Encode config
	configJson, err := json.Encode(c.Config, false)
	if err != nil {
		return nil, fmt.Errorf(`cannot JSON encode config configuration: %s`, err)
	}

	return map[string]string{
		"name":              c.Name,
		"description":       c.Description,
		"changeDescription": c.ChangeDescription,
		"configuration":     string(configJson),
	}, nil
}
