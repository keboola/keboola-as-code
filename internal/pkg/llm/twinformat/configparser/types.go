//nolint:tagliatelle // RFC specifies snake_case for JSON output in twin format.
package configparser

// TransformationConfig represents a transformation configuration fetched from API.
type TransformationConfig struct {
	ID           string           `json:"id"`
	Name         string           `json:"name"`
	ComponentID  string           `json:"component_id"`
	Description  string           `json:"description,omitempty"`
	IsDisabled   bool             `json:"is_disabled"`
	InputTables  []StorageMapping `json:"input_tables,omitempty"`
	OutputTables []StorageMapping `json:"output_tables,omitempty"`
	Blocks       []*CodeBlock     `json:"blocks,omitempty"`
	Version      int              `json:"version"`
	Created      string           `json:"created,omitempty"`
}

// StorageMapping represents an input or output table mapping.
type StorageMapping struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// CodeBlock represents a block of code in a transformation.
type CodeBlock struct {
	Name  string  `json:"name"`
	Codes []*Code `json:"codes,omitempty"`
}

// Code represents a single code script within a block.
type Code struct {
	Name   string `json:"name"`
	Script string `json:"script"`
}

// ComponentConfig represents a non-transformation component configuration.
type ComponentConfig struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	ComponentID   string         `json:"component_id"`
	ComponentType string         `json:"component_type"`
	Description   string         `json:"description,omitempty"`
	IsDisabled    bool           `json:"is_disabled"`
	Configuration map[string]any `json:"configuration,omitempty"`
	LastRun       string         `json:"last_run,omitempty"`
	Status        string         `json:"status,omitempty"`
	Version       int            `json:"version"`
	Created       string         `json:"created,omitempty"`
}
