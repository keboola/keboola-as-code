package model

// ConfigYAML represents the unified _config.yml structure that combines
// metadata (from meta.json + description.md) and configuration (from config.json).
type ConfigYAML struct {
	Version int `yaml:"version" json:"version"`

	// Metadata fields (previously in meta.json + description.md)
	Name        string   `yaml:"name" json:"name"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	Disabled    bool     `yaml:"disabled,omitempty" json:"disabled,omitempty"`
	Tags        []string `yaml:"tags,omitempty" json:"tags,omitempty"`

	// Configuration fields (previously in config.json)
	Backend    *BackendYAML        `yaml:"backend,omitempty" json:"backend,omitempty"`
	Input      *StorageInputYAML   `yaml:"input,omitempty" json:"input,omitempty"`
	Output     *StorageOutputYAML  `yaml:"output,omitempty" json:"output,omitempty"`
	Parameters map[string]any      `yaml:"parameters,omitempty" json:"parameters,omitempty"`
	SharedCode []SharedCodeRefYAML `yaml:"shared_code,omitempty" json:"shared_code,omitempty"`

	// For applications
	UserProperties map[string]any `yaml:"user_properties,omitempty" json:"user_properties,omitempty"`

	// Internal Keboola metadata (managed by CLI)
	Keboola *KeboolaMetaYAML `yaml:"_keboola,omitempty" json:"_keboola,omitempty"`
}

// BackendYAML represents backend configuration.
type BackendYAML struct {
	Type    string `yaml:"type,omitempty" json:"type,omitempty"`
	Context string `yaml:"context,omitempty" json:"context,omitempty"`
}

// StorageInputYAML represents input storage mapping.
type StorageInputYAML struct {
	Tables []InputTableYAML `yaml:"tables,omitempty" json:"tables,omitempty"`
	Files  []InputFileYAML  `yaml:"files,omitempty" json:"files,omitempty"`
}

// StorageOutputYAML represents output storage mapping.
type StorageOutputYAML struct {
	Tables []OutputTableYAML `yaml:"tables,omitempty" json:"tables,omitempty"`
	Files  []OutputFileYAML  `yaml:"files,omitempty" json:"files,omitempty"`
}

// InputTableYAML represents an input table mapping.
type InputTableYAML struct {
	Source        string   `yaml:"source" json:"source"`
	Destination   string   `yaml:"destination" json:"destination"`
	Columns       []string `yaml:"columns,omitempty" json:"columns,omitempty"`
	WhereColumn   string   `yaml:"where_column,omitempty" json:"where_column,omitempty"`
	WhereOperator string   `yaml:"where_operator,omitempty" json:"where_operator,omitempty"`
	WhereValues   []string `yaml:"where_values,omitempty" json:"where_values,omitempty"`
	ChangedSince  string   `yaml:"changed_since,omitempty" json:"changed_since,omitempty"`
	Limit         int      `yaml:"limit,omitempty" json:"limit,omitempty"`
}

// InputFileYAML represents an input file mapping.
type InputFileYAML struct {
	Tags        []string `yaml:"tags,omitempty" json:"tags,omitempty"`
	Source      string   `yaml:"source,omitempty" json:"source,omitempty"`
	Destination string   `yaml:"destination,omitempty" json:"destination,omitempty"`
	Query       string   `yaml:"query,omitempty" json:"query,omitempty"`
}

// OutputTableYAML represents an output table mapping.
type OutputTableYAML struct {
	Source       string   `yaml:"source" json:"source"`
	Destination  string   `yaml:"destination" json:"destination"`
	PrimaryKey   []string `yaml:"primary_key,omitempty" json:"primary_key,omitempty"`
	Incremental  bool     `yaml:"incremental,omitempty" json:"incremental,omitempty"`
	DeleteWhere  string   `yaml:"delete_where,omitempty" json:"delete_where,omitempty"`
	Metadata     []any    `yaml:"metadata,omitempty" json:"metadata,omitempty"`
	ColumnMetadata map[string][]any `yaml:"column_metadata,omitempty" json:"column_metadata,omitempty"`
}

// OutputFileYAML represents an output file mapping.
type OutputFileYAML struct {
	Source string   `yaml:"source" json:"source"`
	Tags   []string `yaml:"tags,omitempty" json:"tags,omitempty"`
}

// SharedCodeRefYAML represents a shared code reference.
type SharedCodeRefYAML struct {
	Name  string   `yaml:"name" json:"name"`
	Codes []string `yaml:"codes,omitempty" json:"codes,omitempty"`
}

// MetaYAML represents the _meta.yml structure.
type MetaYAML struct {
	Version     int      `yaml:"version" json:"version"`
	Name        string   `yaml:"name" json:"name"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	Tags        []string `yaml:"tags,omitempty" json:"tags,omitempty"`
	IsDisabled  bool     `yaml:"is_disabled,omitempty" json:"is_disabled,omitempty"`
	Keboola     *KeboolaMetaYAML `yaml:"_keboola,omitempty" json:"_keboola,omitempty"`
}

// KeboolaMetaYAML contains Keboola-specific metadata.
type KeboolaMetaYAML struct {
	ComponentID string `yaml:"component_id,omitempty" json:"component_id,omitempty"`
	ConfigID    string `yaml:"config_id,omitempty" json:"config_id,omitempty"`
}

// VariablesYAML represents the _variables.yml structure.
type VariablesYAML struct {
	Version     int                      `yaml:"version" json:"version"`
	Definitions []VariableDefinitionYAML `yaml:"definitions,omitempty" json:"definitions,omitempty"`
	Values      map[string]VariableValuesYAML `yaml:"values,omitempty" json:"values,omitempty"`
}

// VariableDefinitionYAML represents a variable definition.
type VariableDefinitionYAML struct {
	Name        string `yaml:"name" json:"name"`
	Type        string `yaml:"type,omitempty" json:"type,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
}

// VariableValuesYAML represents a set of variable values.
type VariableValuesYAML struct {
	Name   string         `yaml:"name" json:"name"`
	Values map[string]any `yaml:"values" json:"values"`
}
