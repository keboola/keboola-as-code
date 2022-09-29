package dbt

type SourceFile struct {
	Version int      `yaml:"version"`
	Sources []Source `yaml:"sources"`
}

type Source struct {
	Name          string          `yaml:"name"`
	Freshness     SourceFreshness `yaml:"freshness"`
	Database      string          `yaml:"database"`
	Schema        string          `yaml:"schema"`
	LoadedAtField string          `yaml:"loaded_at_field"` //nolint:tagliatelle
	Tables        []SourceTable   `yaml:"tables"`
}

type SourceTable struct {
	Name    string              `yaml:"name"`
	Quoting SourceTableQuoting  `yaml:"quoting"`
	Columns []SourceTableColumn `yaml:"columns"`
}

type SourceTableColumn struct {
	Name  string   `yaml:"name"`
	Tests []string `yaml:"tests"`
}

type SourceTableQuoting struct {
	Database   bool `yaml:"database"`
	Schema     bool `yaml:"schema"`
	Identifier bool `yaml:"identifier"`
}

type SourceFreshness struct {
	WarnAfter SourceFreshnessWarnAfter `yaml:"warn_after"` //nolint:tagliatelle
}

type SourceFreshnessWarnAfter struct {
	Count  int    `yaml:"count"`
	Period string `yaml:"period"`
}
