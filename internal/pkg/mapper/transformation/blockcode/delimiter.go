package blockcode

import "github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

// Delimiter defines comment markers and delimiters for a component type.
type Delimiter struct {
	Start  string   // Comment start marker (e.g., "/* " or "# ")
	End    string   // Comment end marker (e.g., " */" or "")
	Stmt   string   // Statement delimiter (e.g., ";" for SQL)
	Inline []string // Inline comment markers (e.g., "--", "//")
}

// componentDelimiters maps component IDs to their delimiter configurations.
// This matches the UI implementation in helpers.js getDelimiterAndCommentStrings().
var componentDelimiters = map[string]Delimiter{
	// SQL-based transformations
	"keboola.snowflake-transformation":        {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},
	"keboola.synapse-transformation":          {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},
	"keboola.oracle-transformation":           {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},
	"keboola.google-bigquery-transformation":  {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},
	"keboola.redshift-transformation":         {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},
	"keboola.exasol-transformation":           {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},
	"keboola.teradata-transformation":         {Start: "/* ", End: " */", Stmt: ";", Inline: []string{"--", "//"}},

	// Script-based transformations
	"keboola.python-transformation-v2":      {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},
	"keboola.csas-python-transformation-v2": {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},
	"keboola.r-transformation-v2":           {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},
	"keboola.julia-transformation":          {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},
	"keboola.python-spark-transformation":   {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},
	"keboola.python-snowpark-transformation": {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},
	"keboola.python-mlflow":                 {Start: "# ", End: "", Stmt: "", Inline: []string{"#"}},

	// DuckDB (SQL-like but no statement delimiter)
	"keboola.duckdb-transformation": {Start: "/* ", End: " */", Stmt: "", Inline: nil},
}

// GetDelimiter returns the delimiter configuration for a component.
// If the component is not found, returns the default (SQL-style comments without delimiter).
func GetDelimiter(componentID keboola.ComponentID) Delimiter {
	if d, ok := componentDelimiters[componentID.String()]; ok {
		return d
	}
	// Default: SQL-style comments without statement delimiter
	return Delimiter{Start: "/* ", End: " */", Stmt: "", Inline: nil}
}

// IsSQLComponent returns true if the component uses SQL-style block comments.
func IsSQLComponent(componentID keboola.ComponentID) bool {
	d := GetDelimiter(componentID)
	return d.Start == "/* "
}

// IsScriptComponent returns true if the component uses script-style line comments.
func IsScriptComponent(componentID keboola.ComponentID) bool {
	d := GetDelimiter(componentID)
	return d.Start == "# "
}
