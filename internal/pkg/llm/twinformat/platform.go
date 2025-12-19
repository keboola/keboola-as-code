package twinformat

import (
	"strings"
)

// Platform constants for transformation platforms.
const (
	PlatformSnowflake  = "snowflake"
	PlatformRedshift   = "redshift"
	PlatformBigQuery   = "bigquery"
	PlatformSynapse    = "synapse"
	PlatformDuckDB     = "duckdb"
	PlatformPython     = "python"
	PlatformR          = "r"
	PlatformDBT        = "dbt"
	PlatformOracle     = "oracle"
	PlatformMySQL      = "mysql"
	PlatformPostgreSQL = "postgresql"
	PlatformSQL        = "sql"
	PlatformUnknown    = "unknown"
)

// platformMapping represents a mapping from component ID pattern to platform.
type platformMapping struct {
	pattern  string
	platform string
}

// getPlatformMappings returns the platform mappings.
// Order matters - more specific patterns should come first.
func getPlatformMappings() []platformMapping {
	return []platformMapping{
		// Snowflake
		{"keboola.snowflake-transformation", PlatformSnowflake},
		{"keboola.snowflake", PlatformSnowflake},
		{"snowflake", PlatformSnowflake},

		// Redshift
		{"keboola.redshift-transformation", PlatformRedshift},
		{"keboola.redshift", PlatformRedshift},
		{"redshift", PlatformRedshift},

		// BigQuery
		{"keboola.bigquery-transformation", PlatformBigQuery},
		{"keboola.bigquery", PlatformBigQuery},
		{"bigquery", PlatformBigQuery},

		// Synapse
		{"keboola.synapse-transformation", PlatformSynapse},
		{"keboola.synapse", PlatformSynapse},
		{"synapse", PlatformSynapse},

		// DuckDB
		{"keboola.duckdb-transformation", PlatformDuckDB},
		{"keboola.duckdb", PlatformDuckDB},
		{"duckdb", PlatformDuckDB},

		// Python
		{"keboola.python-transformation-v2", PlatformPython},
		{"keboola.python-transformation", PlatformPython},
		{"keboola.python", PlatformPython},
		{"python", PlatformPython},

		// R
		{"keboola.r-transformation", PlatformR},
		{"keboola.r", PlatformR},

		// dbt
		{"keboola.dbt-transformation", PlatformDBT},
		{"keboola.dbt", PlatformDBT},
		{"dbt", PlatformDBT},

		// Oracle
		{"keboola.oracle-transformation", PlatformOracle},
		{"keboola.oracle", PlatformOracle},
		{"oracle", PlatformOracle},

		// MySQL
		{"keboola.mysql-transformation", PlatformMySQL},
		{"keboola.mysql", PlatformMySQL},
		{"mysql", PlatformMySQL},

		// PostgreSQL
		{"keboola.postgresql-transformation", PlatformPostgreSQL},
		{"keboola.postgresql", PlatformPostgreSQL},
		{"keboola.postgres-transformation", PlatformPostgreSQL},
		{"keboola.postgres", PlatformPostgreSQL},
		{"postgresql", PlatformPostgreSQL},
		{"postgres", PlatformPostgreSQL},

		// Generic SQL (fallback for SQL-like transformations)
		{"keboola.sql-transformation", PlatformSQL},
		{"-transformation", PlatformSQL}, // Catch-all for other transformations
	}
}

// DetectPlatform detects the platform from a component ID.
// Returns the platform name (e.g., "snowflake", "python", "dbt").
// Target: 0 unknown platforms.
func DetectPlatform(componentID string) string {
	if componentID == "" {
		return PlatformUnknown
	}

	componentIDLower := strings.ToLower(componentID)

	for _, mapping := range getPlatformMappings() {
		if strings.Contains(componentIDLower, mapping.pattern) {
			return mapping.platform
		}
	}

	return PlatformUnknown
}

// IsSQLPlatform returns true if the platform is SQL-based.
func IsSQLPlatform(platform string) bool {
	switch platform {
	case PlatformSnowflake, PlatformRedshift, PlatformBigQuery, PlatformSynapse,
		PlatformDuckDB, PlatformOracle, PlatformMySQL, PlatformPostgreSQL, PlatformSQL:
		return true
	default:
		return false
	}
}

// IsTransformationComponent returns true if the component ID represents a transformation.
func IsTransformationComponent(componentID string) bool {
	componentIDLower := strings.ToLower(componentID)
	return strings.Contains(componentIDLower, "transformation")
}

// GetPlatformDisplayName returns a human-readable display name for a platform.
func GetPlatformDisplayName(platform string) string {
	switch platform {
	case PlatformSnowflake:
		return "Snowflake"
	case PlatformRedshift:
		return "Redshift"
	case PlatformBigQuery:
		return "BigQuery"
	case PlatformSynapse:
		return "Synapse"
	case PlatformDuckDB:
		return "DuckDB"
	case PlatformPython:
		return "Python"
	case PlatformR:
		return "R"
	case PlatformDBT:
		return "dbt"
	case PlatformOracle:
		return "Oracle"
	case PlatformMySQL:
		return "MySQL"
	case PlatformPostgreSQL:
		return "PostgreSQL"
	case PlatformSQL:
		return "SQL"
	default:
		return "Unknown"
	}
}
