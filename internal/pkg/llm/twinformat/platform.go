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

// DetectPlatform extracts the platform from a component ID.
// Component IDs follow the pattern: "keboola.{platform}-transformation[-v2]"
// Examples:
//   - "keboola.snowflake-transformation" -> "snowflake"
//   - "keboola.python-transformation-v2" -> "python"
func DetectPlatform(componentID string) string {
	if componentID == "" {
		return PlatformUnknown
	}

	componentIDLower := strings.ToLower(componentID)

	// Extract platform from "keboola.{platform}-transformation" pattern
	if strings.HasPrefix(componentIDLower, "keboola.") && strings.Contains(componentIDLower, "-transformation") {
		// Remove "keboola." prefix
		withoutPrefix := strings.TrimPrefix(componentIDLower, "keboola.")
		// Extract platform (everything before "-transformation")
		if idx := strings.Index(withoutPrefix, "-transformation"); idx > 0 {
			platform := withoutPrefix[:idx]
			// Normalize postgres -> postgresql
			if platform == "postgres" {
				return PlatformPostgreSQL
			}
			return platform
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
