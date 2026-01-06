package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectPlatform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		componentID string
		expected    string
	}{
		// Snowflake
		{name: "snowflake transformation", componentID: "keboola.snowflake-transformation", expected: PlatformSnowflake},
		{name: "snowflake generic", componentID: "keboola.snowflake", expected: PlatformSnowflake},
		{name: "snowflake shorthand", componentID: "snowflake", expected: PlatformSnowflake},

		// Redshift
		{name: "redshift transformation", componentID: "keboola.redshift-transformation", expected: PlatformRedshift},
		{name: "redshift generic", componentID: "keboola.redshift", expected: PlatformRedshift},

		// BigQuery
		{name: "bigquery transformation", componentID: "keboola.bigquery-transformation", expected: PlatformBigQuery},
		{name: "bigquery generic", componentID: "keboola.bigquery", expected: PlatformBigQuery},

		// Synapse
		{name: "synapse transformation", componentID: "keboola.synapse-transformation", expected: PlatformSynapse},

		// DuckDB
		{name: "duckdb transformation", componentID: "keboola.duckdb-transformation", expected: PlatformDuckDB},

		// Python
		{name: "python transformation v2", componentID: "keboola.python-transformation-v2", expected: PlatformPython},
		{name: "python transformation", componentID: "keboola.python-transformation", expected: PlatformPython},
		{name: "python generic", componentID: "keboola.python", expected: PlatformPython},

		// R
		{name: "r transformation", componentID: "keboola.r-transformation", expected: PlatformR},
		{name: "r generic", componentID: "keboola.r", expected: PlatformR},

		// dbt
		{name: "dbt transformation", componentID: "keboola.dbt-transformation", expected: PlatformDBT},
		{name: "dbt generic", componentID: "keboola.dbt", expected: PlatformDBT},

		// Oracle
		{name: "oracle transformation", componentID: "keboola.oracle-transformation", expected: PlatformOracle},

		// MySQL
		{name: "mysql transformation", componentID: "keboola.mysql-transformation", expected: PlatformMySQL},

		// PostgreSQL
		{name: "postgresql transformation", componentID: "keboola.postgresql-transformation", expected: PlatformPostgreSQL},
		{name: "postgres transformation", componentID: "keboola.postgres-transformation", expected: PlatformPostgreSQL},

		// Case insensitivity
		{name: "uppercase snowflake", componentID: "KEBOOLA.SNOWFLAKE-TRANSFORMATION", expected: PlatformSnowflake},

		// Unknown
		{name: "empty component", componentID: "", expected: PlatformUnknown},
		{name: "unknown component", componentID: "keboola.extractor-generic", expected: PlatformUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := DetectPlatform(tc.componentID)
			assert.Equal(t, tc.expected, result, "DetectPlatform(%q)", tc.componentID)
		})
	}
}

func TestIsSQLPlatform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		platform string
		expected bool
	}{
		{PlatformSnowflake, true},
		{PlatformRedshift, true},
		{PlatformBigQuery, true},
		{PlatformSynapse, true},
		{PlatformDuckDB, true},
		{PlatformOracle, true},
		{PlatformMySQL, true},
		{PlatformPostgreSQL, true},
		{PlatformSQL, true},
		{PlatformPython, false},
		{PlatformR, false},
		{PlatformDBT, false},
		{PlatformUnknown, false},
	}

	for _, tc := range tests {
		t.Run(tc.platform, func(t *testing.T) {
			t.Parallel()
			result := IsSQLPlatform(tc.platform)
			assert.Equal(t, tc.expected, result, "IsSQLPlatform(%q)", tc.platform)
		})
	}
}

func TestIsTransformationComponent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		componentID string
		expected    bool
	}{
		{"keboola.snowflake-transformation", true},
		{"keboola.python-transformation-v2", true},
		{"keboola.transformation-generic", true},
		{"keboola.extractor-generic", false},
		{"keboola.writer-generic", false},
		{"", false},
	}

	for _, tc := range tests {
		t.Run(tc.componentID, func(t *testing.T) {
			t.Parallel()
			result := IsTransformationComponent(tc.componentID)
			assert.Equal(t, tc.expected, result, "IsTransformationComponent(%q)", tc.componentID)
		})
	}
}

func TestGetPlatformDisplayName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		platform string
		expected string
	}{
		{PlatformSnowflake, "Snowflake"},
		{PlatformPython, "Python"},
		{PlatformDBT, "dbt"},
		{PlatformUnknown, "Unknown"},
		{"invalid", "Unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.platform, func(t *testing.T) {
			t.Parallel()
			result := GetPlatformDisplayName(tc.platform)
			assert.Equal(t, tc.expected, result)
		})
	}
}
