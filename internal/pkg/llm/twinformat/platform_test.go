package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectPlatform(t *testing.T) {
	t.Parallel()

	// Only real Keboola transformation component IDs are supported.
	// Format: "keboola.{platform}-transformation[-v2]"
	tests := []struct {
		name        string
		componentID string
		expected    string
	}{
		// SQL platforms
		{name: "snowflake transformation", componentID: "keboola.snowflake-transformation", expected: PlatformSnowflake},
		{name: "redshift transformation", componentID: "keboola.redshift-transformation", expected: PlatformRedshift},
		{name: "bigquery transformation", componentID: "keboola.bigquery-transformation", expected: PlatformBigQuery},
		{name: "synapse transformation", componentID: "keboola.synapse-transformation", expected: PlatformSynapse},
		{name: "duckdb transformation", componentID: "keboola.duckdb-transformation", expected: PlatformDuckDB},
		{name: "oracle transformation", componentID: "keboola.oracle-transformation", expected: PlatformOracle},
		{name: "mysql transformation", componentID: "keboola.mysql-transformation", expected: PlatformMySQL},
		{name: "postgresql transformation", componentID: "keboola.postgresql-transformation", expected: PlatformPostgreSQL},
		{name: "postgres transformation", componentID: "keboola.postgres-transformation", expected: PlatformPostgreSQL},

		// Non-SQL platforms
		{name: "python transformation v2", componentID: "keboola.python-transformation-v2", expected: PlatformPython},
		{name: "python transformation", componentID: "keboola.python-transformation", expected: PlatformPython},
		{name: "r transformation", componentID: "keboola.r-transformation", expected: PlatformR},
		{name: "dbt transformation", componentID: "keboola.dbt-transformation", expected: PlatformDBT},

		// Case insensitivity
		{name: "uppercase snowflake", componentID: "KEBOOLA.SNOWFLAKE-TRANSFORMATION", expected: PlatformSnowflake},

		// Unknown (non-transformation components)
		{name: "empty component", componentID: "", expected: PlatformUnknown},
		{name: "extractor component", componentID: "keboola.ex-db-mysql", expected: PlatformUnknown},
		{name: "writer component", componentID: "keboola.wr-db-snowflake", expected: PlatformUnknown},
		{name: "application component", componentID: "kds-team.app-custom-python", expected: PlatformUnknown},
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

func TestPlatformDisplayName(t *testing.T) {
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
			result := PlatformDisplayName(tc.platform)
			assert.Equal(t, tc.expected, result)
		})
	}
}
