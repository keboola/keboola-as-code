package twinformat

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// mockGeneratorDeps implements GeneratorDependencies for testing.
type mockGeneratorDeps struct {
	fs     filesystem.Fs
	logger log.Logger
}

func (m *mockGeneratorDeps) Fs() filesystem.Fs  { return m.fs }
func (m *mockGeneratorDeps) Logger() log.Logger { return m.logger }

func newTestGenerator(t *testing.T) (*Generator, filesystem.Fs) {
	t.Helper()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(log.NewNopLogger()))
	deps := &mockGeneratorDeps{
		fs:     fs,
		logger: log.NewNopLogger(),
	}
	return NewGenerator(deps, "/output"), fs
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestLanguageToExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		language string
		expected string
	}{
		{name: "python", language: PlatformPython, expected: ".py"},
		{name: "r", language: PlatformR, expected: ".r"},
		{name: "sql", language: "sql", expected: ".sql"},
		{name: "snowflake", language: PlatformSnowflake, expected: ".sql"},
		{name: "bigquery", language: PlatformBigQuery, expected: ".sql"},
		{name: "unknown", language: "unknown", expected: ".sql"},
		{name: "empty", language: "", expected: ".sql"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := languageToExtension(tc.language)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFormatSourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		expected string
	}{
		{name: "shopify", source: "shopify", expected: "Shopify"},
		{name: "hubspot", source: "hubspot", expected: "HubSpot"},
		{name: "salesforce", source: "salesforce", expected: "Salesforce"},
		{name: "google", source: "google", expected: "Google"},
		{name: "facebook", source: "facebook", expected: "Facebook"},
		{name: "snowflake", source: "snowflake", expected: "Snowflake"},
		{name: "bigquery", source: "bigquery", expected: "BigQuery"},
		{name: "postgres", source: "postgres", expected: "PostgreSQL"},
		{name: "transformation", source: "transformation", expected: "Transformation Output"},
		{name: "unknown", source: "unknown", expected: "Unknown Source"},
		{name: "custom source", source: "my-custom-source", expected: "my-custom-source"},
		{name: "empty", source: "", expected: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := formatSourceName(tc.source)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestInferSourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		expected string
	}{
		{name: "shopify extractor", source: "shopify", expected: "extractor"},
		{name: "hubspot extractor", source: "hubspot", expected: "extractor"},
		{name: "salesforce extractor", source: "salesforce", expected: "extractor"},
		{name: "google extractor", source: "google", expected: "extractor"},
		{name: "stripe extractor", source: "stripe", expected: "extractor"},
		{name: "snowflake database", source: "snowflake", expected: "database"},
		{name: "bigquery database", source: "bigquery", expected: "database"},
		{name: "postgres database", source: "postgres", expected: "database"},
		{name: "mysql database", source: "mysql", expected: "database"},
		{name: "mongodb database", source: "mongodb", expected: "database"},
		{name: "transformation internal", source: "transformation", expected: "internal"},
		{name: "unknown source", source: "custom", expected: "unknown"},
		{name: "empty source", source: "", expected: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := inferSourceType(tc.source)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildSourcesList(t *testing.T) {
	t.Parallel()

	t.Run("empty buckets", func(t *testing.T) {
		t.Parallel()
		result := buildSourcesList(nil)
		assert.Empty(t, result)
	})

	t.Run("single source", func(t *testing.T) {
		t.Parallel()
		buckets := []*ProcessedBucket{
			{
				Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-shopify"}}},
				Source:     "shopify",
				TableCount: 5,
			},
		}
		result := buildSourcesList(buckets)
		require.Len(t, result, 1)
		assert.Equal(t, "shopify", result[0]["id"])
		assert.Equal(t, "Shopify", result[0]["name"])
		assert.Equal(t, "extractor", result[0]["type"])
		assert.Equal(t, 1, result[0]["instances"])
		assert.Equal(t, 5, result[0]["total_tables"])
	})

	t.Run("multiple sources", func(t *testing.T) {
		t.Parallel()
		buckets := []*ProcessedBucket{
			{
				Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-shopify"}}},
				Source:     "shopify",
				TableCount: 5,
			},
			{
				Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-shopify-2"}}},
				Source:     "shopify",
				TableCount: 3,
			},
			{
				Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-hubspot"}}},
				Source:     "hubspot",
				TableCount: 2,
			},
		}
		result := buildSourcesList(buckets)
		require.Len(t, result, 2)

		// Find sources by ID
		sourceMap := make(map[string]map[string]any)
		for _, s := range result {
			sourceMap[s["id"].(string)] = s
		}

		// Verify shopify source
		shopify := sourceMap["shopify"]
		assert.Equal(t, 2, shopify["instances"])
		assert.Equal(t, 8, shopify["total_tables"])

		// Verify hubspot source
		hubspot := sourceMap["hubspot"]
		assert.Equal(t, 1, hubspot["instances"])
		assert.Equal(t, 2, hubspot["total_tables"])
	})
}

// =============================================================================
// Index Builder Tests
// =============================================================================

func TestBuildBucketIndex(t *testing.T) {
	t.Parallel()

	t.Run("empty data", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			Buckets: []*ProcessedBucket{},
			Tables:  []*ProcessedTable{},
		}
		result := gen.buildBucketIndex(data)

		assert.Equal(t, 0, result["total_buckets"])
		assert.NotEmpty(t, result["_comment"])
		assert.NotEmpty(t, result["_purpose"])
		assert.NotEmpty(t, result["_update_frequency"])
	})

	t.Run("with buckets and tables", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			Buckets: []*ProcessedBucket{
				{
					Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-shopify"}}},
					Source:     "shopify",
					TableCount: 2,
				},
				{
					Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "out", BucketName: "c-transformed"}}},
					Source:     "transformation",
					TableCount: 1,
				},
			},
			Tables: []*ProcessedTable{
				{
					Table:      &keboola.Table{Name: "orders"},
					BucketName: "shopify",
				},
				{
					Table:      &keboola.Table{Name: "customers"},
					BucketName: "shopify",
				},
				{
					Table:      &keboola.Table{Name: "processed"},
					BucketName: "transformed",
				},
			},
		}
		result := gen.buildBucketIndex(data)

		assert.Equal(t, 2, result["total_buckets"])

		// Check by_source stats
		bySource := result["by_source"].(map[string]map[string]any)
		assert.Equal(t, 1, bySource["shopify"]["count"])
		assert.Equal(t, 2, bySource["shopify"]["total_tables"])
		assert.Equal(t, 1, bySource["transformation"]["count"])
		assert.Equal(t, 1, bySource["transformation"]["total_tables"])

		// Check buckets list
		buckets := result["buckets"].([]map[string]any)
		assert.Len(t, buckets, 2)
	})
}

func TestBuildTransformationIndex(t *testing.T) {
	t.Parallel()

	t.Run("empty data", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			Transformations: []*ProcessedTransformation{},
		}
		result := gen.buildTransformationIndex(data)

		assert.Equal(t, 0, result["total_transformations"])
		assert.NotEmpty(t, result["_comment"])
		assert.NotEmpty(t, result["_purpose"])
	})

	t.Run("with transformations", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			Transformations: []*ProcessedTransformation{
				{
					UID:        "transform:t1",
					Name:       "Transform 1",
					Platform:   PlatformSnowflake,
					IsDisabled: false,
					Dependencies: &TransformationDependencies{
						Consumes: []string{"table:a/b"},
						Produces: []string{"table:c/d"},
					},
					JobExecution: &JobExecution{
						LastRunTime:   "2024-01-15T10:00:00Z",
						LastRunStatus: "success",
						JobReference:  "job-123",
					},
				},
				{
					UID:        "transform:t2",
					Name:       "Transform 2",
					Platform:   PlatformPython,
					IsDisabled: true,
					Dependencies: &TransformationDependencies{
						Consumes: []string{"table:e/f", "table:g/h"},
						Produces: []string{},
					},
				},
			},
		}
		result := gen.buildTransformationIndex(data)

		assert.Equal(t, 2, result["total_transformations"])

		// Check by_platform stats
		byPlatform := result["by_platform"].(map[string]int)
		assert.Equal(t, 1, byPlatform[PlatformSnowflake])
		assert.Equal(t, 1, byPlatform[PlatformPython])

		// Check transformations list
		transformations := result["transformations"].([]map[string]any)
		assert.Len(t, transformations, 2)

		// Verify first transformation
		t1 := transformations[0]
		assert.Equal(t, "transform:t1", t1["uid"])
		assert.Equal(t, "Transform 1", t1["name"])
		assert.Equal(t, PlatformSnowflake, t1["platform"])
		assert.Equal(t, false, t1["is_disabled"])
		assert.Equal(t, 1, t1["input_count"])
		assert.Equal(t, 1, t1["output_count"])
		assert.Equal(t, "2024-01-15T10:00:00Z", t1["last_run_time"])
		assert.Equal(t, "success", t1["last_run_status"])

		// Verify second transformation
		t2 := transformations[1]
		assert.Equal(t, true, t2["is_disabled"])
		assert.Equal(t, 2, t2["input_count"])
		assert.Equal(t, 0, t2["output_count"])
	})
}

func TestBuildJobsIndex(t *testing.T) {
	t.Parallel()

	t.Run("empty data", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			Jobs: []*ProcessedJob{},
		}
		result := gen.buildJobsIndex(data)

		assert.Equal(t, 0, result["total_jobs"])
		assert.Equal(t, 0, result["recent_jobs_count"])
		assert.NotEmpty(t, result["_comment"])
	})

	t.Run("with jobs", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		endTime := iso8601.Time{Time: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)}
		data := &ProcessedData{
			Jobs: []*ProcessedJob{
				{
					QueueJobDetail: &keboola.QueueJobDetail{
						JobKey:          keboola.JobKey{ID: "job-1"},
						ComponentID:     "keboola.snowflake-transformation",
						ConfigID:        "config-1",
						Status:          "success",
						OperationName:   "transformationRun",
						EndTime:         &endTime,
						DurationSeconds: 120,
					},
				},
				{
					QueueJobDetail: &keboola.QueueJobDetail{
						JobKey:        keboola.JobKey{ID: "job-2"},
						ComponentID:   "keboola.ex-db-mysql",
						ConfigID:      "config-2",
						Status:        "error",
						OperationName: "run",
					},
				},
			},
		}
		result := gen.buildJobsIndex(data)

		assert.Equal(t, 2, result["total_jobs"])
		assert.Equal(t, 2, result["recent_jobs_count"])

		// Check by_status stats
		byStatus := result["by_status"].(map[string]int)
		assert.Equal(t, 1, byStatus["success"])
		assert.Equal(t, 1, byStatus["error"])

		// Check by_operation stats
		byOperation := result["by_operation"].(map[string]int)
		assert.Equal(t, 1, byOperation["transformationRun"])
		assert.Equal(t, 1, byOperation["run"])

		// Check transformation stats
		transformations := result["transformations"].(map[string]any)
		assert.Equal(t, 1, transformations["total_runs"])
	})
}

func TestBuildComponentsIndex(t *testing.T) {
	t.Parallel()

	t.Run("empty data", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			ComponentConfigs: []*ComponentConfig{},
		}
		result := gen.buildComponentsIndex(data)

		assert.Equal(t, 0, result["total_components"])
		assert.NotEmpty(t, result["_comment"])
	})

	t.Run("with components", func(t *testing.T) {
		t.Parallel()
		gen, _ := newTestGenerator(t)
		data := &ProcessedData{
			ComponentConfigs: []*ComponentConfig{
				{
					ID:            "config-1",
					Name:          "MySQL Extractor",
					ComponentID:   "keboola.ex-db-mysql",
					ComponentType: "extractor",
					IsDisabled:    false,
					Description:   "Extracts data from MySQL",
					LastRun:       "2024-01-15T10:00:00Z",
					Status:        "success",
				},
				{
					ID:            "config-2",
					Name:          "Snowflake Writer",
					ComponentID:   "keboola.wr-db-snowflake",
					ComponentType: "writer",
					IsDisabled:    true,
				},
			},
		}
		result := gen.buildComponentsIndex(data)

		assert.Equal(t, 2, result["total_components"])

		// Check by_type stats
		byType := result["by_type"].(map[string]int)
		assert.Equal(t, 1, byType["extractor"])
		assert.Equal(t, 1, byType["writer"])

		// Check components list
		components := result["components"].([]map[string]any)
		assert.Len(t, components, 2)

		// Verify first component
		c1 := components[0]
		assert.Equal(t, "config-1", c1["id"])
		assert.Equal(t, "MySQL Extractor", c1["name"])
		assert.Equal(t, "extractor", c1["component_type"])
		assert.Equal(t, false, c1["is_disabled"])
		assert.Equal(t, "Extracts data from MySQL", c1["description"])
		assert.Equal(t, "2024-01-15T10:00:00Z", c1["last_run"])
		assert.Equal(t, "success", c1["status"])
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestGenerator_Generate_Integration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	gen, fs := newTestGenerator(t)

	// Create test data
	data := &ProcessedData{
		ProjectID: 12345,
		BranchID:  67890,
		Buckets: []*ProcessedBucket{
			{
				Bucket:     &keboola.Bucket{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-shopify"}}},
				Source:     "shopify",
				TableCount: 1,
			},
		},
		Tables: []*ProcessedTable{
			{
				Table: &keboola.Table{
					TableKey: keboola.TableKey{
						TableID: keboola.TableID{
							BucketID:  keboola.BucketID{Stage: "in", BucketName: "c-shopify"},
							TableName: "orders",
						},
					},
					Name:    "orders",
					Columns: []string{"id", "customer_id", "total"},
				},
				UID:        "table:shopify/orders",
				Source:     "shopify",
				BucketName: "shopify",
				Dependencies: &TableDependencies{
					ConsumedBy: []string{"transform:process-orders"},
					ProducedBy: []string{},
				},
			},
		},
		Transformations: []*ProcessedTransformation{
			{
				UID:         "transform:config-1",
				Name:        "Process Orders",
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    "config-1",
				Platform:    PlatformSnowflake,
				IsDisabled:  false,
				Dependencies: &TransformationDependencies{
					Consumes: []string{"table:shopify/orders"},
					Produces: []string{"table:transformed/processed_orders"},
				},
				CodeBlocks: []*ProcessedCodeBlock{
					{
						Name:     "Main",
						Language: LanguageSQL,
						Codes: []*ProcessedCode{
							{Name: "Process", Language: LanguageSQL, Script: "SELECT * FROM orders;"},
						},
					},
				},
			},
		},
		Jobs:             []*ProcessedJob{},
		ComponentConfigs: []*ComponentConfig{},
		LineageGraph: &LineageGraph{
			Edges: []*LineageEdge{
				{Source: "table:shopify/orders", Target: "transform:config-1", Type: EdgeTypeConsumedBy},
				{Source: "transform:config-1", Target: "table:transformed/processed_orders", Type: EdgeTypeProduces},
			},
			TableNodes: map[string]bool{"table:shopify/orders": true, "table:transformed/processed_orders": true},
			TransNodes: map[string]bool{"transform:config-1": true},
			NodeCount:  3,
		},
		Statistics: &ProcessingStatistics{
			TotalBuckets:         1,
			TotalTables:          1,
			TotalTransformations: 1,
			TotalEdges:           2,
			ByPlatform:           map[string]int{PlatformSnowflake: 1},
			BySource:             map[string]int{"shopify": 1},
		},
	}

	// Generate
	err := gen.Generate(ctx, data)
	require.NoError(t, err)

	// Verify directory structure was created
	assertDirExists(t, fs, "/output")
	assertDirExists(t, fs, "/output/buckets")
	assertDirExists(t, fs, "/output/transformations")
	assertDirExists(t, fs, "/output/components")
	assertDirExists(t, fs, "/output/jobs")
	assertDirExists(t, fs, "/output/jobs/recent")
	assertDirExists(t, fs, "/output/jobs/by-component")
	assertDirExists(t, fs, "/output/indices")
	assertDirExists(t, fs, "/output/indices/queries")
	assertDirExists(t, fs, "/output/ai")

	// Verify bucket index was created
	assertFileExists(t, fs, "/output/buckets/index.json")
	bucketIndexContent := readJSONFile(t, fs, "/output/buckets/index.json")
	assert.EqualValues(t, 1, bucketIndexContent["total_buckets"])

	// Verify table metadata was created
	assertFileExists(t, fs, "/output/buckets/shopify/tables/orders/metadata.json")
	tableMetadata := readJSONFile(t, fs, "/output/buckets/shopify/tables/orders/metadata.json")
	assert.Equal(t, "table:shopify/orders", tableMetadata["uid"])
	assert.Equal(t, "orders", tableMetadata["name"])
	assert.Equal(t, "shopify", tableMetadata["source"])

	// Verify transformation index was created
	assertFileExists(t, fs, "/output/transformations/index.json")
	transformIndexContent := readJSONFile(t, fs, "/output/transformations/index.json")
	assert.EqualValues(t, 1, transformIndexContent["total_transformations"])

	// Verify transformation metadata was created (directory name is sanitized)
	assertFileExists(t, fs, "/output/transformations/Process-Orders/metadata.json")
	transformMetadata := readJSONFile(t, fs, "/output/transformations/Process-Orders/metadata.json")
	assert.Equal(t, "transform:config-1", transformMetadata["uid"])
	assert.Equal(t, "Process Orders", transformMetadata["name"])
	assert.Equal(t, PlatformSnowflake, transformMetadata["platform"])

	// Verify transformation code was created
	assertFileExists(t, fs, "/output/transformations/Process-Orders/code/01-Process.sql")

	// Verify jobs index was created
	assertFileExists(t, fs, "/output/jobs/index.json")

	// Verify lineage graph was created
	assertFileExists(t, fs, "/output/indices/graph.jsonl")

	// Verify sources index was created
	assertFileExists(t, fs, "/output/indices/sources.json")

	// Verify query files were created
	assertFileExists(t, fs, "/output/indices/queries/tables-by-source.json")
	assertFileExists(t, fs, "/output/indices/queries/transformations-by-platform.json")
	assertFileExists(t, fs, "/output/indices/queries/most-connected-nodes.json")

	// Verify root files were created
	assertFileExists(t, fs, "/output/manifest-extended.json")
	assertFileExists(t, fs, "/output/manifest.yaml")
	assertFileExists(t, fs, "/output/README.md")

	// Verify AI guide was created
	assertFileExists(t, fs, "/output/ai/AGENT_INSTRUCTIONS.md")
	assertFileExists(t, fs, "/output/ai/README.md")
}

func TestGenerator_Generate_EmptyData(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	gen, fs := newTestGenerator(t)

	// Create minimal test data
	data := &ProcessedData{
		ProjectID:        12345,
		BranchID:         67890,
		Buckets:          []*ProcessedBucket{},
		Tables:           []*ProcessedTable{},
		Transformations:  []*ProcessedTransformation{},
		Jobs:             []*ProcessedJob{},
		ComponentConfigs: []*ComponentConfig{},
		LineageGraph: &LineageGraph{
			Edges:      []*LineageEdge{},
			TableNodes: map[string]bool{},
			TransNodes: map[string]bool{},
			NodeCount:  0,
		},
		Statistics: &ProcessingStatistics{
			TotalBuckets:         0,
			TotalTables:          0,
			TotalTransformations: 0,
			TotalEdges:           0,
			ByPlatform:           map[string]int{},
			BySource:             map[string]int{},
		},
	}

	// Generate
	err := gen.Generate(ctx, data)
	require.NoError(t, err)

	// Verify directory structure was created
	assertDirExists(t, fs, "/output")

	// Verify index files were created even with empty data
	assertFileExists(t, fs, "/output/buckets/index.json")
	assertFileExists(t, fs, "/output/transformations/index.json")
	assertFileExists(t, fs, "/output/jobs/index.json")
	assertFileExists(t, fs, "/output/manifest-extended.json")
	assertFileExists(t, fs, "/output/README.md")
}

func TestGenerator_GenerateComponentConfig(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	gen, fs := newTestGenerator(t)

	// Create directories
	err := fs.Mkdir(ctx, "/output/components")
	require.NoError(t, err)

	config := &ComponentConfig{
		ID:            "config-123",
		Name:          "MySQL Extractor",
		ComponentID:   "keboola.ex-db-mysql",
		ComponentType: "extractor",
		IsDisabled:    false,
		Description:   "Extracts customer data from MySQL",
		Version:       5,
		Created:       "2024-01-01T00:00:00Z",
		LastRun:       "2024-01-15T10:00:00Z",
		Status:        "success",
		Configuration: map[string]any{
			"host": "db.example.com",
			"port": 3306,
		},
	}

	err = gen.generateComponentConfig(ctx, config)
	require.NoError(t, err)

	// Verify file was created
	assertFileExists(t, fs, "/output/components/keboola.ex-db-mysql/config-123/config.json")

	// Verify content
	content := readJSONFile(t, fs, "/output/components/keboola.ex-db-mysql/config-123/config.json")
	assert.Equal(t, "config-123", content["id"])
	assert.Equal(t, "MySQL Extractor", content["name"])
	assert.Equal(t, "keboola.ex-db-mysql", content["component_id"])
	assert.Equal(t, "extractor", content["component_type"])
	assert.Equal(t, false, content["is_disabled"])
	assert.Equal(t, "Extracts customer data from MySQL", content["description"])
	assert.EqualValues(t, 5, content["version"])
	assert.Equal(t, "2024-01-01T00:00:00Z", content["created"])
	assert.Equal(t, "2024-01-15T10:00:00Z", content["last_run"])
	assert.Equal(t, "success", content["status"])

	// Verify configuration was included
	configuration := content["configuration"].(map[string]any)
	assert.Equal(t, "db.example.com", configuration["host"])
}

func TestGenerator_GenerateTransformationCode(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	gen, fs := newTestGenerator(t)

	// Create transformation directory
	transformDir := "/output/transformations/test-transform"
	err := fs.Mkdir(ctx, transformDir)
	require.NoError(t, err)

	transform := &ProcessedTransformation{
		UID:      "transform:test",
		Name:     "Test Transform",
		Platform: PlatformSnowflake,
		CodeBlocks: []*ProcessedCodeBlock{
			{
				Name:     "Setup",
				Language: LanguageSQL,
				Codes: []*ProcessedCode{
					{Name: "Create Table", Language: LanguageSQL, Script: "CREATE TABLE test;"},
					{Name: "Insert Data", Language: LanguageSQL, Script: "INSERT INTO test VALUES (1);"},
				},
			},
			{
				Name:     "Transform",
				Language: LanguageSQL,
				Codes: []*ProcessedCode{
					{Name: "Process", Language: LanguageSQL, Script: "SELECT * FROM test;"},
				},
			},
		},
	}

	err = gen.generateTransformationCode(ctx, transformDir, transform)
	require.NoError(t, err)

	// Verify code files were created with correct names
	assertFileExists(t, fs, "/output/transformations/test-transform/code/01-Create-Table.sql")
	assertFileExists(t, fs, "/output/transformations/test-transform/code/02-Insert-Data.sql")
	assertFileExists(t, fs, "/output/transformations/test-transform/code/03-Process.sql")

	// Verify content of first file
	content1, err := fs.ReadFile(ctx, filesystem.NewFileDef("/output/transformations/test-transform/code/01-Create-Table.sql"))
	require.NoError(t, err)
	assert.Contains(t, content1.Content, "CREATE TABLE test;")
}

func TestGenerator_GeneratePythonCode(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	gen, fs := newTestGenerator(t)

	// Create transformation directory
	transformDir := "/output/transformations/python-transform"
	err := fs.Mkdir(ctx, transformDir)
	require.NoError(t, err)

	transform := &ProcessedTransformation{
		UID:      "transform:python",
		Name:     "Python Transform",
		Platform: PlatformPython,
		CodeBlocks: []*ProcessedCodeBlock{
			{
				Name:     "Main",
				Language: LanguagePython,
				Codes: []*ProcessedCode{
					{Name: "Script", Language: LanguagePython, Script: "print('Hello World')"},
				},
			},
		},
	}

	err = gen.generateTransformationCode(ctx, transformDir, transform)
	require.NoError(t, err)

	// Verify Python file was created with .py extension
	assertFileExists(t, fs, "/output/transformations/python-transform/code/01-Script.py")
}

// =============================================================================
// Data Quality Tests
// =============================================================================

func TestCalculateDataQuality(t *testing.T) {
	t.Parallel()

	g := &Generator{}

	tests := []struct {
		name     string
		sample   *TableSample
		expected map[string]any
	}{
		{
			name: "empty sample",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{},
				Rows:     [][]string{},
				RowCount: 0,
			},
			expected: map[string]any{
				"completeness":    map[string]int{},
				"null_counts":     map[string]int{},
				"distinct_counts": map[string]int{},
				"sample_size":     0,
			},
		},
		{
			name: "all non-null values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1", "col2"},
				Rows:     [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}},
				RowCount: 3,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 100, "col2": 100},
				"null_counts":     map[string]int{"col1": 0, "col2": 0},
				"distinct_counts": map[string]int{"col1": 3, "col2": 3},
				"sample_size":     3,
			},
		},
		{
			name: "some null values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1", "col2"},
				Rows:     [][]string{{"a", ""}, {"", "d"}, {"e", "f"}, {"", ""}},
				RowCount: 4,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 50, "col2": 50},
				"null_counts":     map[string]int{"col1": 2, "col2": 2},
				"distinct_counts": map[string]int{"col1": 2, "col2": 2},
				"sample_size":     4,
			},
		},
		{
			name: "all null values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1"},
				Rows:     [][]string{{""}, {""}, {""}},
				RowCount: 3,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 0},
				"null_counts":     map[string]int{"col1": 3},
				"distinct_counts": map[string]int{"col1": 0},
				"sample_size":     3,
			},
		},
		{
			name: "duplicate values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1"},
				Rows:     [][]string{{"a"}, {"a"}, {"b"}, {"b"}, {"b"}},
				RowCount: 5,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 100},
				"null_counts":     map[string]int{"col1": 0},
				"distinct_counts": map[string]int{"col1": 2},
				"sample_size":     5,
			},
		},
		{
			name: "row shorter than columns",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1", "col2", "col3"},
				Rows:     [][]string{{"a"}, {"b", "c"}},
				RowCount: 2,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 100, "col2": 50, "col3": 0},
				"null_counts":     map[string]int{"col1": 0, "col2": 1, "col3": 2},
				"distinct_counts": map[string]int{"col1": 2, "col2": 1, "col3": 0},
				"sample_size":     2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := g.calculateDataQuality(tt.sample)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Test Helpers
// =============================================================================

func assertDirExists(t *testing.T, fs filesystem.Fs, path string) {
	t.Helper()
	exists := fs.IsDir(context.Background(), path)
	assert.True(t, exists, "directory %s should exist", path)
}

func assertFileExists(t *testing.T, fs filesystem.Fs, path string) {
	t.Helper()
	exists := fs.IsFile(context.Background(), path)
	assert.True(t, exists, "file %s should exist", path)
}

func readJSONFile(t *testing.T, fs filesystem.Fs, path string) map[string]any {
	t.Helper()
	file, err := fs.ReadFile(context.Background(), filesystem.NewFileDef(path))
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal([]byte(file.Content), &result)
	require.NoError(t, err)
	return result
}
