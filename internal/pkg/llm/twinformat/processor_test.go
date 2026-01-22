package twinformat

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat/configparser"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func TestPlatformToLanguage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		platform string
		expected string
	}{
		{name: "python platform", platform: PlatformPython, expected: LanguagePython},
		{name: "r platform", platform: PlatformR, expected: LanguageR},
		{name: "snowflake platform", platform: PlatformSnowflake, expected: LanguageSQL},
		{name: "bigquery platform", platform: PlatformBigQuery, expected: LanguageSQL},
		{name: "dbt platform", platform: PlatformDBT, expected: LanguageSQL},
		{name: "unknown platform", platform: PlatformUnknown, expected: LanguageSQL},
		{name: "empty platform", platform: "", expected: LanguageSQL},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := platformToLanguage(tc.platform)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractBucketName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucketID string
		expected string
	}{
		{name: "input bucket with c- prefix", bucketID: "in.c-shopify", expected: "shopify"},
		{name: "output bucket with c- prefix", bucketID: "out.c-transformed", expected: "transformed"},
		{name: "bucket without c- prefix", bucketID: "in.shopify", expected: "shopify"},
		{name: "complex bucket name", bucketID: "in.c-google-ads-data", expected: "google-ads-data"},
		{name: "single part", bucketID: "bucket", expected: "bucket"},
		{name: "empty string", bucketID: "", expected: ""},
		{name: "malformed c- only", bucketID: "in.c-", expected: "in.c-"}, // c- without name returns original
		{name: "three parts", bucketID: "in.c-bucket.extra", expected: "bucket"},
		{name: "missing bucket part", bucketID: "in.", expected: "in."}, // malformed returns original
		{name: "whitespace only", bucketID: "   ", expected: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractBucketName(tc.bucketID)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsJobNewer(t *testing.T) {
	t.Parallel()

	// Create test times
	olderTime := iso8601.Time{Time: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)}
	newerTime := iso8601.Time{Time: time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)}

	tests := []struct {
		name          string
		jobStartTime  *iso8601.Time
		existingStart *iso8601.Time
		expected      bool
	}{
		{
			name:          "both nil - keep existing",
			jobStartTime:  nil,
			existingStart: nil,
			expected:      false,
		},
		{
			name:          "job nil, existing has time - job is older",
			jobStartTime:  nil,
			existingStart: &olderTime,
			expected:      false,
		},
		{
			name:          "job has time, existing nil - job is newer",
			jobStartTime:  &olderTime,
			existingStart: nil,
			expected:      true,
		},
		{
			name:          "job is newer",
			jobStartTime:  &newerTime,
			existingStart: &olderTime,
			expected:      true,
		},
		{
			name:          "job is older",
			jobStartTime:  &olderTime,
			existingStart: &newerTime,
			expected:      false,
		},
		{
			name:          "same time - not newer",
			jobStartTime:  &olderTime,
			existingStart: &olderTime,
			expected:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			job := &keboola.QueueJobDetail{StartTime: tc.jobStartTime}
			existing := &keboola.QueueJobDetail{StartTime: tc.existingStart}
			result := isJobNewer(job, existing)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildJobMap(t *testing.T) {
	t.Parallel()

	// Create test times
	olderTime := iso8601.Time{Time: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)}
	newerTime := iso8601.Time{Time: time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)}

	t.Run("empty jobs", func(t *testing.T) {
		t.Parallel()
		result := buildJobMap(nil)
		assert.Empty(t, result)
	})

	t.Run("single job", func(t *testing.T) {
		t.Parallel()
		jobs := []*keboola.QueueJobDetail{
			{
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    "123",
				StartTime:   &olderTime,
			},
		}
		result := buildJobMap(jobs)
		assert.Len(t, result, 1)
		assert.Equal(t, jobs[0], result["keboola.snowflake-transformation:123"])
	})

	t.Run("multiple jobs same config - keeps newer", func(t *testing.T) {
		t.Parallel()
		olderJob := &keboola.QueueJobDetail{
			ComponentID: "keboola.snowflake-transformation",
			ConfigID:    "123",
			StartTime:   &olderTime,
			Status:      "success",
		}
		newerJob := &keboola.QueueJobDetail{
			ComponentID: "keboola.snowflake-transformation",
			ConfigID:    "123",
			StartTime:   &newerTime,
			Status:      "error",
		}
		jobs := []*keboola.QueueJobDetail{olderJob, newerJob}
		result := buildJobMap(jobs)
		assert.Len(t, result, 1)
		assert.Equal(t, "error", result["keboola.snowflake-transformation:123"].Status)
	})

	t.Run("multiple configs", func(t *testing.T) {
		t.Parallel()
		job1 := &keboola.QueueJobDetail{
			ComponentID: "keboola.snowflake-transformation",
			ConfigID:    "123",
			StartTime:   &olderTime,
		}
		job2 := &keboola.QueueJobDetail{
			ComponentID: "keboola.python-transformation",
			ConfigID:    "456",
			StartTime:   &newerTime,
		}
		jobs := []*keboola.QueueJobDetail{job1, job2}
		result := buildJobMap(jobs)
		assert.Len(t, result, 2)
		assert.Equal(t, job1, result["keboola.snowflake-transformation:123"])
		assert.Equal(t, job2, result["keboola.python-transformation:456"])
	})
}

func TestLanguageConstants(t *testing.T) {
	t.Parallel()

	// Verify language constants have expected values
	assert.Equal(t, "python", LanguagePython)
	assert.Equal(t, "r", LanguageR)
	assert.Equal(t, "sql", LanguageSQL)
}

// mockProcessorDeps implements ProcessorDependencies for testing.
type mockProcessorDeps struct {
	fs        filesystem.Fs
	logger    log.Logger
	telemetry telemetry.Telemetry
}

func (m *mockProcessorDeps) Fs() filesystem.Fs              { return m.fs }
func (m *mockProcessorDeps) Logger() log.Logger             { return m.logger }
func (m *mockProcessorDeps) Telemetry() telemetry.Telemetry { return m.telemetry }

func newTestProcessor(t *testing.T) *Processor {
	t.Helper()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(log.NewNopLogger()))
	deps := &mockProcessorDeps{
		fs:        fs,
		logger:    log.NewNopLogger(),
		telemetry: telemetry.NewNop(),
	}
	return NewProcessor(deps)
}

func TestProcessor_Process_Integration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	processor := newTestProcessor(t)

	// Create test data with multiple transformations and tables
	startTime := iso8601.Time{Time: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)}
	projectData := &ProjectData{
		ProjectID: 12345,
		BranchID:  67890,
		Buckets: []*keboola.Bucket{
			{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "in", BucketName: "c-shopify"}}},
			{BucketKey: keboola.BucketKey{BucketID: keboola.BucketID{Stage: "out", BucketName: "c-transformed"}}},
		},
		Tables: []*keboola.Table{
			{
				TableKey: keboola.TableKey{
					TableID: keboola.TableID{
						BucketID:  keboola.BucketID{Stage: "in", BucketName: "c-shopify"},
						TableName: "orders",
					},
				},
				Name: "orders",
			},
			{
				TableKey: keboola.TableKey{
					TableID: keboola.TableID{
						BucketID:  keboola.BucketID{Stage: "out", BucketName: "c-transformed"},
						TableName: "processed_orders",
					},
				},
				Name: "processed_orders",
			},
		},
		TransformationConfigs: []*configparser.TransformationConfig{
			{
				ID:          "config-1",
				Name:        "Process Orders",
				ComponentID: "keboola.snowflake-transformation",
				InputTables: []configparser.StorageMapping{
					{Source: "in.c-shopify.orders", Destination: "orders"},
				},
				OutputTables: []configparser.StorageMapping{
					{Source: "processed_orders", Destination: "out.c-transformed.processed_orders"},
				},
				Blocks: []*configparser.CodeBlock{
					{
						Name: "Main",
						Codes: []*configparser.Code{
							{Name: "Process", Script: "SELECT * FROM orders;"},
						},
					},
				},
			},
		},
		Jobs: []*keboola.QueueJobDetail{
			{
				JobKey:      keboola.JobKey{ID: "job-123"},
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    "config-1",
				Status:      "success",
				StartTime:   &startTime,
			},
		},
	}

	// Run the processor
	result, err := processor.Process(ctx, "/project", projectData)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify project metadata
	assert.Equal(t, keboola.ProjectID(12345), result.ProjectID)
	assert.Equal(t, keboola.BranchID(67890), result.BranchID)

	// Verify statistics
	assert.NotNil(t, result.Statistics)
	assert.Equal(t, 2, result.Statistics.TotalBuckets)
	assert.Equal(t, 2, result.Statistics.TotalTables)
	assert.Equal(t, 1, result.Statistics.TotalTransformations)
	assert.Equal(t, 1, result.Statistics.TotalJobs)

	// Verify platform statistics
	assert.Equal(t, 1, result.Statistics.ByPlatform[PlatformSnowflake])

	// Verify lineage graph
	require.NotNil(t, result.LineageGraph)
	assert.NotEmpty(t, result.LineageGraph.Edges)

	// Verify transformations were processed
	require.Len(t, result.Transformations, 1)
	transform := result.Transformations[0]
	assert.Equal(t, "Process Orders", transform.Name)
	assert.Equal(t, PlatformSnowflake, transform.Platform)

	// Verify code blocks were converted
	require.Len(t, transform.CodeBlocks, 1)
	assert.Equal(t, "Main", transform.CodeBlocks[0].Name)
	assert.Equal(t, LanguageSQL, transform.CodeBlocks[0].Language)

	// Verify job execution was linked
	require.NotNil(t, transform.JobExecution)
	assert.Equal(t, "success", transform.JobExecution.LastRunStatus)
	assert.Equal(t, "job-123", transform.JobExecution.JobReference)

	// Verify tables were processed
	require.Len(t, result.Tables, 2)

	// Verify buckets were processed with component-based source detection
	require.Len(t, result.Buckets, 2)
	for _, bucket := range result.Buckets {
		if bucket.BucketID.Stage == "in" {
			// Input bucket has no known source (tables not produced by transformation)
			assert.Equal(t, SourceUnknown, bucket.Source)
		} else {
			// Output bucket source is derived from transformation that produces its tables
			assert.Equal(t, "keboola.snowflake-transformation", bucket.Source)
		}
	}
}

func TestProcessor_Process_EmptyData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	processor := newTestProcessor(t)

	// Process empty data
	projectData := &ProjectData{
		ProjectID: 12345,
		BranchID:  67890,
	}

	result, err := processor.Process(ctx, "/project", projectData)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify empty results
	assert.Empty(t, result.Buckets)
	assert.Empty(t, result.Tables)
	assert.Empty(t, result.Transformations)
	assert.Empty(t, result.Jobs)

	// Verify statistics are zero
	assert.Equal(t, 0, result.Statistics.TotalBuckets)
	assert.Equal(t, 0, result.Statistics.TotalTables)
	assert.Equal(t, 0, result.Statistics.TotalTransformations)
	assert.Equal(t, 0, result.Statistics.TotalJobs)
}

func TestProcessor_Process_MultipleTransformations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	processor := newTestProcessor(t)

	// Create data with multiple transformations on different platforms
	projectData := &ProjectData{
		ProjectID: 12345,
		BranchID:  67890,
		TransformationConfigs: []*configparser.TransformationConfig{
			{
				ID:          "config-1",
				Name:        "Snowflake Transform",
				ComponentID: "keboola.snowflake-transformation",
			},
			{
				ID:          "config-2",
				Name:        "Python Transform",
				ComponentID: "keboola.python-transformation-v2",
			},
			{
				ID:          "config-3",
				Name:        "dbt Transform",
				ComponentID: "keboola.dbt-transformation",
			},
		},
	}

	result, err := processor.Process(ctx, "/project", projectData)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify all transformations were processed
	assert.Len(t, result.Transformations, 3)

	// Verify platform statistics
	assert.Equal(t, 1, result.Statistics.ByPlatform[PlatformSnowflake])
	assert.Equal(t, 1, result.Statistics.ByPlatform[PlatformPython])
	assert.Equal(t, 1, result.Statistics.ByPlatform[PlatformDBT])

	// Verify each transformation has correct platform
	platforms := make(map[string]string)
	for _, t := range result.Transformations {
		platforms[t.Name] = t.Platform
	}
	assert.Equal(t, PlatformSnowflake, platforms["Snowflake Transform"])
	assert.Equal(t, PlatformPython, platforms["Python Transform"])
	assert.Equal(t, PlatformDBT, platforms["dbt Transform"])
}

// mockLineageBuilderDeps implements LineageBuilderDependencies for testing.
type mockLineageBuilderDeps struct {
	logger    log.Logger
	telemetry telemetry.Telemetry
}

func (m *mockLineageBuilderDeps) Logger() log.Logger             { return m.logger }
func (m *mockLineageBuilderDeps) Telemetry() telemetry.Telemetry { return m.telemetry }

func newTestLineageBuilder(t *testing.T) *LineageBuilder {
	t.Helper()
	deps := &mockLineageBuilderDeps{
		logger:    log.NewNopLogger(),
		telemetry: telemetry.NewNop(),
	}
	return NewLineageBuilder(deps)
}

func TestLineageBuilder_BuildLineageGraph_Integration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	builder := newTestLineageBuilder(t)

	// Create transformation configs with input/output mappings
	configs := []*configparser.TransformationConfig{
		{
			ID:          "transform-1",
			Name:        "Process Orders",
			ComponentID: "keboola.snowflake-transformation",
			InputTables: []configparser.StorageMapping{
				{Source: "in.c-shopify.orders", Destination: "orders"},
				{Source: "in.c-shopify.customers", Destination: "customers"},
			},
			OutputTables: []configparser.StorageMapping{
				{Source: "enriched_orders", Destination: "out.c-transformed.enriched_orders"},
			},
		},
		{
			ID:          "transform-2",
			Name:        "Aggregate Orders",
			ComponentID: "keboola.snowflake-transformation",
			InputTables: []configparser.StorageMapping{
				{Source: "out.c-transformed.enriched_orders", Destination: "enriched_orders"},
			},
			OutputTables: []configparser.StorageMapping{
				{Source: "order_summary", Destination: "out.c-transformed.order_summary"},
			},
		},
	}

	graph, err := builder.BuildLineageGraph(ctx, configs)
	require.NoError(t, err)
	require.NotNil(t, graph)

	// Verify graph structure
	assert.NotEmpty(t, graph.Edges)
	assert.NotEmpty(t, graph.TableNodes)
	assert.NotEmpty(t, graph.TransNodes)

	// Verify node counts
	// Tables: orders, customers, enriched_orders, order_summary = 4
	// Transformations: 2
	assert.Len(t, graph.TableNodes, 4)
	assert.Len(t, graph.TransNodes, 2)
	assert.Equal(t, 6, graph.NodeCount)

	// Verify edge count
	// Transform 1: 2 inputs + 1 output = 3 edges
	// Transform 2: 1 input + 1 output = 2 edges
	// Total = 5 edges
	assert.Len(t, graph.Edges, 5)

	// Verify metadata
	require.NotNil(t, graph.Meta)
	assert.Equal(t, 5, graph.Meta.TotalEdges)
	assert.Equal(t, 6, graph.Meta.TotalNodes)

	// Verify specific edges exist
	hasInputEdge := false
	hasOutputEdge := false
	for _, edge := range graph.Edges {
		if edge.Type == EdgeTypeConsumedBy && edge.Source == "table:shopify/orders" {
			hasInputEdge = true
		}
		if edge.Type == EdgeTypeProduces && edge.Target == "table:transformed/enriched_orders" {
			hasOutputEdge = true
		}
	}
	assert.True(t, hasInputEdge, "should have input edge from shopify/orders")
	assert.True(t, hasOutputEdge, "should have output edge to transformed/enriched_orders")
}

func TestLineageBuilder_GetDependencies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	builder := newTestLineageBuilder(t)

	// Create a simple graph
	configs := []*configparser.TransformationConfig{
		{
			ID:          "transform-1",
			Name:        "Process",
			ComponentID: "keboola.snowflake-transformation",
			InputTables: []configparser.StorageMapping{
				{Source: "in.c-source.input", Destination: "input"},
			},
			OutputTables: []configparser.StorageMapping{
				{Source: "output", Destination: "out.c-dest.output"},
			},
		},
	}

	graph, err := builder.BuildLineageGraph(ctx, configs)
	require.NoError(t, err)

	// Test GetTableDependencies for input table
	inputTableUID := "table:source/input"
	inputDeps := builder.GetTableDependencies(graph, inputTableUID)
	assert.NotEmpty(t, inputDeps.ConsumedBy, "input table should be consumed by transformation")
	assert.Empty(t, inputDeps.ProducedBy, "input table should not be produced by anything")

	// Test GetTableDependencies for output table
	outputTableUID := "table:dest/output"
	outputDeps := builder.GetTableDependencies(graph, outputTableUID)
	assert.Empty(t, outputDeps.ConsumedBy, "output table should not be consumed by anything yet")
	assert.NotEmpty(t, outputDeps.ProducedBy, "output table should be produced by transformation")

	// Test GetTransformationDependencies - UID uses config ID for consistency with job-based UIDs
	transformUID := "transform:transform-1"
	transformDeps := builder.GetTransformationDependencies(graph, transformUID)
	assert.NotEmpty(t, transformDeps.Consumes, "transformation should consume input table")
	assert.NotEmpty(t, transformDeps.Produces, "transformation should produce output table")
}

func TestBuildTableUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		table    string
		expected string
	}{
		{name: "simple", bucket: "shopify", table: "orders", expected: "table:shopify/orders"},
		{name: "with dash", bucket: "google-ads", table: "campaigns", expected: "table:google-ads/campaigns"},
		{name: "with space", bucket: "my bucket", table: "my table", expected: "table:my bucket/my table"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := buildTableUID(tc.bucket, tc.table)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildTransformationUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "simple", input: "process-orders", expected: "transform:process-orders"},
		{name: "with space", input: "Process Orders", expected: "transform:Process Orders"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := buildTransformationUID(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
