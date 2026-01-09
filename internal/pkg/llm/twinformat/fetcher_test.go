package twinformat

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// mockFetcherDeps implements FetcherDependencies for testing.
type mockFetcherDeps struct {
	api       *keboola.AuthorizedAPI
	logger    log.Logger
	projectID keboola.ProjectID
	telemetry telemetry.Telemetry
}

func (m *mockFetcherDeps) KeboolaProjectAPI() *keboola.AuthorizedAPI { return m.api }
func (m *mockFetcherDeps) Logger() log.Logger                        { return m.logger }
func (m *mockFetcherDeps) ProjectID() keboola.ProjectID              { return m.projectID }
func (m *mockFetcherDeps) Telemetry() telemetry.Telemetry            { return m.telemetry }

func newTestFetcher(t *testing.T) (*Fetcher, *httpmock.MockTransport) {
	t.Helper()

	httpClient, transport := client.NewMockedClient()

	// Register default responses for API initialization - including queue service for jobs
	transport.RegisterResponder(
		http.MethodGet,
		`https://connection.keboola.local/v2/storage/?exclude=components`,
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{
				Services: keboola.Services{
					{ID: "queue", URL: "https://queue.keboola.local"},
				},
				Features: keboola.Features{},
			},
			Components: keboola.Components{},
		}),
	)

	api, err := keboola.NewAuthorizedAPI(
		t.Context(),
		"https://connection.keboola.local",
		"test-token",
		keboola.WithClient(&httpClient),
	)
	require.NoError(t, err)

	deps := &mockFetcherDeps{
		api:       api,
		logger:    log.NewNopLogger(),
		projectID: 12345,
		telemetry: telemetry.NewNop(),
	}

	return NewFetcher(deps), transport
}

func TestNewFetcher(t *testing.T) {
	t.Parallel()

	fetcher, _ := newTestFetcher(t)

	assert.NotNil(t, fetcher)
	assert.NotNil(t, fetcher.api)
	assert.NotNil(t, fetcher.logger)
	assert.Equal(t, keboola.ProjectID(12345), fetcher.projectID)
}

func TestFetchBucketsWithTables(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Mock buckets response
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/buckets$`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Bucket{
			{
				BucketKey: keboola.BucketKey{
					BranchID: branchID,
					BucketID: keboola.BucketID{
						Stage:      keboola.BucketStageIn,
						BucketName: "test-bucket",
					},
				},
				DisplayName: "Test Bucket",
			},
		}),
	)

	// Mock tables response with column metadata but no columns array
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Table{
			{
				TableKey: keboola.TableKey{
					BranchID: branchID,
					TableID: keboola.TableID{
						BucketID: keboola.BucketID{
							Stage:      keboola.BucketStageIn,
							BucketName: "test-bucket",
						},
						TableName: "test-table",
					},
				},
				DisplayName: "Test Table",
				Columns:     []string{}, // Empty - should be extracted from ColumnMetadata
				ColumnMetadata: map[string]keboola.ColumnMetadata{
					"zebra_col":  {},
					"alpha_col":  {},
					"middle_col": {},
				},
			},
		}),
	)

	buckets, tables, err := fetcher.fetchBucketsWithTables(t.Context(), branchID)
	require.NoError(t, err)

	assert.Len(t, buckets, 1)
	assert.Equal(t, "Test Bucket", buckets[0].DisplayName)

	assert.Len(t, tables, 1)
	assert.Equal(t, "Test Table", tables[0].DisplayName)

	// Verify columns were extracted from ColumnMetadata and sorted
	assert.Equal(t, []string{"alpha_col", "middle_col", "zebra_col"}, tables[0].Columns)
}

func TestFetchBucketsWithTables_ColumnsAlreadyPresent(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Mock buckets response
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/buckets$`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Bucket{}),
	)

	// Mock tables response with columns already present
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Table{
			{
				TableKey: keboola.TableKey{
					BranchID: branchID,
					TableID: keboola.TableID{
						BucketID: keboola.BucketID{
							Stage:      keboola.BucketStageIn,
							BucketName: "test-bucket",
						},
						TableName: "test-table",
					},
				},
				Columns: []string{"original_col1", "original_col2"}, // Already present
				ColumnMetadata: map[string]keboola.ColumnMetadata{
					"different_col": {}, // Should be ignored
				},
			},
		}),
	)

	_, tables, err := fetcher.fetchBucketsWithTables(t.Context(), branchID)
	require.NoError(t, err)

	// Original columns should be preserved (not overwritten by ColumnMetadata)
	assert.Equal(t, []string{"original_col1", "original_col2"}, tables[0].Columns)
}

func TestFetchJobsQueue(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Mock jobs queue search response
	transport.RegisterResponder(
		http.MethodGet,
		`=~queue.keboola.local/search/jobs`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.QueueJobDetail{
			{
				JobKey:      keboola.JobKey{ID: "1001"},
				BranchID:    branchID,
				Status:      "success",
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    "config-1",
			},
			{
				JobKey:      keboola.JobKey{ID: "1002"},
				BranchID:    branchID,
				Status:      "error",
				ComponentID: "keboola.python-transformation-v2",
				ConfigID:    "config-2",
			},
		}),
	)

	jobs, err := fetcher.fetchJobsQueue(t.Context(), branchID)
	require.NoError(t, err)

	assert.Len(t, jobs, 2)
	assert.Equal(t, keboola.JobID("1001"), jobs[0].ID)
	assert.Equal(t, "success", jobs[0].Status)
	assert.Equal(t, keboola.JobID("1002"), jobs[1].ID)
	assert.Equal(t, "error", jobs[1].Status)
}

func TestFetchTransformationConfigs(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Create config content using orderedmap
	configContent := orderedmap.New()
	storageSection := orderedmap.New()
	inputSection := orderedmap.New()
	inputTables := []any{
		map[string]any{
			"source":      "in.c-bucket.source-table",
			"destination": "source_table",
		},
	}
	inputSection.Set("tables", inputTables)
	storageSection.Set("input", inputSection.ToMap())

	outputSection := orderedmap.New()
	outputTables := []any{
		map[string]any{
			"source":      "result_table",
			"destination": "out.c-bucket.result",
		},
	}
	outputSection.Set("tables", outputTables)
	storageSection.Set("output", outputSection.ToMap())
	configContent.Set("storage", storageSection.ToMap())

	paramsSection := orderedmap.New()
	blocks := []any{
		map[string]any{
			"name": "Block 1",
			"codes": []any{
				map[string]any{
					"name":   "Code 1",
					"script": "SELECT * FROM source_table;",
				},
			},
		},
	}
	paramsSection.Set("blocks", blocks)
	configContent.Set("parameters", paramsSection.ToMap())

	// Mock components response - must include transformation and non-transformation components
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.ComponentWithConfigs{
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "keboola.snowflake-transformation"},
					Type:         "transformation",
					Name:         "Snowflake Transformation",
					Flags:        []string{"genericCodeBlocksUI"},
				},
				Configs: []*keboola.ConfigWithRows{
					{
						Config: &keboola.Config{
							ConfigKey: keboola.ConfigKey{
								BranchID:    branchID,
								ComponentID: "keboola.snowflake-transformation",
								ID:          "config-1",
							},
							Name:        "Test Transformation",
							Description: "A test transformation",
							Content:     configContent,
						},
					},
				},
			},
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "keboola.ex-db-mysql"},
					Type:         "extractor",
					Name:         "MySQL Extractor",
				},
				Configs: []*keboola.ConfigWithRows{
					{
						Config: &keboola.Config{
							ConfigKey: keboola.ConfigKey{
								BranchID:    branchID,
								ComponentID: "keboola.ex-db-mysql",
								ID:          "config-extractor",
							},
							Name:    "MySQL Extraction",
							Content: orderedmap.New(),
						},
					},
				},
			},
		}),
	)

	configs, err := fetcher.FetchTransformationConfigs(t.Context(), branchID)
	require.NoError(t, err)

	// Should only have transformation configs, not extractors
	assert.Len(t, configs, 1)
	assert.Equal(t, "Test Transformation", configs[0].Name)
	assert.Equal(t, "keboola.snowflake-transformation", configs[0].ComponentID)
	assert.Len(t, configs[0].InputTables, 1)
	assert.Equal(t, "in.c-bucket.source-table", configs[0].InputTables[0].Source)
	assert.Len(t, configs[0].OutputTables, 1)
	assert.Equal(t, "out.c-bucket.result", configs[0].OutputTables[0].Destination)
	assert.Len(t, configs[0].Blocks, 1)
	assert.Equal(t, "Block 1", configs[0].Blocks[0].Name)
}

func TestFetchComponentConfigs(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Create extractor config content
	extractorConfig := orderedmap.New()
	params := orderedmap.New()
	params.Set("host", "db.example.com")
	extractorConfig.Set("parameters", params.ToMap())

	// Mock components response
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.ComponentWithConfigs{
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "keboola.snowflake-transformation"},
					Type:         "transformation",
					Name:         "Snowflake Transformation",
					Flags:        []string{"genericCodeBlocksUI"},
				},
				Configs: []*keboola.ConfigWithRows{
					{
						Config: &keboola.Config{
							ConfigKey: keboola.ConfigKey{
								BranchID:    branchID,
								ComponentID: "keboola.snowflake-transformation",
								ID:          "config-1",
							},
							Name:    "Test Transformation",
							Content: orderedmap.New(),
						},
					},
				},
			},
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "keboola.ex-db-mysql"},
					Type:         "extractor",
					Name:         "MySQL Extractor",
				},
				Configs: []*keboola.ConfigWithRows{
					{
						Config: &keboola.Config{
							ConfigKey: keboola.ConfigKey{
								BranchID:    branchID,
								ComponentID: "keboola.ex-db-mysql",
								ID:          "config-extractor",
							},
							Name:        "MySQL Extraction",
							Description: "Extract from MySQL",
							Content:     extractorConfig,
						},
					},
				},
			},
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: keboola.SchedulerComponentID},
					Type:         "other",
					Name:         "Scheduler",
				},
				Configs: []*keboola.ConfigWithRows{
					{
						Config: &keboola.Config{
							ConfigKey: keboola.ConfigKey{
								BranchID:    branchID,
								ComponentID: keboola.SchedulerComponentID,
								ID:          "scheduler-1",
							},
							Name:    "Scheduler Config",
							Content: orderedmap.New(),
						},
					},
				},
			},
		}),
	)

	configs, err := fetcher.FetchComponentConfigs(t.Context(), branchID)
	require.NoError(t, err)

	// Should only have non-transformation, non-scheduler configs
	assert.Len(t, configs, 1)
	assert.Equal(t, "MySQL Extraction", configs[0].Name)
	assert.Equal(t, "keboola.ex-db-mysql", configs[0].ComponentID)
	assert.Equal(t, "extractor", configs[0].ComponentType)
}

func TestFetchAll(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Mock buckets response
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/buckets$`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Bucket{
			{
				BucketKey: keboola.BucketKey{
					BranchID: branchID,
					BucketID: keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
				},
			},
		}),
	)

	// Mock tables response
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Table{
			{
				TableKey: keboola.TableKey{
					BranchID: branchID,
					TableID: keboola.TableID{
						BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
						TableName: "table1",
					},
				},
			},
		}),
	)

	// Mock jobs queue search response
	transport.RegisterResponder(
		http.MethodGet,
		`=~queue.keboola.local/search/jobs`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.QueueJobDetail{}),
	)

	// Mock components response for transformations and component configs
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.ComponentWithConfigs{}),
	)

	data, err := fetcher.FetchAll(t.Context(), branchID)
	require.NoError(t, err)

	// Verify FetchedAt is set (can't compare exactly due to runtime value)
	assert.NotZero(t, data.FetchedAt)

	expectedData := &ProjectData{
		ProjectID: 12345,
		BranchID:  branchID,
		FetchedAt: data.FetchedAt, // Copy actual value for comparison
		Buckets: []*keboola.Bucket{
			{
				BucketKey: keboola.BucketKey{
					BranchID: branchID,
					BucketID: keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
				},
			},
		},
		Tables: []*keboola.Table{
			{
				TableKey: keboola.TableKey{
					BranchID: branchID,
					TableID: keboola.TableID{
						BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
						TableName: "table1",
					},
				},
			},
		},
		Jobs:                  []*keboola.QueueJobDetail{},
		TransformationConfigs: []*TransformationConfig{},
		ComponentConfigs:      []*ComponentConfig{},
		Components:            []*keboola.ComponentWithConfigs{},
	}
	assert.Equal(t, expectedData, data)
}

func TestFetchAll_JobsFailure_ReturnsEmptyJobs(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	// Mock buckets/tables - success
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/buckets$`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Bucket{}),
	)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Table{}),
	)

	// Mock jobs queue - failure
	transport.RegisterResponder(
		http.MethodGet,
		`=~queue.keboola.local/search/jobs`,
		httpmock.NewStringResponder(500, "Internal Server Error"),
	)

	// Mock components - success
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.ComponentWithConfigs{}),
	)

	data, err := fetcher.FetchAll(t.Context(), branchID)
	require.NoError(t, err)

	// Jobs failure should not cause overall failure
	assert.Empty(t, data.Jobs)
}

func TestExtractComponentFromUserAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		userAgent string
		want      string
	}{
		{
			name:      "standard keboola client format",
			userAgent: "Keboola Storage API PHP Client/14 kds-team.app-custom-python",
			want:      "kds-team.app-custom-python",
		},
		{
			name:      "transformation component",
			userAgent: "Keboola Storage API PHP Client/14 keboola.snowflake-transformation",
			want:      "keboola.snowflake-transformation",
		},
		{
			name:      "empty user agent",
			userAgent: "",
			want:      "",
		},
		{
			name:      "single word",
			userAgent: "keboola.component",
			want:      "keboola.component",
		},
		{
			name:      "client only no component",
			userAgent: "Keboola Storage API PHP Client/14",
			want:      "Client/14",
		},
		{
			name:      "with multiple spaces",
			userAgent: "Keboola  Storage  API  keboola.extractor",
			want:      "keboola.extractor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractComponentFromUserAgent(tt.userAgent)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetchTableLastImporter(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)

	tableID := keboola.TableID{
		BucketID:  keboola.BucketID{Stage: keboola.BucketStageOut, BucketName: "test"},
		TableName: "mytable",
	}

	// Mock table events response
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/tables/out.test.mytable/events`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.TableEvent{
			{
				ID:    "event-1",
				Event: "storage.tableDataLoaded",
				Context: keboola.TableEventContext{
					UserAgent: "Keboola Storage API PHP Client/14 keboola.snowflake-transformation",
				},
			},
			{
				ID:    "event-2",
				Event: "storage.tableImportDone",
				Context: keboola.TableEventContext{
					UserAgent: "Keboola Storage API PHP Client/14 kds-team.app-custom-python",
				},
			},
			{
				ID:    "event-3",
				Event: "storage.tableCreated",
				Context: keboola.TableEventContext{
					UserAgent: "Keboola Storage API PHP Client/14 keboola.ex-db-mysql",
				},
			},
		}),
	)

	componentID, err := fetcher.FetchTableLastImporter(t.Context(), tableID)
	require.NoError(t, err)

	// Should return the component from the first tableImportDone event
	assert.Equal(t, "kds-team.app-custom-python", componentID)
}

func TestFetchTableLastImporter_NoImportEvents(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)

	tableID := keboola.TableID{
		BucketID:  keboola.BucketID{Stage: keboola.BucketStageOut, BucketName: "test"},
		TableName: "mytable",
	}

	// Mock table events response with no import events
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/tables/out.test.mytable/events`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.TableEvent{
			{
				ID:    "event-1",
				Event: "storage.tableCreated",
				Context: keboola.TableEventContext{
					UserAgent: "Keboola Storage API PHP Client/14 keboola.component",
				},
			},
		}),
	)

	componentID, err := fetcher.FetchTableLastImporter(t.Context(), tableID)
	require.NoError(t, err)

	// No import events, should return empty
	assert.Empty(t, componentID)
}
