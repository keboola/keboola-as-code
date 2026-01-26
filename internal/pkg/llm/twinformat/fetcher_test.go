package twinformat

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
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

func TestFetchTableSample(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	tableKey := keboola.TableKey{
		BranchID: branchID,
		TableID: keboola.TableID{
			BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "test"},
			TableName: "sample_table",
		},
	}

	// Mock table preview response - Storage API returns CSV format
	csvData := "\"id\",\"name\",\"value\"\n\"1\",\"Alice\",\"100\"\n\"2\",\"Bob\",\"200\"\n\"3\",\"Charlie\",\"300\"\n"
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.test.sample_table/data-preview`,
		httpmock.NewStringResponder(200, csvData),
	)

	sample, err := fetcher.FetchTableSample(t.Context(), tableKey, 100)
	require.NoError(t, err)

	assert.Equal(t, tableKey.TableID, sample.TableID)
	assert.Equal(t, []string{"id", "name", "value"}, sample.Columns)
	assert.Len(t, sample.Rows, 3)
	assert.Equal(t, 3, sample.RowCount)
	assert.Equal(t, []string{"1", "Alice", "100"}, sample.Rows[0])
}

func TestFetchTableSamples(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	tables := []*keboola.Table{
		{
			TableKey: keboola.TableKey{
				BranchID: branchID,
				TableID: keboola.TableID{
					BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
					TableName: "table1",
				},
			},
		},
		{
			TableKey: keboola.TableKey{
				BranchID: branchID,
				TableID: keboola.TableID{
					BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
					TableName: "table2",
				},
			},
		},
		{
			TableKey: keboola.TableKey{
				BranchID: branchID,
				TableID: keboola.TableID{
					BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
					TableName: "table3",
				},
			},
		},
	}

	// Mock preview responses for each table - Storage API returns CSV format
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.bucket.table1/data-preview`,
		httpmock.NewStringResponder(200, "\"col1\"\n\"val1\"\n"),
	)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.bucket.table2/data-preview`,
		httpmock.NewStringResponder(200, "\"col2\"\n\"val2\"\n"),
	)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.bucket.table3/data-preview`,
		httpmock.NewStringResponder(200, "\"col3\"\n\"val3\"\n"),
	)

	// Test with maxTables=2 (should only fetch first 2 tables)
	samples, err := fetcher.FetchTableSamples(t.Context(), tables, branchID, 100, 2)
	require.NoError(t, err)

	assert.Len(t, samples, 2)
	assert.Equal(t, "in.bucket.table1", samples[0].TableID.String())
	assert.Equal(t, "in.bucket.table2", samples[1].TableID.String())
}

func TestFetchTableSamples_SkipsFailedTables(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	tables := []*keboola.Table{
		{
			TableKey: keboola.TableKey{
				BranchID: branchID,
				TableID: keboola.TableID{
					BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
					TableName: "good_table",
				},
			},
		},
		{
			TableKey: keboola.TableKey{
				BranchID: branchID,
				TableID: keboola.TableID{
					BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
					TableName: "bad_table",
				},
			},
		},
	}

	// First table succeeds - Storage API returns CSV format
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.bucket.good_table/data-preview`,
		httpmock.NewStringResponder(200, "\"col1\"\n\"val1\"\n"),
	)

	// Second table fails
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.bucket.bad_table/data-preview`,
		httpmock.NewStringResponder(500, "Internal Server Error"),
	)

	samples, err := fetcher.FetchTableSamples(t.Context(), tables, branchID, 100, 10)
	require.NoError(t, err)

	// Should have 1 sample (failed table is skipped)
	assert.Len(t, samples, 1)
	assert.Equal(t, "in.bucket.good_table", samples[0].TableID.String())
}
