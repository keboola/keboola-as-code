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

// jsonStringResponder responds with the raw JSON body.
func jsonStringResponder(body string) httpmock.Responder {
	return func(_ *http.Request) (*http.Response, error) {
		return jsonStringResponse(body), nil
	}
}

func jsonStringResponse(body string) *http.Response {
	resp := httpmock.NewStringResponse(http.StatusOK, body)
	resp.Header.Set("Content-Type", "application/json")
	return resp
}

// tableListV2Page returns one page of the table list V2 with the given JSON items; an empty next link ends the listing.
func tableListV2Page(items, next string) string {
	return `{"data":[` + items + `],"meta":{"total":0,"limit":200},"links":{"self":"","first":"","next":"` + next + `"}}`
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

	// Two pages: a typed table with a definition and an untyped table, then a typed table without a definition.
	firstPage := tableListV2Page(`
		{"id":"in.test-bucket.typed","branchId":123,"name":"typed","displayName":"Typed","isTyped":true,"columns":["id"],
		 "definition":{"primaryKeysNames":[],"columns":[{"name":"id","basetype":"INTEGER","definition":{"type":"NUMBER","nullable":false}}]}},
		{"id":"in.test-bucket.untyped","branchId":123,"name":"untyped","displayName":"Untyped","isTyped":false,"columns":["note"],
		 "definition":{"primaryKeysNames":[],"columns":[{"name":"note","definition":{"description":"A note"}}]}}`,
		"https://connection.keboola.local/v2/storage/branch/123/table-list?include=columns,definition&page[after]=cursor1",
	)
	secondPage := tableListV2Page(`
		{"id":"in.test-bucket.legacy","branchId":123,"name":"legacy","displayName":"Legacy","isTyped":true,"columns":["amount"],"definition":null}`,
		"",
	)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/table-list`,
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "columns,definition", req.URL.Query().Get("include"))
			if req.URL.Query().Get("page[after]") == "cursor1" {
				return jsonStringResponse(secondPage), nil
			}
			return jsonStringResponse(firstPage), nil
		},
	)

	// Only the typed table without a definition is read from the table detail.
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.test-bucket.legacy$`,
		jsonStringResponder(`{"id":"in.test-bucket.legacy","name":"legacy",
			"definition":{"primaryKeysNames":[],"columns":[{"name":"amount","basetype":"NUMERIC","definition":{"type":"NUMBER","length":"38,2","nullable":true}}]}}`),
	)

	buckets, tables, err := fetcher.fetchBucketsWithTables(t.Context(), branchID)
	require.NoError(t, err)

	assert.Len(t, buckets, 1)
	assert.Equal(t, "Test Bucket", buckets[0].DisplayName)

	require.Len(t, tables, 3)
	assert.Equal(t, "in.test-bucket.typed", tables[0].TableID.String())
	assert.Equal(t, branchID, tables[0].BranchID)
	assert.Equal(t, []string{"id"}, tables[0].Columns)
	assert.Equal(t, "NUMBER", tables[0].Definition.Columns[0].Definition.Type)

	// The untyped table keeps its description-only definition from the listing.
	require.NotNil(t, tables[1].Definition)
	assert.Equal(t, "A note", tables[1].Definition.Columns[0].Definition.Description)

	// The typed table without a definition in the listing takes it from the table detail.
	require.NotNil(t, tables[2].Definition)
	assert.Equal(t, "38,2", tables[2].Definition.Columns[0].Definition.Length)

	calls := transport.GetCallCountInfo()
	assert.Equal(t, 2, calls[`GET =~/v2/storage/branch/123/table-list`])
	assert.Equal(t, 1, calls[`GET =~/v2/storage/branch/123/tables/in.test-bucket.legacy$`])
}

func TestFetchBucketsWithTables_DefinitionDetailFails(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/buckets$`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Bucket{}),
	)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/table-list`,
		jsonStringResponder(tableListV2Page(
			`{"id":"in.test-bucket.legacy","branchId":123,"name":"legacy","isTyped":true,"columns":["amount"],"definition":null}`, "",
		)),
	)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/123/tables/in.test-bucket.legacy$`,
		httpmock.NewStringResponder(500, "Internal Server Error"),
	)

	_, _, err := fetcher.fetchBucketsWithTables(t.Context(), branchID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot load definition of table in.test-bucket.legacy")
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
		`=~/v2/storage/branch/123/table-list`,
		jsonStringResponder(tableListV2Page(`{"id":"in.bucket.table1","branchId":123,"name":"table1"}`, "")),
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
		Tables: []*keboola.TableListV2Item{
			{
				TableKey: keboola.TableKey{
					BranchID: branchID,
					TableID: keboola.TableID{
						BucketID:  keboola.BucketID{Stage: keboola.BucketStageIn, BucketName: "bucket"},
						TableName: "table1",
					},
				},
				TableBranchID: branchID,
				Name:          "table1",
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
		`=~/v2/storage/branch/123/table-list`,
		jsonStringResponder(tableListV2Page("", "")),
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
	assert.Equal(t, 3, sample.RowCount())
	assert.Equal(t, []string{"1", "Alice", "100"}, sample.Rows[0])
}

func TestFetchTableSamples(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	tables := []*keboola.TableListV2Item{
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
	samples, err := fetcher.FetchTableSamples(t.Context(), tables, 100, 2)
	require.NoError(t, err)

	assert.Len(t, samples, 2)
	assert.Equal(t, "in.bucket.table1", samples[0].TableID.String())
	assert.Equal(t, "in.bucket.table2", samples[1].TableID.String())
}

func TestFetchTableSamples_SkipsFailedTables(t *testing.T) {
	t.Parallel()

	fetcher, transport := newTestFetcher(t)
	branchID := keboola.BranchID(123)

	tables := []*keboola.TableListV2Item{
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

	samples, err := fetcher.FetchTableSamples(t.Context(), tables, 100, 10)

	// Should return error indicating partial failure, but still return partial results.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch samples for 1 of 2 tables")

	// Should have 1 sample (failed table is skipped)
	assert.Len(t, samples, 1)
	assert.Equal(t, "in.bucket.good_table", samples[0].TableID.String())
}
