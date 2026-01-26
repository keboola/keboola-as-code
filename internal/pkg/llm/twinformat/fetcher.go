package twinformat

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat/configparser"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// FetcherDependencies defines the dependencies required by the Fetcher.
type FetcherDependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	Telemetry() telemetry.Telemetry
}

// Fetcher retrieves project data from Keboola APIs.
type Fetcher struct {
	api       *keboola.AuthorizedAPI
	logger    log.Logger
	parser    *configparser.Parser
	projectID keboola.ProjectID
	telemetry telemetry.Telemetry
}

// NewFetcher creates a new Fetcher instance.
func NewFetcher(d FetcherDependencies) *Fetcher {
	return &Fetcher{
		api:       d.KeboolaProjectAPI(),
		logger:    d.Logger(),
		parser:    configparser.NewParser(d.Logger()),
		projectID: d.ProjectID(),
		telemetry: d.Telemetry(),
	}
}

// FetchAll retrieves all project data from Keboola APIs.
func (f *Fetcher) FetchAll(ctx context.Context, branchID keboola.BranchID) (data *ProjectData, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchAll")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching project data from Keboola APIs...")

	data = &ProjectData{
		ProjectID: f.projectID,
		BranchID:  branchID,
		FetchedAt: time.Now().UTC(),
	}

	// Fetch buckets with tables
	buckets, tables, err := f.fetchBucketsWithTables(ctx, branchID)
	if err != nil {
		return nil, err
	}
	data.Buckets = buckets
	data.Tables = tables

	// Fetch jobs from Queue API with detailed result data (including input/output tables)
	jobs, err := f.fetchJobsQueue(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch jobs from Queue API: %v", err)
		data.Jobs = []*keboola.QueueJobDetail{}
	} else {
		data.Jobs = jobs
	}

	// Fetch all components with configs (single API call)
	compResult, err := f.fetchAllComponents(ctx, branchID)
	if err != nil {
		return nil, err
	}
	data.Components = compResult.Components
	data.TransformationConfigs = compResult.TransformConfigs
	data.ComponentConfigs = compResult.ComponentConfigs

	f.logger.Infof(ctx, "Fetched %d buckets, %d tables, %d jobs, %d components, %d transformations, %d component configs",
		len(data.Buckets), len(data.Tables), len(data.Jobs),
		len(data.Components), len(data.TransformationConfigs), len(data.ComponentConfigs))

	return data, nil
}

// fetchBucketsWithTables fetches all buckets and their tables.
func (f *Fetcher) fetchBucketsWithTables(ctx context.Context, branchID keboola.BranchID) (buckets []*keboola.Bucket, tables []*keboola.Table, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.fetchBucketsWithTables")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching buckets...")

	// Fetch buckets
	bucketsResult, err := f.api.ListBucketsRequest(branchID).Send(ctx)
	if err != nil {
		return nil, nil, err
	}
	buckets = *bucketsResult

	f.logger.Infof(ctx, "Found %d buckets", len(buckets))

	// Fetch tables for all buckets with column metadata
	f.logger.Info(ctx, "Fetching tables with column metadata...")
	tablesResult, err := f.api.ListTablesRequest(branchID, keboola.WithColumnMetadata()).Send(ctx)
	if err != nil {
		return nil, nil, err
	}
	tables = *tablesResult

	f.logger.Infof(ctx, "Found %d tables", len(tables))

	// Extract column names from ColumnMetadata if Columns is empty
	// (API sometimes returns only columnMetadata without columns array)
	for _, t := range tables {
		if len(t.Columns) == 0 && len(t.ColumnMetadata) > 0 {
			t.Columns = make([]string, 0, len(t.ColumnMetadata))
			for colName := range t.ColumnMetadata {
				t.Columns = append(t.Columns, colName)
			}
			// Sort column names for deterministic output
			sort.Strings(t.Columns)
		}
	}

	// Debug: Log column info for first few tables
	for i, t := range tables {
		if i < 3 {
			f.logger.Debugf(ctx, "Table %s: %d columns, %d column metadata entries",
				t.TableID, len(t.Columns), len(t.ColumnMetadata))
		}
	}

	return buckets, tables, nil
}

// fetchJobsQueue fetches jobs from the Jobs Queue API with detailed result data.
func (f *Fetcher) fetchJobsQueue(ctx context.Context, branchID keboola.BranchID) (jobs []*keboola.QueueJobDetail, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.fetchJobsQueue")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching jobs from Queue API...")

	// Search for jobs in the branch with limit of 100, using detail request to get input/output tables
	jobsResult, err := f.api.SearchJobsDetailRequest(
		keboola.WithSearchJobsBranch(branchID),
		keboola.WithSearchJobsLimit(100),
	).Send(ctx)
	if err != nil {
		return nil, err
	}

	jobs = *jobsResult

	// Log summary of output tables found
	totalOutputTables := 0
	for _, job := range jobs {
		if job.Result.Output != nil {
			totalOutputTables += len(job.Result.Output.Tables)
		}
		// Also count tables from flow tasks
		for _, task := range job.Result.Tasks {
			for _, result := range task.Results {
				if result.Result.Output != nil {
					totalOutputTables += len(result.Result.Output.Tables)
				}
			}
		}
	}

	f.logger.Infof(ctx, "Found %d jobs with %d output tables", len(jobs), totalOutputTables)

	return jobs, nil
}

// componentsResult holds the result of fetchAllComponents.
type componentsResult struct {
	Components       []*keboola.ComponentWithConfigs
	TransformConfigs []*configparser.TransformationConfig
	ComponentConfigs []*configparser.ComponentConfig
}

// TableSample represents a sample of table data.
type TableSample struct {
	TableID keboola.TableID
	Columns []string
	Rows    [][]string
}

// RowCount returns the number of rows in the sample.
func (s *TableSample) RowCount() int {
	return len(s.Rows)
}

// FetchTableSample fetches a sample of data from a table.
func (f *Fetcher) FetchTableSample(ctx context.Context, tableKey keboola.TableKey, limit uint) (sample *TableSample, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchTableSample")
	defer span.End(&err)

	f.logger.Debugf(ctx, "Fetching sample for table %s (limit: %d)", tableKey.TableID, limit)

	// Fetch table preview using the SDK.
	preview, err := f.api.PreviewTableRequest(tableKey, keboola.WithLimitRows(limit)).Send(ctx)
	if err != nil {
		return nil, err
	}

	sample = &TableSample{
		TableID: tableKey.TableID,
		Columns: preview.Columns,
		Rows:    preview.Rows,
	}

	f.logger.Debugf(ctx, "Fetched %d rows for table %s", sample.RowCount(), tableKey.TableID)

	return sample, nil
}

// FetchTableSamples fetches samples for multiple tables concurrently.
func (f *Fetcher) FetchTableSamples(ctx context.Context, tables []*keboola.Table, limit uint, maxTables int) (samples []*TableSample, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchTableSamples")
	defer span.End(&err)

	// Guard against non-positive maxTables to avoid panics from negative slice capacities.
	if maxTables <= 0 {
		return []*TableSample{}, nil
	}

	// Limit tables to fetch.
	tablesToFetch := tables
	if len(tablesToFetch) > maxTables {
		tablesToFetch = tablesToFetch[:maxTables]
	}

	f.logger.Infof(ctx, "Fetching samples for %d tables concurrently (limit: %d rows each)", len(tablesToFetch), limit)

	// Use bounded concurrency to respect API rate limits.
	const maxConcurrency = 5

	type indexedSample struct {
		index  int
		sample *TableSample
	}

	var (
		mu        sync.Mutex
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, maxConcurrency)
		results   = make([]indexedSample, 0, len(tablesToFetch))
	)

	for i, table := range tablesToFetch {
		wg.Add(1)
		go func(idx int, t *keboola.Table) {
			defer wg.Done()

			// Acquire semaphore.
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}

			tableKey := keboola.TableKey{
				BranchID: t.BranchID,
				TableID:  t.TableID,
			}

			sample, fetchErr := f.FetchTableSample(ctx, tableKey, limit)
			if fetchErr != nil {
				f.logger.Warnf(ctx, "Failed to fetch sample for table %s: %v", t.TableID, fetchErr)
				return
			}

			mu.Lock()
			results = append(results, indexedSample{index: idx, sample: sample})
			mu.Unlock()
		}(i, table)
	}

	wg.Wait()

	// Sort by original index to preserve order.
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})

	samples = make([]*TableSample, 0, len(results))
	for _, r := range results {
		samples = append(samples, r.sample)
	}

	f.logger.Infof(ctx, "Fetched samples for %d tables", len(samples))

	return samples, nil
}

// fetchAllComponents fetches all components and extracts transformation and component configs.
// This makes a single API call and returns all data needed for processing.
func (f *Fetcher) fetchAllComponents(ctx context.Context, branchID keboola.BranchID) (result *componentsResult, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.fetchAllComponents")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching all components from API...")

	// Fetch all components with configs (single API call)
	apiResult, err := f.api.ListConfigsAndRowsFrom(keboola.BranchKey{ID: branchID}).Send(ctx)
	if err != nil {
		return nil, err
	}

	result = &componentsResult{
		Components:       make([]*keboola.ComponentWithConfigs, 0, len(*apiResult)),
		TransformConfigs: make([]*configparser.TransformationConfig, 0),
		ComponentConfigs: make([]*configparser.ComponentConfig, 0),
	}

	for _, comp := range *apiResult {
		// Store all components for the registry
		result.Components = append(result.Components, comp)

		// Process transformation components
		if comp.IsTransformation() {
			for _, cfg := range comp.Configs {
				config := f.parser.ParseTransformationConfig(ctx, comp.ID.String(), cfg)
				if config == nil {
					continue
				}
				result.TransformConfigs = append(result.TransformConfigs, config)
			}
			continue
		}

		// Skip internal components for component configs
		if comp.IsScheduler() || comp.IsOrchestrator() || comp.IsVariables() || comp.IsSharedCode() {
			continue
		}

		// Process non-transformation component configs
		for _, cfg := range comp.Configs {
			config := configparser.ParseComponentConfig(comp, cfg)
			if config != nil {
				result.ComponentConfigs = append(result.ComponentConfigs, config)
			}
		}
	}

	f.logger.Infof(ctx, "Found %d components, %d transformation configs, %d component configs",
		len(result.Components), len(result.TransformConfigs), len(result.ComponentConfigs))

	return result, nil
}

// FetchTableLastImporter fetches the component that last imported data to a table.
// It looks at the storage events for the table and finds the latest tableImportDone event.
// Returns the component ID extracted from the event's userAgent field.
func (f *Fetcher) FetchTableLastImporter(ctx context.Context, tableID keboola.TableID) (componentID string, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchTableLastImporter")
	defer span.End(&err)

	// List events for the table using SDK method
	events, err := f.api.ListTableEventsRequest(tableID, keboola.WithTableEventsLimit(50)).Send(ctx)
	if err != nil {
		return "", err
	}

	// Find the latest tableImportDone event
	for _, event := range *events {
		if event.Event == "storage.tableImportDone" {
			// Extract component ID from userAgent
			// Format: "Keboola Storage API PHP Client/14 kds-team.app-custom-python"
			componentID := extractComponentFromUserAgent(event.Context.UserAgent)
			if componentID != "" {
				return componentID, nil
			}
		}
	}

	return "", nil
}

// extractComponentFromUserAgent extracts the component ID from a userAgent string.
// Format: "Keboola Storage API PHP Client/14 kds-team.app-custom-python"
// Returns the last space-separated part which is the component ID.
func extractComponentFromUserAgent(userAgent string) string {
	if userAgent == "" {
		return ""
	}
	parts := strings.Split(userAgent, " ")
	return parts[len(parts)-1]
}
