package twinformat

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat/configparser"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// maxAPIConcurrency bounds the concurrent per-table API requests to respect API rate limits.
const maxAPIConcurrency = 10

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
func (f *Fetcher) fetchBucketsWithTables(ctx context.Context, branchID keboola.BranchID) (buckets []*keboola.Bucket, tables []*keboola.TableListV2Item, err error) {
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

	// Fetch tables with columns and the native definition (column types and descriptions)
	f.logger.Info(ctx, "Fetching tables with columns and definitions...")
	tables, err = f.api.ListTablesV2(ctx, branchID, keboola.WithTableListV2Columns(), keboola.WithTableListV2Definition())
	if err != nil {
		return nil, nil, err
	}

	f.logger.Infof(ctx, "Found %d tables", len(tables))

	if err := f.loadMissingDefinitions(ctx, tables); err != nil {
		return nil, nil, err
	}

	return buckets, tables, nil
}

// loadMissingDefinitions reads the table detail of each typed table the listing returns without a definition:
// the listing omits it while the column types of the table are not all native.
//
//   - An untyped table has no native column types at all, so its listing definition (descriptions only) is kept.
//   - A table whose detail cannot be read stays without a definition and is exported without column types:
//     one unreadable table does not fail the export. A cancelled context does.
func (f *Fetcher) loadMissingDefinitions(ctx context.Context, tables []*keboola.TableListV2Item) error {
	sem := semaphore.NewWeighted(maxAPIConcurrency)
	group, groupCtx := errgroup.WithContext(ctx)
	for _, table := range tables {
		if !table.IsTyped || table.Definition != nil {
			continue
		}
		group.Go(func() error {
			if err := sem.Acquire(groupCtx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			return f.loadDefinition(groupCtx, table)
		})
	}
	return group.Wait()
}

// loadDefinition sets the definition of the table from its table detail.
func (f *Fetcher) loadDefinition(ctx context.Context, table *keboola.TableListV2Item) error {
	detail, err := f.api.GetTableRequest(table.TableKey).Send(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		f.logger.Warnf(ctx, "Cannot load the definition of table %s, exporting it without column types: %s", table.TableID, err)
		return nil
	}
	table.Definition = detail.Definition
	return nil
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
	// Don't pass WithLimitRows when limit is 0, as it may have different semantics than omitting.
	var preview *keboola.TablePreview
	if limit == 0 {
		preview, err = f.api.PreviewTableRequest(tableKey).Send(ctx)
	} else {
		preview, err = f.api.PreviewTableRequest(tableKey, keboola.WithLimitRows(limit)).Send(ctx)
	}
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

// indexedSample pairs a sample with its original index for ordering.
type indexedSample struct {
	index  int
	sample *TableSample
}

// sampleFetchCollector collects results from concurrent sample fetching.
type sampleFetchCollector struct {
	mu          sync.Mutex
	results     []indexedSample
	failedCount int
}

// recordFailure increments the failure count.
func (c *sampleFetchCollector) recordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failedCount++
}

// recordSuccess records a successfully fetched sample.
func (c *sampleFetchCollector) recordSuccess(idx int, sample *TableSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results = append(c.results, indexedSample{index: idx, sample: sample})
}

// FetchTableSamples fetches samples for multiple tables concurrently.
func (f *Fetcher) FetchTableSamples(ctx context.Context, tables []*keboola.TableListV2Item, limit uint, maxTables int) (samples []*TableSample, err error) {
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

	sem := semaphore.NewWeighted(maxAPIConcurrency)
	group, groupCtx := errgroup.WithContext(ctx)
	collector := &sampleFetchCollector{
		results: make([]indexedSample, 0, len(tablesToFetch)),
	}

	for i, table := range tablesToFetch {
		idx, t := i, table
		group.Go(func() error {
			// Acquire semaphore - blocks until slot available or context cancelled.
			if err := sem.Acquire(groupCtx, 1); err != nil {
				collector.recordFailure()
				return nil // Don't propagate - we want partial results
			}
			defer sem.Release(1)

			f.fetchTableSample(groupCtx, collector, idx, t, limit)
			return nil
		})
	}

	// Wait for all goroutines to complete.
	_ = group.Wait()

	// If context was cancelled and no samples were fetched, propagate the cancellation error.
	if len(collector.results) == 0 && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Sort by original index to preserve order.
	sort.Slice(collector.results, func(i, j int) bool {
		return collector.results[i].index < collector.results[j].index
	})

	samples = make([]*TableSample, 0, len(collector.results))
	for _, r := range collector.results {
		samples = append(samples, r.sample)
	}

	f.logger.Infof(ctx, "Fetched samples for %d tables (%d failed)", len(samples), collector.failedCount)

	// Return error if any tables failed to fetch, but still return partial results.
	if collector.failedCount > 0 {
		return samples, errors.Errorf("failed to fetch samples for %d of %d tables", collector.failedCount, len(tablesToFetch))
	}

	return samples, nil
}

// fetchTableSample fetches a sample for a single table and records the result.
func (f *Fetcher) fetchTableSample(ctx context.Context, collector *sampleFetchCollector, idx int, t *keboola.TableListV2Item, limit uint) {
	sample, err := f.FetchTableSample(ctx, t.TableKey, limit)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch sample for table %s: %v", t.TableID, err)
		collector.recordFailure()
		return
	}

	collector.recordSuccess(idx, sample)
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
