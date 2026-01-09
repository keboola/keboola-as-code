package twinformat

import (
	"context"
	"sort"
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
	projectID keboola.ProjectID
	telemetry telemetry.Telemetry
}

// NewFetcher creates a new Fetcher instance.
func NewFetcher(d FetcherDependencies) *Fetcher {
	return &Fetcher{
		api:       d.KeboolaProjectAPI(),
		logger:    d.Logger(),
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

	// Fetch jobs from Queue API
	data.Jobs, err = f.fetchJobsQueue(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch jobs from Queue API: %v", err)
		data.Jobs = []*keboola.QueueJob{}
	}

	// Fetch transformation configs
	data.TransformationConfigs, err = f.FetchTransformationConfigs(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch transformation configs: %v", err)
		data.TransformationConfigs = []*configparser.TransformationConfig{}
	}

	// Fetch component configs
	data.ComponentConfigs, err = f.FetchComponentConfigs(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch component configs: %v", err)
		data.ComponentConfigs = []*configparser.ComponentConfig{}
	}

	f.logger.Infof(ctx, "Fetched %d buckets, %d tables, %d jobs, %d transformations, %d components",
		len(data.Buckets), len(data.Tables), len(data.Jobs),
		len(data.TransformationConfigs), len(data.ComponentConfigs))

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

// fetchJobsQueue fetches jobs from the Jobs Queue API.
func (f *Fetcher) fetchJobsQueue(ctx context.Context, branchID keboola.BranchID) (jobs []*keboola.QueueJob, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.fetchJobsQueue")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching jobs from Queue API...")

	// Search for jobs in the branch with limit of 100
	jobsResult, err := f.api.SearchJobsRequest(
		keboola.WithSearchJobsBranch(branchID),
		keboola.WithSearchJobsLimit(100),
	).Send(ctx)
	if err != nil {
		return nil, err
	}

	jobs = *jobsResult
	f.logger.Infof(ctx, "Found %d jobs", len(jobs))

	return jobs, nil
}

// FetchTransformationConfigs fetches transformation configurations from the API.
func (f *Fetcher) FetchTransformationConfigs(ctx context.Context, branchID keboola.BranchID) (configs []*configparser.TransformationConfig, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchTransformationConfigs")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching transformation configs from API...")

	// Fetch all components with configs
	components, err := f.api.ListConfigsAndRowsFrom(keboola.BranchKey{ID: branchID}).Send(ctx)
	if err != nil {
		return nil, err
	}

	configs = make([]*configparser.TransformationConfig, 0)
	for _, comp := range *components {
		// Only process transformation components
		if !comp.IsTransformation() {
			continue
		}

		for _, cfg := range comp.Configs {
			config := configparser.ParseTransformationConfig(ctx, comp.ID.String(), cfg, f.logger)
			if config == nil {
				continue
			}

			configs = append(configs, config)
		}
	}

	f.logger.Infof(ctx, "Found %d transformation configs", len(configs))
	return configs, nil
}

// FetchComponentConfigs fetches non-transformation component configurations from the API.
func (f *Fetcher) FetchComponentConfigs(ctx context.Context, branchID keboola.BranchID) (configs []*configparser.ComponentConfig, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchComponentConfigs")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching component configs from API...")

	// Fetch all components with configs
	components, err := f.api.ListConfigsAndRowsFrom(keboola.BranchKey{ID: branchID}).Send(ctx)
	if err != nil {
		return nil, err
	}

	configs = make([]*configparser.ComponentConfig, 0)
	for _, comp := range *components {
		// Skip transformation components (handled separately)
		if comp.IsTransformation() {
			continue
		}

		// Skip internal components
		if comp.IsScheduler() || comp.IsOrchestrator() || comp.IsVariables() || comp.IsSharedCode() {
			continue
		}

		for _, cfg := range comp.Configs {
			config := configparser.ParseComponentConfig(comp, cfg)
			if config == nil {
				continue
			}
			configs = append(configs, config)
		}
	}

	f.logger.Infof(ctx, "Found %d component configs", len(configs))
	return configs, nil
}
