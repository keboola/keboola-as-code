package twinformat

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// FetcherDependencies defines the dependencies required by the Fetcher.
type FetcherDependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	StorageAPIHost() string
	StorageAPIToken() keboola.Token
	Telemetry() telemetry.Telemetry
}

// Fetcher retrieves project data from Keboola APIs.
type Fetcher struct {
	api       *keboola.AuthorizedAPI
	logger    log.Logger
	projectID keboola.ProjectID
	host      string
	token     keboola.Token
	telemetry telemetry.Telemetry
}

// NewFetcher creates a new Fetcher instance.
func NewFetcher(d FetcherDependencies) *Fetcher {
	return &Fetcher{
		api:       d.KeboolaProjectAPI(),
		logger:    d.Logger(),
		projectID: d.ProjectID(),
		host:      d.StorageAPIHost(),
		token:     d.StorageAPIToken(),
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
		Host:      f.host,
		Token:     f.token,
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
	jobs, err := f.fetchJobsQueue(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch jobs from Queue API: %v", err)
		data.Jobs = []*keboola.QueueJob{}
	} else {
		data.Jobs = jobs
	}

	f.logger.Infof(ctx, "Fetched %d buckets, %d tables, %d jobs", len(data.Buckets), len(data.Tables), len(data.Jobs))

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

	// Fetch tables for all buckets
	f.logger.Info(ctx, "Fetching tables...")
	tablesResult, err := f.api.ListTablesRequest(branchID).Send(ctx)
	if err != nil {
		return nil, nil, err
	}
	tables = *tablesResult

	f.logger.Infof(ctx, "Found %d tables", len(tables))

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

// TableSample represents a sample of table data.
type TableSample struct {
	TableID  keboola.TableID
	Columns  []string
	Rows     [][]string
	RowCount int
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
		TableID:  tableKey.TableID,
		Columns:  preview.Columns,
		Rows:     preview.Rows,
		RowCount: len(preview.Rows),
	}

	f.logger.Debugf(ctx, "Fetched %d rows for table %s", sample.RowCount, tableKey.TableID)

	return sample, nil
}

// FetchTableSamples fetches samples for multiple tables.
func (f *Fetcher) FetchTableSamples(ctx context.Context, tables []*keboola.Table, branchID keboola.BranchID, limit uint, maxTables int) (samples []*TableSample, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchTableSamples")
	defer span.End(&err)

	f.logger.Infof(ctx, "Fetching samples for up to %d tables (limit: %d rows each)", maxTables, limit)

	samples = make([]*TableSample, 0, maxTables)
	count := 0

	for _, table := range tables {
		if count >= maxTables {
			break
		}

		tableKey := keboola.TableKey{
			BranchID: branchID,
			TableID:  table.TableID,
		}

		sample, err := f.FetchTableSample(ctx, tableKey, limit)
		if err != nil {
			f.logger.Warnf(ctx, "Failed to fetch sample for table %s: %v", table.TableID, err)
			continue
		}

		samples = append(samples, sample)
		count++
	}

	f.logger.Infof(ctx, "Fetched samples for %d tables", len(samples))

	return samples, nil
}
