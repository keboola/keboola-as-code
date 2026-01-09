package twinformat

import (
	"context"
	"sort"
	"strings"
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

	// Fetch jobs from Queue API
	data.Jobs, err = f.fetchJobsQueue(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch jobs from Queue API: %v", err)
		data.Jobs = []*keboola.QueueJob{}
	}

	// Fetch all components with configs (single API call)
	components, transformConfigs, componentConfigs, err := f.fetchAllComponents(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch components: %v", err)
		data.Components = []*keboola.ComponentWithConfigs{}
		data.TransformationConfigs = []*configparser.TransformationConfig{}
		data.ComponentConfigs = []*configparser.ComponentConfig{}
	} else {
		data.Components = components
		data.TransformationConfigs = transformConfigs
		data.ComponentConfigs = componentConfigs
	}

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

// fetchAllComponents fetches all components and extracts transformation and component configs.
// This makes a single API call and returns all data needed for processing.
func (f *Fetcher) fetchAllComponents(ctx context.Context, branchID keboola.BranchID) (
	components []*keboola.ComponentWithConfigs,
	transformConfigs []*configparser.TransformationConfig,
	componentConfigs []*configparser.ComponentConfig,
	err error,
) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.fetchAllComponents")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching all components from API...")

	// Fetch all components with configs (single API call)
	result, err := f.api.ListConfigsAndRowsFrom(keboola.BranchKey{ID: branchID}).Send(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	components = make([]*keboola.ComponentWithConfigs, 0, len(*result))
	transformConfigs = make([]*configparser.TransformationConfig, 0)
	componentConfigs = make([]*configparser.ComponentConfig, 0)

	for _, comp := range *result {
		// Store all components for the registry
		components = append(components, comp)

		// Process transformation components
		if comp.IsTransformation() {
			for _, cfg := range comp.Configs {
				config := f.parser.ParseTransformationConfig(ctx, comp.ID.String(), cfg)
				if config == nil {
					continue
				}
				transformConfigs = append(transformConfigs, config)
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
				componentConfigs = append(componentConfigs, config)
			}
		}
	}

	f.logger.Infof(ctx, "Found %d components, %d transformation configs, %d component configs",
		len(components), len(transformConfigs), len(componentConfigs))

	return components, transformConfigs, componentConfigs, nil
}

// FetchTransformationConfigs fetches transformation configurations from the API.
// Deprecated: Use FetchAll which fetches everything in a single call.
func (f *Fetcher) FetchTransformationConfigs(ctx context.Context, branchID keboola.BranchID) (configs []*configparser.TransformationConfig, err error) {
	_, configs, _, err = f.fetchAllComponents(ctx, branchID)
	return configs, err
}

// FetchComponentConfigs fetches non-transformation component configurations from the API.
// Deprecated: Use FetchAll which fetches everything in a single call.
func (f *Fetcher) FetchComponentConfigs(ctx context.Context, branchID keboola.BranchID) (configs []*configparser.ComponentConfig, err error) {
	_, _, configs, err = f.fetchAllComponents(ctx, branchID)
	return configs, err
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
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}
