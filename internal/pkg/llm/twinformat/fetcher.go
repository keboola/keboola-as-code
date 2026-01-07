package twinformat

import (
	"context"
	"strings"
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

	// Fetch transformation configs
	transformConfigs, err := f.FetchTransformationConfigs(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch transformation configs: %v", err)
		data.TransformationConfigs = []*TransformationConfig{}
	} else {
		data.TransformationConfigs = transformConfigs
	}

	// Fetch component configs
	componentConfigs, err := f.FetchComponentConfigs(ctx, branchID)
	if err != nil {
		f.logger.Warnf(ctx, "Failed to fetch component configs: %v", err)
		data.ComponentConfigs = []*ComponentConfig{}
	} else {
		data.ComponentConfigs = componentConfigs
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

// FetchTransformationConfigs fetches transformation configurations from the API.
func (f *Fetcher) FetchTransformationConfigs(ctx context.Context, branchID keboola.BranchID) (configs []*TransformationConfig, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchTransformationConfigs")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching transformation configs from API...")

	// Fetch all components with configs
	components, err := f.api.ListConfigsAndRowsFrom(keboola.BranchKey{ID: branchID}).Send(ctx)
	if err != nil {
		return nil, err
	}

	configs = make([]*TransformationConfig, 0)
	for _, comp := range *components {
		// Only process transformation components
		if !comp.IsTransformation() {
			continue
		}

		for _, cfg := range comp.Configs {
			config := f.parseTransformationConfig(comp.ID.String(), cfg)
			if config != nil {
				configs = append(configs, config)
			}
		}
	}

	f.logger.Infof(ctx, "Found %d transformation configs", len(configs))
	return configs, nil
}

// parseTransformationConfig parses a Config into TransformationConfig.
func (f *Fetcher) parseTransformationConfig(componentID string, cfg *keboola.ConfigWithRows) *TransformationConfig {
	config := &TransformationConfig{
		ID:          cfg.ID.String(),
		Name:        cfg.Name,
		ComponentID: componentID,
		Description: cfg.Description,
		IsDisabled:  cfg.IsDisabled,
		Version:     cfg.Version,
		Created:     cfg.Created.String(),
	}

	// Parse configuration content for input/output tables and blocks
	if cfg.Content != nil {
		// Parse storage.input.tables and storage.output.tables
		if storage, ok := cfg.Content.Get("storage"); ok {
			if storageMap, ok := storage.(map[string]any); ok {
				config.InputTables = f.parseStorageMappings(storageMap, "input")
				config.OutputTables = f.parseStorageMappings(storageMap, "output")
			}
		}

		// Parse parameters.blocks for transformation code
		if params, ok := cfg.Content.Get("parameters"); ok {
			if paramsMap, ok := params.(map[string]any); ok {
				config.Blocks = f.parseCodeBlocks(paramsMap)
			}
		}
	}

	return config
}

// parseStorageMappings parses input or output table mappings from storage config.
func (f *Fetcher) parseStorageMappings(storage map[string]any, key string) []StorageMapping {
	mappings := make([]StorageMapping, 0)

	if section, ok := storage[key]; ok {
		if sectionMap, ok := section.(map[string]any); ok {
			if tables, ok := sectionMap["tables"]; ok {
				if tablesSlice, ok := tables.([]any); ok {
					for _, t := range tablesSlice {
						if tableMap, ok := t.(map[string]any); ok {
							mapping := StorageMapping{}
							if src, ok := tableMap["source"].(string); ok {
								mapping.Source = src
							}
							if dst, ok := tableMap["destination"].(string); ok {
								mapping.Destination = dst
							}
							if mapping.Source != "" || mapping.Destination != "" {
								mappings = append(mappings, mapping)
							}
						}
					}
				}
			}
		}
	}

	return mappings
}

// parseCodeBlocks parses code blocks from transformation parameters.
func (f *Fetcher) parseCodeBlocks(params map[string]any) []*CodeBlock {
	blocks := make([]*CodeBlock, 0)

	if blocksRaw, ok := params["blocks"]; ok {
		if blocksSlice, ok := blocksRaw.([]any); ok {
			for _, b := range blocksSlice {
				if blockMap, ok := b.(map[string]any); ok {
					block := &CodeBlock{}
					if name, ok := blockMap["name"].(string); ok {
						block.Name = name
					}

					// Parse codes within the block
					if codesRaw, ok := blockMap["codes"]; ok {
						if codesSlice, ok := codesRaw.([]any); ok {
							for _, c := range codesSlice {
								if codeMap, ok := c.(map[string]any); ok {
									code := &Code{}
									if name, ok := codeMap["name"].(string); ok {
										code.Name = name
									}
									// Script can be in different fields depending on transformation type
									if script, ok := codeMap["script"].(string); ok {
										code.Script = script
									} else if scripts, ok := codeMap["scripts"].([]any); ok {
										// Some transformations use scripts array
										var scriptParts []string
										for _, s := range scripts {
											if str, ok := s.(string); ok {
												scriptParts = append(scriptParts, str)
											}
										}
										code.Script = joinScripts(scriptParts)
									}
									if code.Name != "" || code.Script != "" {
										block.Codes = append(block.Codes, code)
									}
								}
							}
						}
					}

					if block.Name != "" || len(block.Codes) > 0 {
						blocks = append(blocks, block)
					}
				}
			}
		}
	}

	return blocks
}

// joinScripts joins script parts with newlines.
func joinScripts(scripts []string) string {
	var result strings.Builder
	for i, s := range scripts {
		if i > 0 {
			result.WriteString("\n")
		}
		result.WriteString(s)
	}
	return result.String()
}

// FetchComponentConfigs fetches non-transformation component configurations from the API.
func (f *Fetcher) FetchComponentConfigs(ctx context.Context, branchID keboola.BranchID) (configs []*ComponentConfig, err error) {
	ctx, span := f.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.fetcher.FetchComponentConfigs")
	defer span.End(&err)

	f.logger.Info(ctx, "Fetching component configs from API...")

	// Fetch all components with configs
	components, err := f.api.ListConfigsAndRowsFrom(keboola.BranchKey{ID: branchID}).Send(ctx)
	if err != nil {
		return nil, err
	}

	configs = make([]*ComponentConfig, 0)
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
			config := f.parseComponentConfig(comp, cfg)
			if config != nil {
				configs = append(configs, config)
			}
		}
	}

	f.logger.Infof(ctx, "Found %d component configs", len(configs))
	return configs, nil
}

// parseComponentConfig parses a Config into ComponentConfig.
func (f *Fetcher) parseComponentConfig(comp *keboola.ComponentWithConfigs, cfg *keboola.ConfigWithRows) *ComponentConfig {
	config := &ComponentConfig{
		ID:            cfg.ID.String(),
		Name:          cfg.Name,
		ComponentID:   comp.ID.String(),
		ComponentType: comp.Type,
		Description:   cfg.Description,
		IsDisabled:    cfg.IsDisabled,
		Version:       cfg.Version,
		Created:       cfg.Created.String(),
	}

	// Convert configuration content to map
	if cfg.Content != nil {
		config.Configuration = make(map[string]any)
		for _, key := range cfg.Content.Keys() {
			if val, ok := cfg.Content.Get(key); ok {
				config.Configuration[key] = val
			}
		}
	}

	return config
}
