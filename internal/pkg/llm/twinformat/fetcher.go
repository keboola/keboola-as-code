package twinformat

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"golang.org/x/exp/maps"

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
	debugCount := 0
	for _, comp := range *components {
		// Only process transformation components
		if !comp.IsTransformation() {
			continue
		}

		for _, cfg := range comp.Configs {
			debug := debugCount < 3
			config := f.parseTransformationConfig(comp.ID.String(), cfg, debug, f.logger, ctx)
			if config == nil {
				continue
			}

			configs = append(configs, config)

			// Debug: Log parsing results for first few configs
			if !debug {
				continue
			}

			codeCount := 0
			for _, b := range config.Blocks {
				codeCount += len(b.Codes)
			}
			f.logger.Debugf(ctx, "Transformation %s: %d inputs, %d outputs, %d blocks, %d codes",
				config.Name, len(config.InputTables), len(config.OutputTables), len(config.Blocks), codeCount)
			debugCount++
		}
	}

	f.logger.Infof(ctx, "Found %d transformation configs", len(configs))
	return configs, nil
}

// parseTransformationConfig parses a Config into TransformationConfig.
func (f *Fetcher) parseTransformationConfig(componentID string, cfg *keboola.ConfigWithRows, debug bool, logger log.Logger, ctx context.Context) *TransformationConfig {
	config := &TransformationConfig{
		ID:          cfg.ID.String(),
		Name:        cfg.Name,
		ComponentID: componentID,
		Description: cfg.Description,
		IsDisabled:  cfg.IsDisabled,
		Version:     cfg.Version,
		Created:     cfg.Created.String(),
	}

	if cfg.Content == nil {
		return config
	}

	// Debug: log available keys
	if debug {
		logger.Debugf(ctx, "Config %s keys: %v", cfg.Name, cfg.Content.Keys())
	}

	// Parse storage.input.tables and storage.output.tables
	f.parseStorageSection(config, cfg, debug, logger, ctx)

	// Parse parameters.blocks for transformation code
	f.parseParametersSection(config, cfg, debug, logger, ctx)

	return config
}

// parseStorageSection parses the storage section of a transformation config.
func (f *Fetcher) parseStorageSection(config *TransformationConfig, cfg *keboola.ConfigWithRows, debug bool, logger log.Logger, ctx context.Context) {
	storage, ok := cfg.Content.Get("storage")
	if !ok {
		return
	}

	storageMap := toStringMap(storage)
	if storageMap == nil {
		if debug {
			logger.Debugf(ctx, "Config %s storage type: %T", cfg.Name, storage)
		}
		return
	}

	if debug {
		logger.Debugf(ctx, "Config %s storage keys: %v", cfg.Name, maps.Keys(storageMap))
	}
	config.InputTables = f.parseStorageMappings(storageMap, "input")
	config.OutputTables = f.parseStorageMappings(storageMap, "output")
}

// parseParametersSection parses the parameters section of a transformation config.
func (f *Fetcher) parseParametersSection(config *TransformationConfig, cfg *keboola.ConfigWithRows, debug bool, logger log.Logger, ctx context.Context) {
	params, ok := cfg.Content.Get("parameters")
	if !ok {
		return
	}

	paramsMap := toStringMap(params)
	if paramsMap == nil {
		if debug {
			logger.Debugf(ctx, "Config %s parameters type: %T", cfg.Name, params)
		}
		return
	}

	if debug {
		logger.Debugf(ctx, "Config %s parameters keys: %v", cfg.Name, maps.Keys(paramsMap))
	}
	config.Blocks = f.parseCodeBlocks(paramsMap, debug, logger, ctx)
}

// toStringMap converts various map types to map[string]any.
// Handles both map[string]any and *orderedmap.OrderedMap from the SDK.
func toStringMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	// Handle orderedmap.OrderedMap (used by Keboola SDK)
	if om, ok := v.(interface {
		Keys() []string
		Get(key string) (any, bool)
	}); ok {
		result := make(map[string]any)
		for _, key := range om.Keys() {
			if val, ok := om.Get(key); ok {
				result[key] = val
			}
		}
		return result
	}
	return nil
}

// parseStorageMappings parses input or output table mappings from storage config.
func (f *Fetcher) parseStorageMappings(storage map[string]any, key string) []StorageMapping {
	mappings := make([]StorageMapping, 0)

	if section, ok := storage[key]; ok {
		sectionMap := toStringMap(section)
		if sectionMap != nil {
			if tables, ok := sectionMap["tables"]; ok {
				if tablesSlice, ok := tables.([]any); ok {
					for _, t := range tablesSlice {
						tableMap := toStringMap(t)
						if tableMap != nil {
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
func (f *Fetcher) parseCodeBlocks(params map[string]any, debug bool, logger log.Logger, ctx context.Context) []*CodeBlock {
	blocks := make([]*CodeBlock, 0)

	if blocksRaw, ok := params["blocks"]; ok {
		if blocksSlice, ok := blocksRaw.([]any); ok {
			for _, b := range blocksSlice {
				blockMap := toStringMap(b)
				if blockMap != nil {
					block := &CodeBlock{}
					if name, ok := blockMap["name"].(string); ok {
						block.Name = name
					}
					if debug {
						logger.Debugf(ctx, "Block %s keys: %v", block.Name, maps.Keys(blockMap))
					}

					// Parse codes within the block
					if codesRaw, ok := blockMap["codes"]; ok {
						if codesSlice, ok := codesRaw.([]any); ok {
							for _, c := range codesSlice {
								codeMap := toStringMap(c)
								if codeMap != nil {
									code := &Code{}
									if name, ok := codeMap["name"].(string); ok {
										code.Name = name
									}
									if debug {
										logger.Debugf(ctx, "Code %s keys: %v", code.Name, maps.Keys(codeMap))
									}
									// Script can be in different fields depending on transformation type
									// Handle both string and array formats for "script" field
									if script, ok := codeMap["script"].(string); ok {
										code.Script = script
									} else if scriptSlice, ok := codeMap["script"].([]any); ok {
										// API returns script as array of strings
										var scriptParts []string
										for _, s := range scriptSlice {
											if str, ok := s.(string); ok {
												scriptParts = append(scriptParts, str)
											}
										}
										code.Script = joinScripts(scriptParts)
									} else if scripts, ok := codeMap["scripts"].([]any); ok {
										// Some transformations use "scripts" (plural) field
										var scriptParts []string
										for _, s := range scripts {
											if str, ok := s.(string); ok {
												scriptParts = append(scriptParts, str)
											}
										}
										code.Script = joinScripts(scriptParts)
									}
									if debug {
										logger.Debugf(ctx, "Code %s script length: %d", code.Name, len(code.Script))
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

	// Handle Snowflake/SQL transformations that use "queries" instead of "blocks"
	if queriesRaw, ok := params["queries"]; ok {
		if queriesSlice, ok := queriesRaw.([]any); ok {
			block := &CodeBlock{Name: "queries"}
			for i, q := range queriesSlice {
				if queryStr, ok := q.(string); ok {
					block.Codes = append(block.Codes, &Code{
						Name:   fmt.Sprintf("query_%d", i+1),
						Script: queryStr,
					})
				}
			}
			if len(block.Codes) > 0 {
				blocks = append(blocks, block)
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
