//nolint:tagliatelle // RFC specifies snake_case for JSON output in twin format.
package twinformat

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/timeutils"
)

// Version represents a format version number.
type Version int

const (
	// TwinFormatVersion is the version of the twin format specification.
	TwinFormatVersion Version = 1
	// FormatVersion is the version of the output format.
	FormatVersion Version = 2
)

// GeneratorDependencies defines the dependencies for the Generator.
type GeneratorDependencies interface {
	Logger() log.Logger
	Fs() filesystem.Fs
}

// Generator generates the twin format output directory structure.
type Generator struct {
	logger      log.Logger
	fs          filesystem.Fs
	jsonWriter  *writer.JSONWriter
	jsonlWriter *writer.JSONLWriter
	mdWriter    *writer.MarkdownWriter
	outputDir   string
}

// NewGenerator creates a new Generator.
func NewGenerator(d GeneratorDependencies, outputDir string) *Generator {
	fs := d.Fs()
	return &Generator{
		logger:      d.Logger(),
		fs:          fs,
		jsonWriter:  writer.NewJSONWriter(fs),
		jsonlWriter: writer.NewJSONLWriter(fs),
		mdWriter:    writer.NewMarkdownWriter(fs),
		outputDir:   outputDir,
	}
}

// Generate generates the complete twin format output from processed data.
func (g *Generator) Generate(ctx context.Context, data *ProcessedData) error {
	g.logger.Infof(ctx, "Generating twin format output to %s", g.outputDir)

	// Create output directory structure.
	if err := g.createDirectories(ctx); err != nil {
		return errors.Errorf("failed to create directories: %w", err)
	}

	// Generate all output files.
	if err := g.generateBuckets(ctx, data); err != nil {
		return errors.Errorf("failed to generate buckets: %w", err)
	}

	if err := g.generateTransformations(ctx, data); err != nil {
		return errors.Errorf("failed to generate transformations: %w", err)
	}

	if err := g.generateJobs(ctx, data); err != nil {
		return errors.Errorf("failed to generate jobs: %w", err)
	}

	if err := g.generateComponents(ctx, data); err != nil {
		return errors.Errorf("failed to generate components: %w", err)
	}

	if err := g.generateIndices(ctx, data); err != nil {
		return errors.Errorf("failed to generate indices: %w", err)
	}

	if err := g.generateRootFiles(ctx, data); err != nil {
		return errors.Errorf("failed to generate root files: %w", err)
	}

	if err := g.generateAIGuide(ctx, data); err != nil {
		return errors.Errorf("failed to generate AI guide: %w", err)
	}

	g.logger.Infof(ctx, "Twin format output generated successfully")
	return nil
}

// createDirectories creates the output directory structure.
func (g *Generator) createDirectories(ctx context.Context) error {
	dirs := []string{
		g.outputDir,
		filesystem.Join(g.outputDir, "buckets"),
		filesystem.Join(g.outputDir, "transformations"),
		filesystem.Join(g.outputDir, "components"),
		filesystem.Join(g.outputDir, "jobs"),
		filesystem.Join(g.outputDir, "jobs", "recent"),
		filesystem.Join(g.outputDir, "jobs", "by-component"),
		filesystem.Join(g.outputDir, "indices"),
		filesystem.Join(g.outputDir, "indices", "queries"),
		filesystem.Join(g.outputDir, "ai"),
	}

	for _, dir := range dirs {
		if err := g.fs.Mkdir(ctx, dir); err != nil {
			return errors.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// generateBuckets generates buckets/index.json and per-table metadata files.
func (g *Generator) generateBuckets(ctx context.Context, data *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating buckets index and table metadata")

	// Build bucket index.
	bucketIndex := g.buildBucketIndex(data)

	// Write buckets/index.json.
	indexPath := filesystem.Join(g.outputDir, "buckets", "index.json")
	if err := g.jsonWriter.Write(ctx, indexPath, bucketIndex); err != nil {
		return err
	}

	// Generate per-table metadata files.
	for _, table := range data.Tables {
		if err := g.generateTableMetadata(ctx, table); err != nil {
			return err
		}
	}

	return nil
}

// buildBucketIndex builds the bucket index structure.
func (g *Generator) buildBucketIndex(data *ProcessedData) map[string]any {
	docFields := BucketsIndexDocFields()

	// Group tables by bucket.
	bucketTables := make(map[string][]string)
	for _, table := range data.Tables {
		bucketTables[table.BucketName] = append(bucketTables[table.BucketName], table.Name)
	}

	// Build by_source stats using typed counters.
	type sourceStats struct {
		count       int
		totalTables int
	}
	bySourceStats := make(map[string]*sourceStats)
	for _, bucket := range data.Buckets {
		source := bucket.Source
		stats, ok := bySourceStats[source]
		if !ok {
			stats = &sourceStats{}
			bySourceStats[source] = stats
		}
		stats.count++
		stats.totalTables += bucket.TableCount
	}

	// Convert typed stats to the generic map structure for JSON output.
	bySource := make(map[string]map[string]any, len(bySourceStats))
	for source, stats := range bySourceStats {
		bySource[source] = map[string]any{
			"count":        stats.count,
			"total_tables": stats.totalTables,
		}
	}

	// Build buckets list.
	buckets := make([]map[string]any, 0, len(data.Buckets))
	for _, bucket := range data.Buckets {
		// Extract bucket name from BucketID.
		bucketName := extractBucketName(bucket.BucketID.String())
		tables := bucketTables[bucketName]
		if tables == nil {
			tables = []string{}
		}
		buckets = append(buckets, map[string]any{
			"name":        bucketName,
			"source":      bucket.Source,
			"table_count": bucket.TableCount,
			"tables":      tables,
		})
	}

	return map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"total_buckets":     len(data.Buckets),
		"by_source":         bySource,
		"buckets":           buckets,
	}
}

// generateTableMetadata generates a table metadata.json file.
func (g *Generator) generateTableMetadata(ctx context.Context, table *ProcessedTable) error {
	// Create bucket/tables directory structure.
	tableDir := filesystem.Join(g.outputDir, "buckets", table.BucketName, "tables", table.Name)
	if err := g.fs.Mkdir(ctx, tableDir); err != nil {
		return errors.Errorf("failed to create table directory %s: %w", tableDir, err)
	}

	docFields := TableMetadataDocFields(table.Source)

	metadata := map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"uid":               table.UID,
		"name":              table.Name,
		"type":              "table",
		"bucket":            table.BucketName,
		"source":            table.Source,
	}

	if table.DisplayName != "" {
		metadata["description"] = table.DisplayName
	}

	if len(table.Columns) > 0 {
		columns := g.buildColumnDetails(table)
		metadata["columns"] = columns
	}

	if len(table.PrimaryKey) > 0 {
		metadata["primary_key"] = table.PrimaryKey
	}

	if table.RowsCount > 0 {
		metadata["rows_count"] = table.RowsCount
	}

	if table.DataSizeBytes > 0 {
		metadata["data_size_bytes"] = table.DataSizeBytes
	}

	// Get dependencies from the Dependencies field.
	consumedBy := []string{}
	producedBy := []string{}
	if table.Dependencies != nil {
		consumedBy = table.Dependencies.ConsumedBy
		producedBy = table.Dependencies.ProducedBy
	}
	if consumedBy == nil {
		consumedBy = []string{}
	}
	if producedBy == nil {
		producedBy = []string{}
	}

	metadata["dependencies"] = map[string]any{
		"consumed_by": consumedBy,
		"produced_by": producedBy,
	}

	metadataPath := filesystem.Join(tableDir, "metadata.json")
	return g.jsonWriter.Write(ctx, metadataPath, metadata)
}

// buildColumnDetails builds detailed column information including metadata.
func (g *Generator) buildColumnDetails(table *ProcessedTable) []map[string]any {
	columns := make([]map[string]any, 0, len(table.Columns))

	for _, colName := range table.Columns {
		col := map[string]any{
			"name": colName,
		}

		// Extract column metadata if available
		if table.Table != nil && table.ColumnMetadata != nil {
			if colMeta, ok := table.ColumnMetadata[colName]; ok {
				for _, meta := range colMeta {
					switch meta.Key {
					case "KBC.datatype.basetype":
						col["base_type"] = meta.Value
					case "KBC.datatype.type":
						col["type"] = meta.Value
					case "KBC.datatype.nullable":
						col["nullable"] = meta.Value == "1" || meta.Value == "true"
					case "KBC.datatype.length":
						col["length"] = meta.Value
					case "KBC.description":
						col["description"] = meta.Value
					}
				}
			}
		}

		columns = append(columns, col)
	}

	return columns
}

// generateTransformations generates transformations/index.json and per-transformation metadata files.
func (g *Generator) generateTransformations(ctx context.Context, data *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating transformations index and metadata")

	// Build transformation index.
	transformIndex := g.buildTransformationIndex(data)

	// Write transformations/index.json.
	indexPath := filesystem.Join(g.outputDir, "transformations", "index.json")
	if err := g.jsonWriter.Write(ctx, indexPath, transformIndex); err != nil {
		return err
	}

	// Generate per-transformation metadata files.
	for _, transform := range data.Transformations {
		if err := g.generateTransformationMetadata(ctx, transform); err != nil {
			return err
		}
	}

	return nil
}

// buildTransformationIndex builds the transformation index structure.
func (g *Generator) buildTransformationIndex(data *ProcessedData) map[string]any {
	docFields := TransformationsIndexDocFields()

	// Count by platform.
	byPlatform := make(map[string]int)
	for _, transform := range data.Transformations {
		byPlatform[transform.Platform]++
	}

	// Build transformations list.
	transformations := make([]map[string]any, 0, len(data.Transformations))
	for _, transform := range data.Transformations {
		// Get input/output counts from dependencies.
		inputCount := 0
		outputCount := 0
		if transform.Dependencies != nil {
			inputCount = len(transform.Dependencies.Consumes)
			outputCount = len(transform.Dependencies.Produces)
		}

		entry := map[string]any{
			"uid":          transform.UID,
			"name":         transform.Name,
			"platform":     transform.Platform,
			"is_disabled":  transform.IsDisabled,
			"input_count":  inputCount,
			"output_count": outputCount,
		}

		if transform.JobExecution != nil {
			if transform.JobExecution.LastRunTime != "" {
				entry["last_run_time"] = transform.JobExecution.LastRunTime
			}
			if transform.JobExecution.LastRunStatus != "" {
				entry["last_run_status"] = transform.JobExecution.LastRunStatus
			}
			if transform.JobExecution.JobReference != "" {
				entry["job_reference"] = transform.JobExecution.JobReference
			}
		}

		transformations = append(transformations, entry)
	}

	return map[string]any{
		"_comment":              docFields.Comment,
		"_purpose":              docFields.Purpose,
		"_update_frequency":     docFields.UpdateFrequency,
		"total_transformations": len(data.Transformations),
		"by_platform":           byPlatform,
		"transformations":       transformations,
	}
}

// generateTransformationMetadata generates a transformation metadata.json file.
func (g *Generator) generateTransformationMetadata(ctx context.Context, transform *ProcessedTransformation) error {
	// Create transformation directory with sanitized name for filesystem safety.
	transformDir := filesystem.Join(g.outputDir, "transformations", strhelper.SanitizeFilename(transform.Name))
	if err := g.fs.Mkdir(ctx, transformDir); err != nil {
		return errors.Errorf("failed to create transformation directory %s: %w", transformDir, err)
	}

	docFields := TransformationMetadataDocFields()

	metadata := map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"uid":               transform.UID,
		"name":              transform.Name,
		"type":              "transformation",
		"platform":          transform.Platform,
		"component_id":      transform.ComponentID,
		"is_disabled":       transform.IsDisabled,
	}

	if transform.ConfigID != "" {
		metadata["config_id"] = transform.ConfigID
	}

	if transform.Description != "" {
		metadata["description"] = transform.Description
	}

	if transform.Path != "" {
		metadata["original_path"] = transform.Path
	}

	// Get dependencies from the Dependencies field.
	consumes := []string{}
	produces := []string{}
	if transform.Dependencies != nil {
		consumes = transform.Dependencies.Consumes
		produces = transform.Dependencies.Produces
	}
	if consumes == nil {
		consumes = []string{}
	}
	if produces == nil {
		produces = []string{}
	}

	metadata["dependencies"] = map[string]any{
		"consumes": consumes,
		"produces": produces,
	}

	// Add job execution info if available.
	if transform.JobExecution != nil {
		metadata["job_execution"] = buildJobExecutionMap(transform.JobExecution)
	}

	// Add code block summary to metadata
	if len(transform.CodeBlocks) > 0 {
		codeInfo := make([]map[string]any, 0, len(transform.CodeBlocks))
		for _, block := range transform.CodeBlocks {
			blockInfo := map[string]any{
				"name":       block.Name,
				"language":   block.Language,
				"code_count": len(block.Codes),
			}
			codeInfo = append(codeInfo, blockInfo)
		}
		metadata["code_blocks"] = codeInfo
	}

	metadataPath := filesystem.Join(transformDir, "metadata.json")
	if err := g.jsonWriter.Write(ctx, metadataPath, metadata); err != nil {
		return err
	}

	// Generate code files if code blocks exist
	if len(transform.CodeBlocks) > 0 {
		if err := g.generateTransformationCode(ctx, transformDir, transform); err != nil {
			return err
		}
	}

	return nil
}

// generateTransformationCode writes transformation code blocks to files.
func (g *Generator) generateTransformationCode(ctx context.Context, transformDir string, transform *ProcessedTransformation) error {
	codeDir := filesystem.Join(transformDir, "code")
	if err := g.fs.Mkdir(ctx, codeDir); err != nil {
		return errors.Errorf("failed to create code directory %s: %w", codeDir, err)
	}

	blockNum := 0
	for _, block := range transform.CodeBlocks {
		for _, code := range block.Codes {
			blockNum++
			// Create filename: 01-block-name.sql
			ext := languageToExtension(block.Language)
			filename := fmt.Sprintf("%02d-%s%s", blockNum, strhelper.SanitizeFilename(code.Name), ext)
			filePath := filesystem.Join(codeDir, filename)

			if err := g.mdWriter.Write(ctx, filePath, code.Script); err != nil {
				return errors.Errorf("failed to write code file %s: %w", filePath, err)
			}
		}
	}

	return nil
}

// languageToExtension converts a language to a file extension.
func languageToExtension(language string) string {
	switch language {
	case PlatformPython:
		return ".py"
	case PlatformR:
		return ".r"
	default:
		return ".sql"
	}
}

// buildJobExecutionMap builds a map of job execution info for metadata.
func buildJobExecutionMap(exec *JobExecution) map[string]any {
	result := map[string]any{}
	if exec.LastRunTime != "" {
		result["last_run_time"] = exec.LastRunTime
	}
	if exec.LastRunStatus != "" {
		result["last_run_status"] = exec.LastRunStatus
	}
	if exec.JobReference != "" {
		result["job_reference"] = exec.JobReference
	}
	if exec.DurationSeconds > 0 {
		result["duration_seconds"] = exec.DurationSeconds
	}
	if exec.LastError != "" {
		result["last_error"] = exec.LastError
	}
	return result
}

// generateJobs generates jobs/index.json and job detail files.
func (g *Generator) generateJobs(ctx context.Context, data *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating jobs index and details")

	// Build jobs index.
	jobsIndex := g.buildJobsIndex(data)

	// Write jobs/index.json.
	indexPath := filesystem.Join(g.outputDir, "jobs", "index.json")
	if err := g.jsonWriter.Write(ctx, indexPath, jobsIndex); err != nil {
		return err
	}

	// Generate recent job files.
	for _, job := range data.Jobs {
		if err := g.generateJobFile(ctx, job); err != nil {
			return err
		}
	}

	return nil
}

// buildJobsIndex builds the jobs index structure.
func (g *Generator) buildJobsIndex(data *ProcessedData) map[string]any {
	docFields := JobsIndexDocFields()

	// Count by status and operation.
	byStatus := make(map[string]int)
	byOperation := make(map[string]int)
	transformationRuns := 0
	byPlatform := make(map[string]int)
	recentTransformations := make([]map[string]any, 0)

	for _, job := range data.Jobs {
		byStatus[job.Status]++
		if job.OperationName != "" {
			byOperation[job.OperationName]++
		}

		// Track transformation runs.
		if job.OperationName == "transformationRun" {
			transformationRuns++
			platform := DetectPlatform(job.ComponentID.String())
			byPlatform[platform]++

			recentTransformations = append(recentTransformations, map[string]any{
				"job_id":           job.ID.String(),
				"transformation":   job.ConfigID.String(),
				"component_id":     job.ComponentID.String(),
				"status":           job.Status,
				"completed_time":   timeutils.FormatISO8601Ptr(job.EndTime),
				"duration_seconds": job.DurationSeconds,
			})
		}
	}

	return map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"total_jobs":        len(data.Jobs),
		"recent_jobs_count": len(data.Jobs),
		"by_status":         byStatus,
		"by_operation":      byOperation,
		"transformations": map[string]any{
			"_comment":               "Track transformation executions separately",
			"total_runs":             transformationRuns,
			"by_platform":            byPlatform,
			"recent_transformations": recentTransformations,
		},
		"retention_policy": map[string]any{
			"recent_jobs":  "Last 100 jobs",
			"by_component": "Latest job per component configuration",
		},
	}
}

// generateJobFile generates a job detail file.
func (g *Generator) generateJobFile(ctx context.Context, job *ProcessedJob) error {
	docFields := JobMetadataDocFields()

	jobID := job.ID.String()
	componentID := job.ComponentID.String()
	configID := job.ConfigID.String()

	jobData := map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"id":                jobID,
		"status":            job.Status,
		"component_id":      componentID,
	}

	if configID != "" {
		jobData["config_id"] = configID
	}
	if job.OperationName != "" {
		jobData["operation_name"] = job.OperationName
	}
	if job.StartTime != nil {
		jobData["start_time"] = timeutils.FormatISO8601Ptr(job.StartTime)
	}
	if job.EndTime != nil {
		jobData["end_time"] = timeutils.FormatISO8601Ptr(job.EndTime)
	}
	if job.DurationSeconds > 0 {
		jobData["duration_seconds"] = job.DurationSeconds
	}
	if job.Result.Message != "" {
		jobData["error_message"] = job.Result.Message
	}

	// Write to jobs/recent/{job_id}.json.
	recentPath := filesystem.Join(g.outputDir, "jobs", "recent", fmt.Sprintf("%s.json", jobID))
	if err := g.jsonWriter.Write(ctx, recentPath, jobData); err != nil {
		return err
	}

	// Also write to jobs/by-component/{component_id}/{config_id}/latest.json if applicable.
	if componentID != "" && configID != "" {
		byComponentDir := filesystem.Join(g.outputDir, "jobs", "by-component", componentID, configID)
		if err := g.fs.Mkdir(ctx, byComponentDir); err != nil {
			return errors.Errorf("failed to create by-component directory %s: %w", byComponentDir, err)
		}

		latestPath := filesystem.Join(byComponentDir, "latest.json")
		if err := g.jsonWriter.Write(ctx, latestPath, jobData); err != nil {
			return err
		}
	}

	return nil
}

// generateComponents generates components/index.json and per-component config files.
func (g *Generator) generateComponents(ctx context.Context, data *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating components index and configs")

	if len(data.ComponentConfigs) == 0 {
		g.logger.Debugf(ctx, "No component configs to generate")
		return nil
	}

	// Build components index
	componentsIndex := g.buildComponentsIndex(data)

	// Write components/index.json
	indexPath := filesystem.Join(g.outputDir, "components", "index.json")
	if err := g.jsonWriter.Write(ctx, indexPath, componentsIndex); err != nil {
		return err
	}

	// Generate per-component config files
	for _, config := range data.ComponentConfigs {
		if err := g.generateComponentConfig(ctx, config); err != nil {
			return err
		}
	}

	return nil
}

// buildComponentsIndex builds the components index structure.
func (g *Generator) buildComponentsIndex(data *ProcessedData) map[string]any {
	// Count by component type
	byType := make(map[string]int)
	for _, config := range data.ComponentConfigs {
		byType[config.ComponentType]++
	}

	// Build components list
	components := make([]map[string]any, 0, len(data.ComponentConfigs))
	for _, config := range data.ComponentConfigs {
		entry := map[string]any{
			"id":             config.ID,
			"name":           config.Name,
			"component_id":   config.ComponentID,
			"component_type": config.ComponentType,
			"is_disabled":    config.IsDisabled,
		}
		if config.Description != "" {
			entry["description"] = config.Description
		}
		if config.LastRun != "" {
			entry["last_run"] = config.LastRun
		}
		if config.Status != "" {
			entry["status"] = config.Status
		}
		components = append(components, entry)
	}

	docFields := ComponentsIndexDocFields()
	return map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"total_components":  len(data.ComponentConfigs),
		"by_type":           byType,
		"components":        components,
	}
}

// generateComponentConfig generates a component config.json file.
func (g *Generator) generateComponentConfig(ctx context.Context, config *ComponentConfig) error {
	// Create component directory structure: components/{component_id}/{config_id}/
	configDir := filesystem.Join(g.outputDir, "components", config.ComponentID, config.ID)
	if err := g.fs.Mkdir(ctx, configDir); err != nil {
		return errors.Errorf("failed to create component config directory %s: %w", configDir, err)
	}

	docFields := ComponentMetadataDocFields()
	configData := map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"id":                config.ID,
		"name":              config.Name,
		"component_id":      config.ComponentID,
		"component_type":    config.ComponentType,
		"is_disabled":       config.IsDisabled,
		"version":           config.Version,
	}

	if config.Description != "" {
		configData["description"] = config.Description
	}
	if config.Created != "" {
		configData["created"] = config.Created
	}
	if config.LastRun != "" {
		configData["last_run"] = config.LastRun
	}
	if config.Status != "" {
		configData["status"] = config.Status
	}
	if config.Configuration != nil {
		configData["configuration"] = config.Configuration
	}

	configPath := filesystem.Join(configDir, "config.json")
	return g.jsonWriter.Write(ctx, configPath, configData)
}

// generateIndices generates indices/graph.jsonl, sources.json, and query files.
func (g *Generator) generateIndices(ctx context.Context, data *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating indices")

	// Generate lineage graph.
	if err := g.generateLineageGraph(ctx, data); err != nil {
		return err
	}

	// Generate sources index.
	if err := g.generateSourcesIndex(ctx, data); err != nil {
		return err
	}

	// Generate query files.
	if err := g.generateQueryFiles(ctx, data); err != nil {
		return err
	}

	return nil
}

// generateLineageGraph generates indices/graph.jsonl.
func (g *Generator) generateLineageGraph(ctx context.Context, data *ProcessedData) error {
	// Build meta object.
	meta := map[string]any{
		"_meta": map[string]any{
			"total_edges":           data.Statistics.TotalEdges,
			"total_nodes":           data.Statistics.TotalTables + data.Statistics.TotalTransformations,
			"total_tables":          data.Statistics.TotalTables,
			"total_transformations": data.Statistics.TotalTransformations,
			"sources":               len(data.Statistics.BySource),
			"updated":               time.Now().UTC().Format(time.RFC3339),
		},
	}

	// Build edge items from the lineage graph.
	edges := make([]*LineageEdge, 0)
	if data.LineageGraph != nil {
		edges = data.LineageGraph.Edges
	}

	items := make([]any, 0, len(edges))
	for _, edge := range edges {
		items = append(items, map[string]any{
			"source": edge.Source,
			"target": edge.Target,
			"type":   edge.Type,
		})
	}

	graphPath := filesystem.Join(g.outputDir, "indices", "graph.jsonl")
	return g.jsonlWriter.WriteWithMeta(ctx, graphPath, meta, items)
}

// generateSourcesIndex generates indices/sources.json.
func (g *Generator) generateSourcesIndex(ctx context.Context, data *ProcessedData) error {
	docFields := SourcesIndexDocFields()
	sources := buildSourcesList(data.Buckets)

	sourcesIndex := map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"sources":           sources,
	}

	sourcesPath := filesystem.Join(g.outputDir, "indices", "sources.json")
	return g.jsonWriter.Write(ctx, sourcesPath, sourcesIndex)
}

type sourceInfo struct {
	ID        string
	Name      string
	Type      string
	Instances int
	Tables    int
	Buckets   []string
}

// buildSourcesList builds sources list from bucket data.
// Extracted helper to avoid code duplication between generateSourcesIndex and generateManifestExtended.
func buildSourcesList(buckets []*ProcessedBucket) []map[string]any {
	sourceMap := make(map[string]*sourceInfo)
	for _, bucket := range buckets {
		source := bucket.Source
		if _, ok := sourceMap[source]; !ok {
			sourceMap[source] = &sourceInfo{
				ID:        source,
				Name:      formatSourceName(source),
				Type:      inferSourceType(source),
				Instances: 0,
				Tables:    0,
				Buckets:   []string{},
			}
		}
		sourceMap[source].Instances++
		sourceMap[source].Tables += bucket.TableCount
		sourceMap[source].Buckets = append(sourceMap[source].Buckets, bucket.DisplayName)
	}

	sources := make([]map[string]any, 0, len(sourceMap))
	for _, info := range sourceMap {
		sources = append(sources, map[string]any{
			"id":           info.ID,
			"name":         info.Name,
			"type":         info.Type,
			"instances":    info.Instances,
			"total_tables": info.Tables,
			"buckets":      info.Buckets,
		})
	}
	return sources
}

// formatSourceName converts a source ID to a human-readable name.
func formatSourceName(source string) string {
	names := map[string]string{
		"shopify":        "Shopify",
		"hubspot":        "HubSpot",
		"salesforce":     "Salesforce",
		"google":         "Google",
		"facebook":       "Facebook",
		"instagram":      "Instagram",
		"linkedin":       "LinkedIn",
		"twitter":        "Twitter",
		"stripe":         "Stripe",
		"zendesk":        "Zendesk",
		"intercom":       "Intercom",
		"mailchimp":      "Mailchimp",
		"segment":        "Segment",
		"mixpanel":       "Mixpanel",
		"amplitude":      "Amplitude",
		"snowflake":      "Snowflake",
		"bigquery":       "BigQuery",
		"redshift":       "Redshift",
		"postgres":       "PostgreSQL",
		"mysql":          "MySQL",
		"mongodb":        "MongoDB",
		"transformation": "Transformation Output",
		"ecommerce":      "E-Commerce Platform",
		"unknown":        "Unknown Source",
	}

	if name, ok := names[source]; ok {
		return name
	}
	return source
}

// inferSourceType infers the type of a source.
func inferSourceType(source string) string {
	extractors := map[string]bool{
		"shopify": true, "hubspot": true, "salesforce": true, "google": true,
		"facebook": true, "instagram": true, "linkedin": true, "twitter": true,
		"stripe": true, "zendesk": true, "intercom": true, "mailchimp": true,
		"segment": true, "mixpanel": true, "amplitude": true, "ecommerce": true,
	}

	databases := map[string]bool{
		"snowflake": true, "bigquery": true, "redshift": true,
		"postgres": true, "mysql": true, "mongodb": true,
	}

	if extractors[source] {
		return "extractor"
	}
	if databases[source] {
		return "database"
	}
	if source == "transformation" {
		return "internal"
	}
	return "unknown"
}

// generateQueryFiles generates pre-computed query files.
func (g *Generator) generateQueryFiles(ctx context.Context, data *ProcessedData) error {
	// Generate tables-by-source.json.
	if err := g.generateTablesBySource(ctx, data); err != nil {
		return err
	}

	// Generate transformations-by-platform.json.
	if err := g.generateTransformationsByPlatform(ctx, data); err != nil {
		return err
	}

	// Generate most-connected-nodes.json.
	if err := g.generateMostConnectedNodes(ctx, data); err != nil {
		return err
	}

	return nil
}

// generateTablesBySource generates indices/queries/tables-by-source.json.
func (g *Generator) generateTablesBySource(ctx context.Context, data *ProcessedData) error {
	tablesBySource := make(map[string][]string)
	for _, table := range data.Tables {
		tablesBySource[table.Source] = append(tablesBySource[table.Source], table.UID)
	}
	// Ensure empty slices for sources with no tables.
	for source := range data.Statistics.BySource {
		if _, ok := tablesBySource[source]; !ok {
			tablesBySource[source] = []string{}
		}
	}

	queryData := map[string]any{
		"_comment":          "GENERATION: Computed from table metadata",
		"_purpose":          "Quick lookup of tables by data source",
		"_update_frequency": "Every sync",
		"tables_by_source":  tablesBySource,
	}

	queryPath := filesystem.Join(g.outputDir, "indices", "queries", "tables-by-source.json")
	return g.jsonWriter.Write(ctx, queryPath, queryData)
}

// generateTransformationsByPlatform generates indices/queries/transformations-by-platform.json.
func (g *Generator) generateTransformationsByPlatform(ctx context.Context, data *ProcessedData) error {
	transformsByPlatform := make(map[string][]string)
	for _, transform := range data.Transformations {
		transformsByPlatform[transform.Platform] = append(transformsByPlatform[transform.Platform], transform.UID)
	}

	queryData := map[string]any{
		"_comment":                    "GENERATION: Computed from transformation metadata",
		"_purpose":                    "Quick lookup of transformations by platform",
		"_update_frequency":           "Every sync",
		"transformations_by_platform": transformsByPlatform,
	}

	queryPath := filesystem.Join(g.outputDir, "indices", "queries", "transformations-by-platform.json")
	return g.jsonWriter.Write(ctx, queryPath, queryData)
}

// generateMostConnectedNodes generates indices/queries/most-connected-nodes.json.
func (g *Generator) generateMostConnectedNodes(ctx context.Context, data *ProcessedData) error {
	// Count connections for each node.
	connectionCounts := make(map[string]int)

	for _, table := range data.Tables {
		consumedByCount := 0
		producedByCount := 0
		if table.Dependencies != nil {
			consumedByCount = len(table.Dependencies.ConsumedBy)
			producedByCount = len(table.Dependencies.ProducedBy)
		}
		connectionCounts[table.UID] = consumedByCount + producedByCount
	}

	for _, transform := range data.Transformations {
		consumesCount := 0
		producesCount := 0
		if transform.Dependencies != nil {
			consumesCount = len(transform.Dependencies.Consumes)
			producesCount = len(transform.Dependencies.Produces)
		}
		connectionCounts[transform.UID] = consumesCount + producesCount
	}

	// Sort by connection count.
	type nodeConnection struct {
		UID         string
		Connections int
	}

	nodes := make([]nodeConnection, 0, len(connectionCounts))
	for uid, count := range connectionCounts {
		nodes = append(nodes, nodeConnection{UID: uid, Connections: count})
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Connections > nodes[j].Connections
	})

	// Limit to top 20 nodes.
	if len(nodes) > 20 {
		nodes = nodes[:20]
	}

	// Convert to output format.
	topNodes := make([]map[string]any, 0, len(nodes))
	for _, node := range nodes {
		topNodes = append(topNodes, map[string]any{
			"uid":         node.UID,
			"connections": node.Connections,
		})
	}

	queryData := map[string]any{
		"_comment":             "GENERATION: Computed from lineage graph",
		"_purpose":             "Identify most connected nodes in the data flow",
		"_update_frequency":    "Every sync",
		"most_connected_nodes": topNodes,
	}

	queryPath := filesystem.Join(g.outputDir, "indices", "queries", "most-connected-nodes.json")
	return g.jsonWriter.Write(ctx, queryPath, queryData)
}

// generateRootFiles generates manifest.yaml, manifest-extended.json, and README.md.
func (g *Generator) generateRootFiles(ctx context.Context, data *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating root files")

	// Generate manifest-extended.json.
	if err := g.generateManifestExtended(ctx, data); err != nil {
		return err
	}

	// Generate manifest.yaml.
	if err := g.generateManifestYAML(ctx, data); err != nil {
		return err
	}

	// Generate README.md.
	if err := g.generateREADME(ctx, data); err != nil {
		return err
	}

	return nil
}

// generateManifestExtended generates manifest-extended.json.
func (g *Generator) generateManifestExtended(ctx context.Context, data *ProcessedData) error {
	docFields := ManifestExtendedDocFields()
	sources := buildSourcesList(data.Buckets)

	// Build platform counts.
	platformCounts := make(map[string]int)
	for _, transform := range data.Transformations {
		platformCounts[transform.Platform]++
	}

	manifest := map[string]any{
		"_comment":          docFields.Comment,
		"_purpose":          docFields.Purpose,
		"_update_frequency": docFields.UpdateFrequency,
		"project_id":        data.ProjectID,
		"twin_version":      TwinFormatVersion,
		"format_version":    FormatVersion,
		"updated":           time.Now().UTC().Format(time.RFC3339),
		"statistics": map[string]any{
			"total_buckets":         data.Statistics.TotalBuckets,
			"total_tables":          data.Statistics.TotalTables,
			"total_transformations": data.Statistics.TotalTransformations,
			"total_edges":           data.Statistics.TotalEdges,
		},
		"sources":                  sources,
		"transformation_platforms": platformCounts,
	}

	manifestPath := filesystem.Join(g.outputDir, "manifest-extended.json")
	return g.jsonWriter.Write(ctx, manifestPath, manifest)
}

// generateManifestYAML generates manifest.yaml.
func (g *Generator) generateManifestYAML(ctx context.Context, data *ProcessedData) error {
	// Simple YAML manifest.
	content := fmt.Sprintf(`# Keboola Twin Format Manifest
# Generated: %s

project_id: "%s"
twin_version: %d
format_version: %d

statistics:
  total_buckets: %d
  total_tables: %d
  total_transformations: %d
  total_edges: %d
`,
		time.Now().UTC().Format(time.RFC3339),
		data.ProjectID.String(),
		TwinFormatVersion,
		FormatVersion,
		data.Statistics.TotalBuckets,
		data.Statistics.TotalTables,
		data.Statistics.TotalTransformations,
		data.Statistics.TotalEdges,
	)

	manifestPath := filesystem.Join(g.outputDir, "manifest.yaml")
	return g.mdWriter.Write(ctx, manifestPath, content)
}

// generateREADME generates README.md.
func (g *Generator) generateREADME(ctx context.Context, data *ProcessedData) error {
	stats := writer.ProjectStats{
		TotalBuckets:         data.Statistics.TotalBuckets,
		TotalTables:          data.Statistics.TotalTables,
		TotalTransformations: data.Statistics.TotalTransformations,
		TotalEdges:           data.Statistics.TotalEdges,
	}

	content := writer.GenerateProjectREADME(data.ProjectID.String(), stats)

	readmePath := filesystem.Join(g.outputDir, "README.md")
	return g.mdWriter.Write(ctx, readmePath, content)
}

// generateAIGuide generates ai/ files from embedded templates.
func (g *Generator) generateAIGuide(ctx context.Context, _ *ProcessedData) error {
	g.logger.Debugf(ctx, "Generating AI guide from templates")

	// List all files in the embedded ai/ directory.
	entries, err := templates.AITemplates.ReadDir("ai")
	if err != nil {
		return errors.Errorf("failed to read embedded ai templates directory: %w", err)
	}

	// Copy all embedded templates to output directory.
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		templateFile := "ai/" + entry.Name()
		content, err := templates.AITemplates.ReadFile(templateFile)
		if err != nil {
			return errors.Errorf("failed to read embedded template %s: %w", templateFile, err)
		}

		outputPath := filesystem.Join(g.outputDir, templateFile)
		if err := g.mdWriter.Write(ctx, outputPath, string(content)); err != nil {
			return errors.Errorf("failed to write %s: %w", outputPath, err)
		}
	}

	return nil
}
