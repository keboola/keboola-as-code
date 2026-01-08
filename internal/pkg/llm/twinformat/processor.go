package twinformat

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/relvacode/iso8601"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// ProcessedData holds all processed data ready for generation.
type ProcessedData struct {
	// Project metadata
	ProjectID keboola.ProjectID
	BranchID  keboola.BranchID

	// Processed tables with lineage
	Tables []*ProcessedTable

	// Processed transformations with lineage and job status
	Transformations []*ProcessedTransformation

	// Processed buckets with source inference
	Buckets []*ProcessedBucket

	// Processed jobs
	Jobs []*ProcessedJob

	// Lineage graph
	LineageGraph *LineageGraph

	// Statistics
	Statistics *ProcessingStatistics

	// Timestamp
	ProcessedAt time.Time
}

// ProcessedTable represents a table with computed lineage.
type ProcessedTable struct {
	*keboola.Table
	UID          string
	Source       string
	BucketName   string
	Dependencies *TableDependencies
}

// ProcessedTransformation represents a transformation with computed data.
type ProcessedTransformation struct {
	// Embedded scanned transformation (may be nil if only API data available)
	*ScannedTransformation
	// Basic metadata
	UID         string
	Name        string
	ComponentID string
	ConfigID    string
	Description string
	IsDisabled  bool
	Platform    string
	Path        string
	// Dependencies and job execution
	Dependencies *TransformationDependencies
	JobExecution *JobExecution
	// Code blocks (from local or API)
	CodeBlocks []*ProcessedCodeBlock
	// Data source flag
	FromAPI bool
}

// ProcessedCodeBlock represents a code block ready for output.
type ProcessedCodeBlock struct {
	Name     string
	Language string
	Codes    []*ProcessedCode
}

// ProcessedCode represents a single code script.
type ProcessedCode struct {
	Name     string
	Language string
	Script   string
}

// ProcessedBucket represents a bucket with source inference.
type ProcessedBucket struct {
	*keboola.Bucket
	Source     string
	TableCount int
	TableNames []string
}

// ProcessedJob represents a processed job.
type ProcessedJob struct {
	*keboola.QueueJob
	TransformationUID string
	Platform          string
}

// ProcessingStatistics holds statistics about the processed data.
type ProcessingStatistics struct {
	TotalBuckets         int
	TotalTables          int
	TotalTransformations int
	TotalJobs            int
	TotalEdges           int
	ByPlatform           map[string]int
	BySource             map[string]int
	ByJobStatus          map[string]int
}

// ProcessorDependencies defines dependencies for the Processor.
type ProcessorDependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs
}

// Processor orchestrates all data processing steps.
type Processor struct {
	logger         log.Logger
	telemetry      telemetry.Telemetry
	fs             filesystem.Fs
	scanner        *Scanner
	lineageBuilder *LineageBuilder
}

// NewProcessor creates a new Processor instance.
func NewProcessor(d ProcessorDependencies) *Processor {
	return &Processor{
		logger:         d.Logger(),
		telemetry:      d.Telemetry(),
		fs:             d.Fs(),
		scanner:        NewScanner(d),
		lineageBuilder: NewLineageBuilder(d),
	}
}

// Process processes all fetched data and returns processed data ready for generation.
func (p *Processor) Process(ctx context.Context, projectDir string, fetchedData *ProjectData) (processed *ProcessedData, err error) {
	ctx, span := p.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.processor.Process")
	defer span.End(&err)

	p.logger.Info(ctx, "Processing fetched data...")

	processed = &ProcessedData{
		ProjectID:   fetchedData.ProjectID,
		BranchID:    fetchedData.BranchID,
		ProcessedAt: time.Now().UTC(),
		Statistics: &ProcessingStatistics{
			ByPlatform:  make(map[string]int),
			BySource:    make(map[string]int),
			ByJobStatus: make(map[string]int),
		},
	}

	// Step 1: Build lineage graph from API-fetched transformation configs
	lineageGraph, err := p.lineageBuilder.BuildLineageGraphFromAPI(ctx, fetchedData.TransformationConfigs)
	if err != nil {
		return nil, err
	}
	processed.LineageGraph = lineageGraph
	processed.Statistics.TotalEdges = len(lineageGraph.Edges)

	// Step 2: Process buckets with source inference
	processed.Buckets = p.processBuckets(ctx, fetchedData.Buckets, fetchedData.Tables, processed.Statistics)

	// Step 3: Process tables with lineage
	processed.Tables = p.processTables(ctx, fetchedData.Tables, lineageGraph)

	// Step 4: Process transformations from API data with platform detection and job linking
	processed.Transformations = p.processTransformationsFromAPI(ctx, fetchedData.TransformationConfigs, fetchedData.Jobs, lineageGraph, processed.Statistics)

	// Step 5: Process jobs
	processed.Jobs = p.processJobs(ctx, fetchedData.Jobs, processed.Statistics)

	// Update final statistics
	processed.Statistics.TotalBuckets = len(processed.Buckets)
	processed.Statistics.TotalTables = len(processed.Tables)
	processed.Statistics.TotalTransformations = len(processed.Transformations)
	processed.Statistics.TotalJobs = len(processed.Jobs)

	p.logger.Infof(ctx, "Processed data: %d buckets, %d tables, %d transformations, %d jobs, %d edges",
		processed.Statistics.TotalBuckets,
		processed.Statistics.TotalTables,
		processed.Statistics.TotalTransformations,
		processed.Statistics.TotalJobs,
		processed.Statistics.TotalEdges)

	return processed, nil
}

// processBuckets processes buckets with source inference.
func (p *Processor) processBuckets(ctx context.Context, buckets []*keboola.Bucket, tables []*keboola.Table, stats *ProcessingStatistics) []*ProcessedBucket {
	// Build a map of bucket ID to tables
	bucketTables := make(map[string][]string)
	for _, table := range tables {
		// Use TableID.BucketID instead of Bucket.BucketID since Bucket can be nil
		bucketID := table.TableID.BucketID.String()
		bucketTables[bucketID] = append(bucketTables[bucketID], table.Name)
	}

	processed := make([]*ProcessedBucket, 0, len(buckets))
	for _, bucket := range buckets {
		source := InferSourceFromBucket(bucket.BucketID.String())
		stats.BySource[source]++

		tableNames := bucketTables[bucket.BucketID.String()]
		processed = append(processed, &ProcessedBucket{
			Bucket:     bucket,
			Source:     source,
			TableCount: len(tableNames),
			TableNames: tableNames,
		})
	}

	p.logger.Infof(ctx, "Processed %d buckets", len(processed))
	return processed
}

// processTables processes tables with lineage dependencies.
func (p *Processor) processTables(ctx context.Context, tables []*keboola.Table, graph *LineageGraph) []*ProcessedTable {
	processed := make([]*ProcessedTable, 0, len(tables))

	for _, table := range tables {
		// Build table UID - use TableID.BucketID since Bucket can be nil
		bucketID := table.TableID.BucketID.String()
		bucketName := extractBucketName(bucketID)
		uid := BuildTableUIDFromParts(bucketName, table.Name)

		// Get dependencies from lineage graph
		deps := p.lineageBuilder.GetTableDependencies(graph, uid)

		// Infer source from bucket
		source := InferSourceFromBucket(bucketID)

		processed = append(processed, &ProcessedTable{
			Table:        table,
			UID:          uid,
			Source:       source,
			BucketName:   bucketName,
			Dependencies: deps,
		})
	}

	p.logger.Infof(ctx, "Processed %d tables", len(processed))
	return processed
}

// processTransformationsFromAPI processes transformations from API-fetched configs.
func (p *Processor) processTransformationsFromAPI(ctx context.Context, configs []*TransformationConfig, jobs []*keboola.QueueJob, graph *LineageGraph, stats *ProcessingStatistics) []*ProcessedTransformation {
	// Build a map of component+config to latest job
	jobMap := buildJobMap(jobs)

	processed := make([]*ProcessedTransformation, 0, len(configs))

	for _, cfg := range configs {
		// Detect platform
		platform := DetectPlatform(cfg.ComponentID)
		stats.ByPlatform[platform]++

		// Build transformation UID
		name := cfg.Name
		if name == "" {
			name = cfg.ID
		}
		uid := BuildTransformationUIDFromName(name)

		// Get dependencies from lineage graph
		deps := p.lineageBuilder.GetTransformationDependencies(graph, uid)

		// Link to latest job
		var jobExec *JobExecution
		jobKey := cfg.ComponentID + ":" + cfg.ID
		if job, ok := jobMap[jobKey]; ok {
			jobExec = &JobExecution{
				LastRunTime:     formatJobTimePtr(job.StartTime),
				LastRunStatus:   job.Status,
				JobReference:    job.ID.String(),
				DurationSeconds: job.DurationSeconds,
			}
			if job.Status == "error" && job.Result.Message != "" {
				jobExec.LastError = job.Result.Message
			}
		}

		// Convert code blocks from API format to processed format
		codeBlocks := p.convertCodeBlocks(cfg.Blocks, platform)

		processed = append(processed, &ProcessedTransformation{
			UID:          uid,
			Name:         cfg.Name,
			ComponentID:  cfg.ComponentID,
			ConfigID:     cfg.ID,
			Description:  cfg.Description,
			IsDisabled:   cfg.IsDisabled,
			Platform:     platform,
			Dependencies: deps,
			JobExecution: jobExec,
			CodeBlocks:   codeBlocks,
			FromAPI:      true,
		})
	}

	p.logger.Infof(ctx, "Processed %d transformations from API", len(processed))
	return processed
}

// convertCodeBlocks converts API code blocks to processed format.
func (p *Processor) convertCodeBlocks(blocks []*CodeBlock, platform string) []*ProcessedCodeBlock {
	if len(blocks) == 0 {
		return nil
	}

	language := platformToLanguage(platform)
	result := make([]*ProcessedCodeBlock, 0, len(blocks))

	for _, block := range blocks {
		processedBlock := &ProcessedCodeBlock{
			Name:     block.Name,
			Language: language,
			Codes:    make([]*ProcessedCode, 0, len(block.Codes)),
		}

		for _, code := range block.Codes {
			processedBlock.Codes = append(processedBlock.Codes, &ProcessedCode{
				Name:     code.Name,
				Language: language,
				Script:   code.Script,
			})
		}

		if len(processedBlock.Codes) > 0 {
			result = append(result, processedBlock)
		}
	}

	return result
}

// platformToLanguage converts a transformation platform to its underlying code language.
// Python and R use their own languages; DBT and other platforms use SQL as the underlying language.
func platformToLanguage(platform string) string {
	switch platform {
	case PlatformPython:
		return PlatformPython
	case PlatformR:
		return PlatformR
	default:
		return PlatformSQL
	}
}

// processJobs processes jobs.
func (p *Processor) processJobs(ctx context.Context, jobs []*keboola.QueueJob, stats *ProcessingStatistics) []*ProcessedJob {
	processed := make([]*ProcessedJob, 0, len(jobs))

	for _, job := range jobs {
		stats.ByJobStatus[job.Status]++

		// Detect platform from component ID
		platform := DetectPlatform(job.ComponentID.String())

		// Build transformation UID if this is a transformation job
		var transformUID string
		if IsTransformationComponent(job.ComponentID.String()) {
			transformUID = BuildTransformationUIDFromName(job.ConfigID.String())
		}

		processed = append(processed, &ProcessedJob{
			QueueJob:          job,
			TransformationUID: transformUID,
			Platform:          platform,
		})
	}

	p.logger.Infof(ctx, "Processed %d jobs", len(processed))
	return processed
}

// buildJobMap builds a map of component+config to latest job.
func buildJobMap(jobs []*keboola.QueueJob) map[string]*keboola.QueueJob {
	jobMap := make(map[string]*keboola.QueueJob)

	for _, job := range jobs {
		key := job.ComponentID.String() + ":" + job.ConfigID.String()

		// Keep the latest job (jobs are typically sorted by time descending)
		if existing, ok := jobMap[key]; !ok || isJobNewer(job, existing) {
			jobMap[key] = job
		}
	}

	return jobMap
}

// isJobNewer returns true if job is newer than existing.
func isJobNewer(job, existing *keboola.QueueJob) bool {
	if job.StartTime == nil {
		return false
	}
	if existing.StartTime == nil {
		return true
	}
	return job.StartTime.After(existing.StartTime.Time)
}

// extractBucketName extracts the bucket name from a bucket ID.
// Input: "in.c-bucket" -> "bucket".
func extractBucketName(bucketID string) string {
	// Split by "." to get stage and bucket parts
	parts := strings.Split(bucketID, ".")
	if len(parts) >= 2 {
		// Remove "c-" prefix if present
		bucket := parts[1]
		if len(bucket) > 2 && bucket[:2] == "c-" {
			return bucket[2:]
		}
		return bucket
	}
	return bucketID
}

// formatJobTimePtr formats a job time pointer for output.
func formatJobTimePtr(t *iso8601.Time) string {
	if t == nil {
		return ""
	}
	return t.Time.UTC().Format(time.RFC3339)
}
