package export

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Fs() filesystem.Fs
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	StorageAPIHost() string
	StorageAPIToken() keboola.Token
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, opts Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.llm.export")
	defer span.End(&err)

	logger := d.Logger()

	// Phase 1: Get default branch
	logger.Info(ctx, "[1/5] Getting default branch...")
	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Using branch: %s (ID: %d)", branch.Name, branch.ID)

	// Phase 2: Fetch project data from APIs
	logger.Info(ctx, "[2/5] Fetching project data from APIs...")
	fetcher := twinformat.NewFetcher(d)
	projectData, err := fetcher.FetchAll(ctx, branch.ID)
	if err != nil {
		return err
	}

	// Handle empty project case
	if len(projectData.Buckets) == 0 && len(projectData.Tables) == 0 {
		logger.Warn(ctx, "Project appears to be empty (no buckets or tables found)")
	}

	// Log summary of fetched data
	logger.Infof(ctx, "Fetched: %d buckets, %d tables, %d jobs",
		len(projectData.Buckets), len(projectData.Tables), len(projectData.Jobs))

	// Phase 3: Process fetched data
	logger.Info(ctx, "[3/5] Processing data (lineage, platforms, sources)...")
	processor := twinformat.NewProcessor(d)
	processedData, err := processor.Process(ctx, d.Fs().BasePath(), projectData)
	if err != nil {
		return err
	}

	// Log processing summary
	logger.Infof(ctx, "Processed: %d buckets, %d tables, %d transformations, %d lineage edges",
		processedData.Statistics.TotalBuckets,
		processedData.Statistics.TotalTables,
		processedData.Statistics.TotalTransformations,
		processedData.Statistics.TotalEdges)

	// Phase 4: Generate twin format output directly to current directory
	logger.Info(ctx, "[4/5] Generating twin format output...")
	outputDir := "."
	generator := twinformat.NewGenerator(d, outputDir)
	if err := generator.Generate(ctx, processedData); err != nil {
		return err
	}

	// Phase 5: Fetch and generate samples if requested
	if opts.ShouldIncludeSamples() {
		logger.Info(ctx, "[5/5] Fetching and generating table samples...")
		samples, err := fetcher.FetchTableSamples(ctx, projectData.Tables, branch.ID, opts.GetSampleLimit(), opts.GetMaxSamples())
		if err != nil {
			logger.Warnf(ctx, "Failed to fetch samples (continuing without samples): %v", err)
		} else if len(samples) > 0 {
			if err := generator.GenerateSamples(ctx, processedData, samples); err != nil {
				logger.Warnf(ctx, "Failed to generate samples (continuing without samples): %v", err)
			} else {
				logger.Infof(ctx, "Generated samples for %d tables", len(samples))
			}
		}
	} else {
		logger.Info(ctx, "[5/5] Skipping samples (not requested)")
	}

	logger.Infof(ctx, "Twin format exported to: %s", d.Fs().BasePath())
	logger.Info(ctx, "Export completed successfully.")

	return nil
}
