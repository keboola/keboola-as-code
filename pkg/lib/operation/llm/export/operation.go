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

	// Get default branch
	logger.Info(ctx, "Getting default branch...")
	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Using branch: %s (ID: %d)", branch.Name, branch.ID)

	// Fetch project data from APIs
	fetcher := twinformat.NewFetcher(d)
	projectData, err := fetcher.FetchAll(ctx, branch.ID)
	if err != nil {
		return err
	}

	// Log summary of fetched data
	logger.Infof(ctx, "Fetched project data: %d buckets, %d tables, %d jobs",
		len(projectData.Buckets), len(projectData.Tables), len(projectData.Jobs))

	// Process fetched data: build lineage, detect platforms, infer sources
	processor := twinformat.NewProcessor(d)
	processedData, err := processor.Process(ctx, d.Fs().BasePath(), projectData)
	if err != nil {
		return err
	}

	// Log processing summary
	logger.Infof(ctx, "Processed data: %d buckets, %d tables, %d transformations, %d edges",
		processedData.Statistics.TotalBuckets,
		processedData.Statistics.TotalTables,
		processedData.Statistics.TotalTransformations,
		processedData.Statistics.TotalEdges)

	// Generate twin format output directly to the current directory
	outputDir := "."
	generator := twinformat.NewGenerator(d, outputDir)
	if err := generator.Generate(ctx, processedData); err != nil {
		return err
	}

	// Fetch and generate samples if requested
	if opts.ShouldIncludeSamples() {
		logger.Info(ctx, "Fetching table samples...")
		samples, err := fetcher.FetchTableSamples(ctx, projectData.Tables, branch.ID, opts.GetSampleLimit(), opts.GetMaxSamples())
		if err != nil {
			logger.Warnf(ctx, "Failed to fetch samples: %v", err)
		} else if len(samples) > 0 {
			if err := generator.GenerateSamples(ctx, processedData, samples); err != nil {
				logger.Warnf(ctx, "Failed to generate samples: %v", err)
			}
		}
	}

	logger.Infof(ctx, "Twin format exported to: %s", d.Fs().BasePath())
	logger.Info(ctx, "Export done.")

	return nil
}
