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

func Run(ctx context.Context, _ Options, d dependencies) (err error) {
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

	// Implementation will be added in subsequent PRs:
	// - PR 4 (DMD-920): Processor to transform data and build lineage
	// - PR 5 (DMD-921): Generator to write twin_format/ directory

	logger.Info(ctx, "Export done.")

	return nil
}
