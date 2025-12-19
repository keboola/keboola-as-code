package export

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
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

	// Implementation will be added in subsequent PRs:
	// - PR 3 (DMD-919): Fetcher to retrieve project data from APIs
	// - PR 4 (DMD-920): Processor to transform data and build lineage
	// - PR 5 (DMD-921): Generator to write twin_format/ directory

	logger.Info(ctx, "Export done.")

	return nil
}
