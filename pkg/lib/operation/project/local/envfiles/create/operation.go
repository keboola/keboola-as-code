package init

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	StorageAPIToken() keboola.Token
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.envfiles.create")
	defer span.End(&err)

	logger := d.Logger()

	// .env.local - with token value
	envLocalMsg := " - it contains the API token, keep it local and secret"
	envLocalLines := []filesystem.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: fmt.Sprintf(`KBC_STORAGE_API_TOKEN="%s"`, d.StorageAPIToken().Token)},
	}
	if err := createFile(ctx, logger, fs, ".env.local", envLocalMsg, envLocalLines); err != nil {
		return err
	}

	// .env.dist - with token template
	envDistMsg := ` - an ".env.local" template`
	envDistLines := []filesystem.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: `KBC_STORAGE_API_TOKEN=`},
	}
	if err := createFile(ctx, logger, fs, ".env.dist", envDistMsg, envDistLines); err != nil {
		return err
	}

	// .gitignore - to keep ".env.local" local
	gitIgnoreMsg := ` - to keep ".env.local" local`
	gitIgnoreLines := []filesystem.FileLine{
		{Line: "/.env.local"},
		{Line: "/.keboola/project.json"},
	}
	if err := createFile(ctx, logger, fs, ".gitignore", gitIgnoreMsg, gitIgnoreLines); err != nil {
		return err
	}

	return nil
}

func createFile(ctx context.Context, logger log.Logger, fs filesystem.Fs, path, desc string, lines []filesystem.FileLine) error {
	updated, err := fs.CreateOrUpdateFile(ctx, filesystem.NewFileDef(path).SetDescription(desc), lines)
	if err != nil {
		return err
	}

	if updated {
		logger.Infof(ctx, "Updated file \"%s\"%s.", path, desc)
	} else {
		logger.Infof(ctx, "Created file \"%s\"%s.", path, desc)
	}

	return nil
}
