package init

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Logger() log.Logger
	Options() *options.Options
	Telemetry() telemetry.Telemetry
	StorageAPIToken() keboola.Token
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "kac.lib.operation.project.local.envfiles.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// .env.local - with token value
	envLocalMsg := " - it contains the API token, keep it local and secret"
	envLocalLines := []filesystem.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: fmt.Sprintf(`KBC_STORAGE_API_TOKEN="%s"`, d.StorageAPIToken().Token)},
	}
	if err := createFile(logger, fs, ".env.local", envLocalMsg, envLocalLines); err != nil {
		return err
	}

	// .env.dist - with token template
	envDistMsg := ` - an ".env.local" template`
	envDistLines := []filesystem.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: `KBC_STORAGE_API_TOKEN=`},
	}
	if err := createFile(logger, fs, ".env.dist", envDistMsg, envDistLines); err != nil {
		return err
	}

	// .gitignore - to keep ".env.local" local
	gitIgnoreMsg := ` - to keep ".env.local" local`
	gitIgnoreLines := []filesystem.FileLine{
		{Line: "/.env.local"},
	}
	if err := createFile(logger, fs, ".gitignore", gitIgnoreMsg, gitIgnoreLines); err != nil {
		return err
	}

	return nil
}

func createFile(logger log.Logger, fs filesystem.Fs, path, desc string, lines []filesystem.FileLine) error {
	updated, err := fs.CreateOrUpdateFile(filesystem.NewFileDef(path).SetDescription(desc), lines)
	if err != nil {
		return err
	}

	if updated {
		logger.Infof("Updated file \"%s\"%s.", path, desc)
	} else {
		logger.Infof("Created file \"%s\"%s.", path, desc)
	}

	return nil
}
