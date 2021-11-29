package init

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	Fs() filesystem.Fs
	StorageApi() (*remote.StorageApi, error)
}

func Run(d dependencies) (err error) {
	logger := d.Logger()
	fs := d.Fs()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// .env.local - with token value
	envLocalMsg := " - it contains the API token, keep it local and secret"
	envLocalLines := []filesystem.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: fmt.Sprintf(`KBC_STORAGE_API_TOKEN="%s"`, storageApi.Token().Token)},
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

func createFile(logger *zap.SugaredLogger, fs filesystem.Fs, path, desc string, lines []filesystem.FileLine) error {
	updated, err := fs.CreateOrUpdateFile(path, desc, lines)
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
