package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type Options struct {
	RepositoryDir filesystem.Fs
	Path          string
}

type dependencies interface {
	Logger() log.Logger
}

func Run(o Options, d dependencies) (filesystem.Fs, error) {
	repositoryDir := o.RepositoryDir

	// Create template dir
	if err := repositoryDir.Mkdir(o.Path); err != nil {
		return nil, err
	}
	d.Logger().Infof(`Created template dir "%s".`, o.Path)

	// Return FS for the template dir
	return repositoryDir.SubDirFs(o.Path)
}
