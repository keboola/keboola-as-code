package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type Options struct {
	Path string
}

type dependencies interface {
	Logger() log.Logger
	TemplateRepositoryDir() (filesystem.Fs, error)
}

func Run(o Options, d dependencies) (filesystem.Fs, error) {
	// Get repository dir
	repositoryDir, err := d.TemplateRepositoryDir()
	if err != nil {
		return nil, err
	}

	// Create template dir
	if err := repositoryDir.Mkdir(o.Path); err != nil {
		return nil, err
	}
	d.Logger().Infof(`Created template dir "%s".`, o.Path)

	// Return FS for the template dir
	return repositoryDir.SubDirFs(o.Path)
}
