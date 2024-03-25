package project

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const FileName = "project.json"

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

type file struct {
	Backends []string            `json:"backends"`
	Features keboola.FeaturesMap `json:"features"`
}

func newFile() *file {
	return &file{
		Backends: make([]string, 0),
		Features: keboola.FeaturesMap{},
	}
}

func Load(ctx context.Context, fs filesystem.Fs) (*file, error) {
	content := newFile()

	path := Path()
	if fs.IsFile(ctx, path) {
		if _, err := fs.FileLoader().ReadJSONFileTo(ctx, filesystem.NewFileDef(path).SetDescription("manifest"), content); err != nil {
			return nil, err
		}
	}
	return content, nil
}
