package ignore

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const KBCIgnoreFilePath = ".keboola/.kbcignore"

type File struct {
	rawStringPattern string
	state            *state.Registry
}

func newFile(pattern string, state *state.Registry) *File {
	return &File{
		rawStringPattern: pattern,
		state:            state,
	}
}

func LoadFile(ctx context.Context, fs filesystem.Fs, state *state.Registry, path string) (*File, error) {
	if !fs.Exists(ctx, path) {
		return nil, errors.Errorf("ignore file \"%s\" not found", path)
	}

	content, err := fs.ReadFile(ctx, filesystem.NewFileDef(path))
	if err != nil {
		return nil, err
	}

	return newFile(content.Content, state), nil
}
