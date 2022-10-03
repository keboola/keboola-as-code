package testfs

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func NewBasePathLocalFs(basePath string) filesystem.Fs {
	fs, err := aferofs.NewLocalFs(log.NewNopLogger(), basePath, `/`)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewMemoryFs() filesystem.Fs {
	return NewMemoryFsWithLogger(log.NewNopLogger())
}

func NewMemoryFsWithLogger(logger log.Logger) filesystem.Fs {
	fs, err := aferofs.NewMemoryFs(logger, `/`)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewMemoryFsFrom(localDir string) filesystem.Fs {
	memoryFs := NewMemoryFs()
	if err := aferofs.CopyFs2Fs(nil, localDir, memoryFs, ``); err != nil {
		panic(fmt.Errorf(`cannot init memory fs from local dir "%s": %w`, localDir, err))
	}
	return memoryFs
}
