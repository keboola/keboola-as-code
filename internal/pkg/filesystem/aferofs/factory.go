// nolint: forbidigo
package aferofs

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/localfs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/memoryfs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func NewLocalFs(logger log.Logger, rootDir string, workingDirRel string) (fs filesystem.Fs, err error) {
	backendFs, err := localfs.New(rootDir)
	if err != nil {
		return nil, err
	}
	return New(logger, backendFs, workingDirRel), nil
}

func NewMemoryFs(logger log.Logger, workingDir string) (fs filesystem.Fs, err error) {
	return New(logger, memoryfs.New(), workingDir), nil
}

func NewMountFs(root filesystem.Fs, mounts ...mountfs.MountPoint) (fs filesystem.Fs, err error) {
	rootFs, ok := root.(*Fs)
	if !ok {
		return nil, fmt.Errorf(`type "%T" is not supported`, root)
	}
	return New(root.Logger(), mountfs.New(rootFs.Backend(), rootFs.BasePath(), mounts...), root.WorkingDir()), nil
}
