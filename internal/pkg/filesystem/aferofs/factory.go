// nolint: forbidigo
package aferofs

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/localfs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/memoryfs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
)

func NewLocalFs(rootDir string, opts ...filesystem.Option) (fs filesystem.Fs, err error) {
	backendFs, err := localfs.New(rootDir)
	if err != nil {
		return nil, err
	}
	return New(backendFs, opts...), nil
}

func NewMemoryFs(opts ...filesystem.Option) filesystem.Fs {
	return New(memoryfs.New(), opts...)
}

// NewMemoryFsOrErr implements filesystem.Factory interface.
func NewMemoryFsOrErr(opts ...filesystem.Option) (filesystem.Fs, error) {
	return New(memoryfs.New(), opts...), nil
}

func NewMemoryFsFrom(localDir string, opts ...filesystem.Option) filesystem.Fs {
	memoryFs, err := NewMemoryFsFromOrErr(localDir, opts...)
	if err != nil {
		panic(err)
	}
	return memoryFs
}

func NewMemoryFsFromOrErr(localDir string, opts ...filesystem.Option) (filesystem.Fs, error) {
	memoryFs := NewMemoryFs(opts...)
	if err := CopyFs2Fs(nil, localDir, memoryFs, ""); err != nil {
		return nil, fmt.Errorf(`cannot init memory fs from local dir "%s": %w`, localDir, err)
	}
	return memoryFs, nil
}

func NewMountFs(root filesystem.Fs, mounts []mountfs.MountPoint, opts ...filesystem.Option) (fs filesystem.Fs, err error) {
	rootFs, ok := root.(*Fs)
	if !ok {
		return nil, fmt.Errorf(`type "%T" is not supported`, root)
	}

	// Use options from root filesystem by default
	opts = append(
		[]filesystem.Option{
			filesystem.WithLogger(root.Logger()),
			filesystem.WithWorkingDir(root.WorkingDir()),
		},
		opts...,
	)

	return New(mountfs.New(rootFs.Backend(), rootFs.BasePath(), mounts...), opts...), nil
}
