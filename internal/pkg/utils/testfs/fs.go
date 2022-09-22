package testfs

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
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

func MinimalProjectFs(t *testing.T) filesystem.Fs {
	t.Helper()

	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)

	// Create Fs
	inputDir := filesystem.Join(testDir, "..", "..", "fixtures", "local", "minimal")
	fs := NewMemoryFsFrom(inputDir)

	// Replace ENVs
	envs := env.Empty()
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", "123")
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	testhelper.MustReplaceEnvsDir(fs, `/`, envs)
	return fs
}
