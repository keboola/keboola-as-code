package fixtures

import (
	"context"
	"runtime"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func LoadFS(ctx context.Context, dirName string, envs testhelper.EnvProvider) (filesystem.Fs, error) {
	// nolint: dogsled
	_, thisFile, _, _ := runtime.Caller(0)
	fixturesDir := filesystem.Dir(thisFile)
	stateDir := filesystem.Join(fixturesDir, dirName)

	// Create Fs
	fs := aferofs.NewMemoryFsFrom(stateDir)
	err := testhelper.ReplaceEnvsDir(ctx, fs, `/`, envs)

	return fs, err
}

func LoadManifest(ctx context.Context, dirName string, envs testhelper.EnvProvider) (*manifest.Manifest, filesystem.Fs, error) {
	fs, err := LoadFS(ctx, dirName, envs)
	if err != nil {
		return nil, nil, err
	}

	m, err := manifest.Load(ctx, log.NewNopLogger(), fs, env.Empty(), false)
	if err != nil {
		return nil, nil, err
	}
	return m, fs, nil
}
