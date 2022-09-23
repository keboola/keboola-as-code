package fixtures

import (
	"runtime"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func LoadFS(dirName string, envs testhelper.EnvProvider) (filesystem.Fs, error) {
	// nolint: dogsled
	_, thisFile, _, _ := runtime.Caller(0)
	fixturesDir := filesystem.Dir(thisFile)
	stateDir := filesystem.Join(fixturesDir, dirName)

	// Create Fs
	fs := testfs.NewMemoryFsFrom(stateDir)
	testhelper.MustReplaceEnvsDir(fs, `/`, envs)

	return fs, nil
}

func LoadManifest(dirName string, envs testhelper.EnvProvider) (*manifest.Manifest, filesystem.Fs, error) {
	fs, err := LoadFS(dirName, envs)
	if err != nil {
		return nil, nil, err
	}

	m, err := manifest.Load(fs, false)
	if err != nil {
		return nil, nil, err
	}
	return m, fs, nil
}
