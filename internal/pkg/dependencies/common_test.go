package dependencies

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

type testAbstractDeps struct {
	storageApiHost  string
	storageApiToken string
}

func (t *testAbstractDeps) Logger() log.Logger {
	return log.NewNopLogger()
}

func (t *testAbstractDeps) Fs() filesystem.Fs {
	panic("not implemented")
}

func (t *testAbstractDeps) Envs() *env.Map {
	panic("not implemented")
}

func (t *testAbstractDeps) ApiVerboseLogs() bool {
	return false
}

func (t *testAbstractDeps) StorageApiHost() (string, error) {
	return t.storageApiHost, nil
}

func (t *testAbstractDeps) StorageApiToken() (string, error) {
	return t.storageApiToken, nil
}

func TestDifferentProjectIdInManifestAndToken(t *testing.T) {
	t.Parallel()
	testProject := testproject.GetTestProject(t, env.Empty())

	d := newCommonDeps(&testAbstractDeps{storageApiHost: testProject.StorageApiHost(), storageApiToken: testProject.Token()}, context.Background())
	d.projectManifest = manifest.New(12345, testProject.StorageApiHost())

	_, err := d.StorageApi()
	expected := fmt.Sprintf(`given token is from the project "%d", but in manifest is defined project "12345"`, testProject.Id())
	assert.Error(t, err)
	assert.Equal(t, expected, err.Error())
}
