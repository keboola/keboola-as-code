package model_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestPathsStateEmpty(t *testing.T) {
	t.Parallel()
	paths, err := loadPathsState(t, "empty")
	assert.NotNil(t, paths)
	assert.NoError(t, err)
	assert.Empty(t, paths.TrackedPaths())
	assert.Empty(t, paths.UntrackedPaths())

	// Mark tracked some non-existing file -> no change
	paths.MarkTracked("foo/bar")
	assert.Empty(t, paths.TrackedPaths())
	assert.Empty(t, paths.UntrackedPaths())
}

func TestPathsStateComplex(t *testing.T) {
	t.Parallel()
	paths, err := loadPathsState(t, "complex")
	assert.NotNil(t, paths)
	assert.NoError(t, err)

	// All untracked + hidden nodes ignored
	assert.Empty(t, paths.TrackedPaths())
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/description.md",
		"123-branch/extractor",
		"123-branch/extractor/ex-generic-v2",
		"123-branch/extractor/ex-generic-v2/456-todos",
		"123-branch/extractor/ex-generic-v2/456-todos/config.json",
		"123-branch/extractor/ex-generic-v2/456-todos/description.md",
		"123-branch/extractor/ex-generic-v2/456-todos/meta.json",
		"123-branch/extractor/ex-generic-v2/456-todos/untracked1",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/meta.json",
		"main",
		"main/description.md",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, paths.UntrackedPaths())

	// Test IsDir/IsFile
	assert.True(t, paths.IsDir(`123-branch/extractor/ex-generic-v2/456-todos`))
	assert.False(t, paths.IsFile(`123-branch/extractor/ex-generic-v2/456-todos`))
	assert.False(t, paths.IsDir(`123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json`))
	assert.True(t, paths.IsFile(`123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json`))
	assert.PanicsWithError(t, `unknown path "abc"`, func() {
		paths.IsDir(`abc`)
	})
	assert.PanicsWithError(t, `unknown path "abc"`, func() {
		paths.IsFile(`abc`)
	})

	// Mark tracked some leaf node
	paths.MarkTracked("123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json")
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/extractor",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
	}, paths.TrackedPaths())
	assert.Equal(t, []string{
		"123-branch/description.md",
		"123-branch/extractor/ex-generic-v2",
		"123-branch/extractor/ex-generic-v2/456-todos",
		"123-branch/extractor/ex-generic-v2/456-todos/config.json",
		"123-branch/extractor/ex-generic-v2/456-todos/description.md",
		"123-branch/extractor/ex-generic-v2/456-todos/meta.json",
		"123-branch/extractor/ex-generic-v2/456-todos/untracked1",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/meta.json",
		"main",
		"main/description.md",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, paths.UntrackedPaths())

	// Mark tracked some directory
	paths.MarkTracked("main/extractor/ex-generic-v2")
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/extractor",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"main",
		"main/extractor",
		"main/extractor/ex-generic-v2",
	}, paths.TrackedPaths())
	assert.Equal(t, []string{
		"123-branch/description.md",
		"123-branch/extractor/ex-generic-v2",
		"123-branch/extractor/ex-generic-v2/456-todos",
		"123-branch/extractor/ex-generic-v2/456-todos/config.json",
		"123-branch/extractor/ex-generic-v2/456-todos/description.md",
		"123-branch/extractor/ex-generic-v2/456-todos/meta.json",
		"123-branch/extractor/ex-generic-v2/456-todos/untracked1",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/meta.json",
		"main/description.md",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, paths.UntrackedPaths())
}

func loadPathsState(t *testing.T, fixture string) (*PathsState, error) {
	t.Helper()
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	projectDir := filesystem.Join(testDir, "..", "fixtures", "local", fixture)
	fs, err := aferofs.NewLocalFs(zap.NewNop().Sugar(), projectDir, ".")
	assert.NoError(t, err)
	return NewPathsState(fs)
}
