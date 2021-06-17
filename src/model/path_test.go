package model

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPathsStateDirNotFound(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := filepath.Join(testDir, "foo", "bar")
	paths, err := NewPathsState(projectDir)
	assert.Error(t, err)
	assert.Regexp(t, `^directory ".+" not found`, err.Error())
	assert.Nil(t, paths)
}

func TestPathsStateEmpty(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := filepath.Join(testDir, "fixtures", "local", "empty")
	paths, err := NewPathsState(projectDir)
	assert.NoError(t, err)
	assert.NotNil(t, paths)
	assert.Equal(t, 0, paths.Error().Len())
	assert.Empty(t, paths.Tracked())
	assert.Empty(t, paths.Untracked())

	// Mark tracked some non-existing file -> no change
	paths.MarkTracked("foo/bar")
	assert.Equal(t, 0, paths.Error().Len())
	assert.Empty(t, paths.Tracked())
	assert.Empty(t, paths.Untracked())
}

func TestPathsStateComplex(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := filepath.Join(testDir, "fixtures", "local", "complex")
	paths, err := NewPathsState(projectDir)
	assert.NoError(t, err)
	assert.NotNil(t, paths)
	assert.Equal(t, 0, paths.Error().Len())

	// All untracked + hidden nodes ignored
	assert.Empty(t, paths.Tracked())
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/keboola.ex-db-mysql",
		"123-branch/keboola.ex-db-mysql/896-tables",
		"123-branch/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/keboola.ex-db-mysql/untrackedDir",
		"123-branch/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/keboola.ex-generic",
		"123-branch/keboola.ex-generic/456-todos",
		"123-branch/keboola.ex-generic/456-todos/config.json",
		"123-branch/keboola.ex-generic/456-todos/meta.json",
		"123-branch/keboola.ex-generic/456-todos/untracked1",
		"123-branch/meta.json",
		"main",
		"main/keboola.ex-generic",
		"main/keboola.ex-generic/456-todos",
		"main/keboola.ex-generic/456-todos/config.json",
		"main/keboola.ex-generic/456-todos/meta.json",
		"main/meta.json",
	}, paths.Untracked())

	// Mark tracked some leaf node
	paths.MarkTracked("123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json")
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/keboola.ex-db-mysql",
		"123-branch/keboola.ex-db-mysql/896-tables",
		"123-branch/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
	}, paths.Tracked())
	assert.Equal(t, []string{
		"123-branch/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/keboola.ex-db-mysql/untrackedDir",
		"123-branch/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/keboola.ex-generic",
		"123-branch/keboola.ex-generic/456-todos",
		"123-branch/keboola.ex-generic/456-todos/config.json",
		"123-branch/keboola.ex-generic/456-todos/meta.json",
		"123-branch/keboola.ex-generic/456-todos/untracked1",
		"123-branch/meta.json",
		"main",
		"main/keboola.ex-generic",
		"main/keboola.ex-generic/456-todos",
		"main/keboola.ex-generic/456-todos/config.json",
		"main/keboola.ex-generic/456-todos/meta.json",
		"main/meta.json",
	}, paths.Untracked())

	// Mark tracked some directory
	paths.MarkTracked("main/keboola.ex-generic")
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/keboola.ex-db-mysql",
		"123-branch/keboola.ex-db-mysql/896-tables",
		"123-branch/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"main",
		"main/keboola.ex-generic",
	}, paths.Tracked())
	assert.Equal(t, []string{
		"123-branch/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/keboola.ex-db-mysql/untrackedDir",
		"123-branch/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/keboola.ex-generic",
		"123-branch/keboola.ex-generic/456-todos",
		"123-branch/keboola.ex-generic/456-todos/config.json",
		"123-branch/keboola.ex-generic/456-todos/meta.json",
		"123-branch/keboola.ex-generic/456-todos/untracked1",
		"123-branch/meta.json",
		"main/keboola.ex-generic/456-todos",
		"main/keboola.ex-generic/456-todos/config.json",
		"main/keboola.ex-generic/456-todos/meta.json",
		"main/meta.json",
	}, paths.Untracked())
}
