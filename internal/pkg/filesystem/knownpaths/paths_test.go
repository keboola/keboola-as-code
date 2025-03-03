package knownpaths_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	. "github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
)

func TestKnownPathsEmpty(t *testing.T) {
	t.Parallel()
	paths, err := loadKnownPaths(t, "empty")
	assert.NotNil(t, paths)
	require.NoError(t, err)
	assert.Empty(t, paths.TrackedPaths())
	assert.Empty(t, paths.UntrackedPaths())

	// Mark tracked some non-existing file -> no change
	paths.MarkTracked("foo/bar")
	assert.Empty(t, paths.TrackedPaths())
	assert.Empty(t, paths.UntrackedPaths())
}

func TestKnownPathsIgnoredFile(t *testing.T) {
	t.Parallel()
	paths, err := loadKnownPaths(t, "ignored-file")
	assert.NotNil(t, paths)
	require.NoError(t, err)
	assert.Empty(t, paths.TrackedPaths())
	assert.Equal(t, []string{`dir`}, paths.UntrackedPaths())

	// Mark tracked some ignored file -> parent dir is marked as tracked
	paths.MarkTracked("dir/.gitkeep")
	assert.Equal(t, []string{`dir`}, paths.TrackedPaths())
	assert.Empty(t, paths.UntrackedPaths())
}

func TestKnownPathsFilter(t *testing.T) {
	t.Parallel()

	paths, err := loadKnownPaths(t, "complex", WithFilter(func(ctx context.Context, path string) (bool, error) {
		isIgnored := strings.Contains(path, "123-branch") || strings.Contains(path, "extractor")
		return isIgnored, nil
	}))

	// All paths that contain "123-branch" or "extractor" are ignored.
	// Compare result with result of the TestKnownPathsComplex.
	require.NoError(t, err)
	assert.Equal(t, []string{
		"description.md",
		"main",
		"main/description.md",
		"main/meta.json",
	}, paths.UntrackedPaths())
}

func TestKnownPathsComplex(t *testing.T) {
	t.Parallel()
	paths, err := loadKnownPaths(t, "complex")
	assert.NotNil(t, paths)
	require.NoError(t, err)

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
		"description.md",
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
		"description.md",
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
		"description.md",
		"main/description.md",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, paths.UntrackedPaths())

	// Mark tracked sub paths
	paths.MarkSubPathsTracked("123-branch/extractor/keboola.ex-db-mysql/896-tables/rows")
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/extractor",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
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
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/meta.json",
		"description.md",
		"main/description.md",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, paths.UntrackedPaths())
}

func TestKnownPathsClone(t *testing.T) {
	t.Parallel()

	paths, err := loadKnownPaths(t, "complex")
	assert.NotNil(t, paths)
	require.NoError(t, err)

	clone := paths.Clone()
	assert.NotSame(t, paths, clone)
	assert.Equal(t, paths, clone)

	paths.MarkTracked(`123-branch/description.md`)
	assert.NotEqual(t, paths, clone)
}

func TestKnownPathsStateMethods(t *testing.T) {
	t.Parallel()
	paths, err := loadKnownPaths(t, "complex")
	assert.NotNil(t, paths)
	require.NoError(t, err)

	path := `123-branch/extractor/ex-generic-v2`
	assert.Equal(t, Untracked, paths.State(path))
	assert.False(t, paths.IsTracked(path))
	assert.True(t, paths.IsUntracked(path))

	paths.MarkTracked(path)
	assert.Equal(t, Tracked, paths.State(path))
	assert.True(t, paths.IsTracked(path))
	assert.False(t, paths.IsUntracked(path))
}

func TestKnownPathsUntrackedDirs(t *testing.T) {
	t.Parallel()
	paths, err := loadKnownPaths(t, "complex")
	assert.NotNil(t, paths)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"123-branch",
		"123-branch/extractor",
		"123-branch/extractor/ex-generic-v2",
		"123-branch/extractor/ex-generic-v2/456-todos",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"main",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
	}, paths.UntrackedDirs(t.Context()))
}

func TestKnownPathsUntrackedDirsFrom(t *testing.T) {
	t.Parallel()
	paths, err := loadKnownPaths(t, "complex")
	assert.NotNil(t, paths)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
	}, paths.UntrackedDirsFrom(t.Context(), `main/extractor`))
}

func loadKnownPaths(t *testing.T, fixture string, options ...Option) (*Paths, error) {
	t.Helper()
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	projectDir := filesystem.Join(testDir, "..", "..", "fixtures", "local", fixture)
	fs, err := aferofs.NewLocalFs(projectDir)
	require.NoError(t, err)
	return New(t.Context(), fs, options...)
}
