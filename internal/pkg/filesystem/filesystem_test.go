package filesystem

import (
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromSlash(t *testing.T) {
	t.Parallel()
	assert.Equal(t, filepath.Join(`abc`, `def`), FromSlash(path.Join(`abc`, `def`))) // nolint forbidifo
}

func TestToSlash(t *testing.T) {
	t.Parallel()
	assert.Equal(t, path.Join(`abc`, `def`), ToSlash(filepath.Join(`abc`, `def`))) // nolint forbidifo
}

func TestRel(t *testing.T) {
	t.Parallel()
	out, err := Rel(`foo/bar`, `foo/bar/abc/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "abc/file.txt", out)
	out, err = Rel(`/foo/bar/../abc`, `/foo/bar/../abc/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "file.txt", out)
	out, err = Rel(`foo/bar/../abc`, `/foo/bar/../abc/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "file.txt", out)
	out, err = Rel(`/foo/bar/../abc`, `foo/bar/../abc/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "file.txt", out)
}

func TestRelFromRootDir(t *testing.T) {
	t.Parallel()
	out, err := Rel(`/`, `/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "file.txt", out)
	out, err = Rel(`/`, `/dir/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "dir/file.txt", out)
	out, err = Rel(``, `/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "file.txt", out)
	out, err = Rel(``, `/dir/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "dir/file.txt", out)
	out, err = Rel(``, `file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "file.txt", out)
	out, err = Rel(``, `dir/file.txt`)
	require.NoError(t, err)
	assert.Equal(t, "dir/file.txt", out)
}

func TestJoin(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `foo/bar/abc/file.txt`, Join("foo/bar/abc", "file.txt"))
}

func TestSplit(t *testing.T) {
	t.Parallel()
	dir, file := Split(`foo/bar/abc/file.txt`)
	assert.Equal(t, "foo/bar/abc/", dir)
	assert.Equal(t, "file.txt", file)
}

func TestDir(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "foo/bar/abc", Dir(`foo/bar/abc/file.txt`))
}

func TestBase(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "file.txt", Base(`foo/bar/abc/file.txt`))
}

func TestMatch(t *testing.T) {
	t.Parallel()
	m, err := Match(`foo/*/*/*`, `foo/bar/abc/file.txt`)
	require.NoError(t, err)
	assert.True(t, m)

	m, err = Match(`abc/**`, `foo/bar/abc/file.txt`)
	require.NoError(t, err)
	assert.False(t, m)
}

func TestIsFrom(t *testing.T) {
	t.Parallel()
	assert.True(t, IsFrom(`abc/def`, `abc`))
	assert.True(t, IsFrom(`abc/def/file.txt`, `abc`))
	assert.True(t, IsFrom(`abc/def/file.txt`, ``))
	assert.False(t, IsFrom(`abc`, `abc`))
	assert.False(t, IsFrom(`xyz`, `abc`))
	assert.False(t, IsFrom(`xyz/def`, `abc`))
	assert.False(t, IsFrom(`xyz/def/file.txt`, `abc`))
}
