package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRel(t *testing.T) {
	assert.Equal(t, "abc/file.txt", Rel(`foo/bar`, `foo/bar/abc/file.txt`))
}

func TestJoin(t *testing.T) {
	assert.Equal(t, `foo/bar/abc/file.txt`, Join("foo/bar/abc", "file.txt"))
}

func TestSplit(t *testing.T) {
	dir, file := Split(`foo/bar/abc/file.txt`)
	assert.Equal(t, "foo/bar/abc/", dir)
	assert.Equal(t, "file.txt", file)
}

func TestDir(t *testing.T) {
	assert.Equal(t, "foo/bar/abc", Dir(`foo/bar/abc/file.txt`))
}

func TestBase(t *testing.T) {
	assert.Equal(t, "file.txt", Base(`foo/bar/abc/file.txt`))
}

func TestMatch(t *testing.T) {
	m, err := Match(`foo/*/*/*`, `foo/bar/abc/file.txt`)
	assert.NoError(t, err)
	assert.True(t, m)

	m, err = Match(`abc/**`, `foo/bar/abc/file.txt`)
	assert.NoError(t, err)
	assert.False(t, m)
}
