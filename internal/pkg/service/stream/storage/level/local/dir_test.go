package local_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
)

func TestNormalizeDirPath(t *testing.T) {
	t.Parallel()
	assert.Empty(t, local.NormalizeDirPath(""))
	assert.Equal(t, "foo", local.NormalizeDirPath("foo"))
	if runtime.GOOS == "windows" {
		assert.Equal(t, `foo\bar\2000-01-01T01-00-00-000Z`, local.NormalizeDirPath(`foo/bar/2000-01-01T01:00:00.000Z`))
	} else {
		assert.Equal(t, `foo/bar/2000-01-01T01-00-00-000Z`, local.NormalizeDirPath(`foo/bar/2000-01-01T01:00:00.000Z`))
	}
}
