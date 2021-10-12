package memoryfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMemoryFs(t *testing.T) {
	t.Parallel()
	fs := New()
	assert.Equal(t, "__memory__", fs.BasePath())
}
