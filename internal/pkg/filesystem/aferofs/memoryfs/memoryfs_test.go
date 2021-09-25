package memoryfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMemoryFs(t *testing.T) {
	fs := New()
	assert.Equal(t, "__memory__", fs.BasePath())
}
