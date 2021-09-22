package memoryfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMemoryFs(t *testing.T) {
	fs := NewMemoryFs()
	assert.Equal(t, "__memory__", fs.ProjectDir())
}
