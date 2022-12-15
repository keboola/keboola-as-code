package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceNumberToFilename(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "slice_10", sliceNumberToFilename(10))
}
