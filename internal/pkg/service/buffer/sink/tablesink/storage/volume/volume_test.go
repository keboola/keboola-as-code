package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateVolumeID(t *testing.T) {
	t.Parallel()
	assert.Len(t, GenerateID(), IDLength)
}
