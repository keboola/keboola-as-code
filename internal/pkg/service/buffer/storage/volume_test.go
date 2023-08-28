package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateVolumeID(t *testing.T) {
	t.Parallel()
	assert.Len(t, GenerateVolumeID(), VolumeIDLength)
}
