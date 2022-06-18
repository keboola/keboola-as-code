package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
)

func TestLocalLoadModelNotFound(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t, nil)

	// Save files
	target := &fixtures.MockedObject{}
	record := &fixtures.MockedManifest{}

	// Load
	found, err := manager.loadObject(context.Background(), record, target)
	assert.False(t, found)
	assert.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}
