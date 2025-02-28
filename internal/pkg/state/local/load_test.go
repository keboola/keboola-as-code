package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
)

func TestLocalLoadModelNotFound(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t, nil)

	// Save files
	target := &fixtures.MockedObject{}
	record := &fixtures.MockedManifest{}

	// Load
	found, err := manager.loadObject(t.Context(), record, target)
	assert.False(t, found)
	require.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}
