package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TestSoftDeleteRemovesExistingOldFile verifies that a .old file from a previous
// failed operation is successfully removed before a new move operation.
func TestSoftDeleteRemovesExistingOldFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	// Create a modelWriter to test softDelete directly
	w := &modelWriter{
		ctx:     ctx,
		backups: make(map[string]string),
		errors:  errors.NewMultiError(),
		Manager: &Manager{
			fs:     fs,
			logger: logger,
		},
	}

	// Phase 1: Create the initial file
	filePath := "test/config.json"
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filePath, `{"name":"original"}`)))
	assert.True(t, fs.IsFile(ctx, filePath), "Original file should exist")

	// Phase 2: Simulate a previous failed operation that left a .old file
	oldFilePath := filePath + ".old"
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(oldFilePath, `{"name":"old backup from previous failure"}`)))
	assert.True(t, fs.IsFile(ctx, oldFilePath), "Old backup file should exist")

	// Phase 3: Call softDelete - should remove existing .old and create new one
	err := w.softDelete(filePath)
	require.NoError(t, err, "softDelete should succeed")

	// Verify: The .old file should exist and contain the "original" content
	assert.True(t, fs.IsFile(ctx, oldFilePath), "New backup file should exist")
	assert.False(t, fs.IsFile(ctx, filePath), "Original file should be moved")

	content, err := fs.ReadFile(ctx, filesystem.NewFileDef(oldFilePath))
	require.NoError(t, err)
	assert.Contains(t, content.Content, "original", "Backup should contain the moved original content, not the old backup from previous failure")
	assert.NotContains(t, content.Content, "previous failure", "Old backup should have been replaced")
}

// TestSoftDeletePropagatesRemovalError verifies that if removal of an existing
// .old file fails, the error is properly propagated.
func TestSoftDeletePropagatesRemovalError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	w := &modelWriter{
		ctx:     ctx,
		backups: make(map[string]string),
		errors:  errors.NewMultiError(),
		Manager: &Manager{
			fs:     fs,
			logger: logger,
		},
	}

	// Phase 1: Create the initial file
	filePath := "test/config.json"
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filePath, `{"name":"original"}`)))

	// Phase 2: Create a .old path as a directory (making it impossible to remove as a file)
	// This simulates a filesystem error during .old file removal
	oldFilePath := filePath + ".old"
	require.NoError(t, fs.Mkdir(ctx, oldFilePath))
	assert.True(t, fs.IsDir(ctx, oldFilePath), "Old path should be a directory")

	// Phase 3: Call softDelete - should fail because .old directory can't be removed
	err := w.softDelete(filePath)

	// Verify: Error should be propagated
	assert.Error(t, err, "Should return error when .old file removal fails")
}

// TestSoftDeleteWithoutExistingOldFile verifies normal operation when no previous
// .old file exists (the common case).
func TestSoftDeleteWithoutExistingOldFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	w := &modelWriter{
		ctx:     ctx,
		backups: make(map[string]string),
		errors:  errors.NewMultiError(),
		Manager: &Manager{
			fs:     fs,
			logger: logger,
		},
	}

	// Phase 1: Create the initial file
	filePath := "test/config.json"
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filePath, `{"name":"original"}`)))

	// No .old file exists (common case)
	oldFilePath := filePath + ".old"
	assert.False(t, fs.Exists(ctx, oldFilePath), "No .old file should exist initially")

	// Phase 2: Call softDelete
	err := w.softDelete(filePath)
	require.NoError(t, err, "softDelete should succeed in normal case")

	// Verify: The .old file should now exist with the original content
	assert.True(t, fs.IsFile(ctx, oldFilePath), "Backup file should be created")
	assert.False(t, fs.IsFile(ctx, filePath), "Original file should be moved")

	content, err := fs.ReadFile(ctx, filesystem.NewFileDef(oldFilePath))
	require.NoError(t, err)
	assert.Contains(t, content.Content, "original", "Backup should contain the original content")
}
