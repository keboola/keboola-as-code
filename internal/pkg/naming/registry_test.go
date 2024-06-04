package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestNamingPathsStorage(t *testing.T) {
	t.Parallel()
	s := NewRegistry()

	// Attach multiple times with same key
	key1 := BranchKey{ID: 123}
	require.NoError(t, s.Attach(key1, NewAbsPath("", "my-branch")))
	require.NoError(t, s.Attach(key1, NewAbsPath("", "my-branch-123")))
	require.NoError(t, s.Attach(key1, NewAbsPath("", "my-branch-abc")))
	assert.Len(t, s.byPath, 1)
	assert.Len(t, s.byKey, 1)
	assert.Equal(t, key1, s.byPath["my-branch-abc"])
	assert.Equal(t, NewAbsPath("", "my-branch-abc"), s.byKey[key1.String()])

	// Attach another key
	key2 := BranchKey{ID: 456}
	require.NoError(t, s.Attach(key2, NewAbsPath("", "my-branch-456")))
	assert.Len(t, s.byPath, 2)
	assert.Len(t, s.byKey, 2)

	// Attach another key with same path
	err := s.Attach(BranchKey{ID: 789}, NewAbsPath("", "my-branch-456"))
	require.Error(t, err)
	msg := `naming error: path "my-branch-456" is attached to branch "456", but new branch "789" has same path`
	assert.Equal(t, msg, err.Error())

	// Detach
	s.Detach(key2)
	assert.Len(t, s.byPath, 1)
	assert.Len(t, s.byKey, 1)

	// Re-use path
	require.NoError(t, s.Attach(BranchKey{ID: 789}, NewAbsPath("", "my-branch-456")))
	assert.Len(t, s.byPath, 2)
	assert.Len(t, s.byKey, 2)
}
