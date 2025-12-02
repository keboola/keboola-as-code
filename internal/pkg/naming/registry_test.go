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

func TestMakeUniquePathTruncatesLongFilenames(t *testing.T) {
	t.Parallel()
	s := NewRegistry()

	// Create a very long filename that exceeds 255 bytes
	longName := ""
	for i := 0; i < 300; i++ {
		longName += "a"
	}

	key1 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "row1"}
	path1 := NewAbsPath("", "codes/"+longName)
	result1 := s.ensureUniquePath(key1, path1)

	// The filename component should be truncated to fit within 255 bytes
	relativePath := result1.GetRelativePath()
	// Extract filename from "codes/filename"
	filename := relativePath
	if len(relativePath) > len("codes/") {
		filename = relativePath[len("codes/"):]
	}
	assert.LessOrEqual(t, len(filename), maxFilenameLength, "Filename should be truncated to max length")

	// Create another path with the same long name - should get a unique suffix
	key2 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "row2"}
	path2 := NewAbsPath("", "codes/"+longName)
	result2 := s.ensureUniquePath(key2, path2)

	// The second path should also be truncated and have a suffix
	relativePath2 := result2.GetRelativePath()
	filename2 := relativePath2
	if len(relativePath2) > len("codes/") {
		filename2 = relativePath2[len("codes/"):]
	}
	assert.LessOrEqual(t, len(filename2), maxFilenameLength, "Filename with suffix should fit within max length")
	assert.NotEqual(t, result1.Path(), result2.Path(), "Paths should be unique")
}
