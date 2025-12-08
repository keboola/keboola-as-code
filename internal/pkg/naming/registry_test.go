package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
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
	for range 300 {
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

func TestMakeUniquePathNormalizesAfterTruncation(t *testing.T) {
	t.Parallel()
	s := NewRegistry()

	// Create a long filename with special characters (uppercase, spaces, underscores)
	// In real usage, names are normalized before entering makeUniquePath
	// This tests that normalization + truncation + suffix work correctly together
	longNameRaw := ""
	for range 60 {
		longNameRaw += "MyFile_Name "
	}
	// Normalize as happens in real code (e.g., from user-provided sink name)
	longNameNormalized := strhelper.NormalizeName(longNameRaw)
	// Total after normalization: 60 * 13 = 780 chars ("my-file-name-" repeated)

	key1 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "row1"}
	path1 := NewAbsPath("", "codes/"+longNameNormalized)
	result1 := s.ensureUniquePath(key1, path1)

	// Extract filename from "codes/filename"
	relativePath := result1.GetRelativePath()
	filename := relativePath
	if len(relativePath) > len("codes/") {
		filename = relativePath[len("codes/"):]
	}

	// Verify the filename is properly normalized
	assert.LessOrEqual(t, len(filename), maxFilenameLength, "Filename should be truncated to max length")
	assert.NotContains(t, filename, " ", "Filename should not contain spaces (should be normalized)")
	assert.NotContains(t, filename, "M", "Filename should not contain uppercase (should be normalized to lowercase)")
	assert.Equal(t, filename, strhelper.NormalizeName(filename), "Filename should be fully normalized")

	// Verify normalization converts spaces to hyphens, underscores, and lowercases
	// "MyFile_Name " becomes "my-file-name-" (space at end becomes hyphen)
	assert.Contains(t, filename, "my-file-name", "Filename should contain normalized 'my-file-name' pattern")

	// Create second path to test uniqueness with special characters
	key2 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "row2"}
	path2 := NewAbsPath("", "codes/"+longNameNormalized)
	result2 := s.ensureUniquePath(key2, path2)

	relativePath2 := result2.GetRelativePath()
	filename2 := relativePath2
	if len(relativePath2) > len("codes/") {
		filename2 = relativePath2[len("codes/"):]
	}

	// Second filename should also be normalized
	assert.LessOrEqual(t, len(filename2), maxFilenameLength, "Second filename should fit within max length")
	assert.NotContains(t, filename2, " ", "Second filename should not contain spaces")
	assert.Equal(t, filename2, strhelper.NormalizeName(filename2), "Second filename should be fully normalized")
	assert.NotEqual(t, result1.Path(), result2.Path(), "Paths should be unique")

	// Verify the suffix is properly normalized and appended
	assert.Regexp(t, `-\d{3}$`, filename2, "Second filename should end with normalized suffix like -001")
}
