package naming

import (
	"strings"
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
	var longNameRaw strings.Builder
	for range 60 {
		longNameRaw.WriteString("MyFile_Name ")
	}
	// Normalize as happens in real code (e.g., from user-provided sink name)
	longNameNormalized := strhelper.NormalizeName(longNameRaw.String())
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

	// Verify the filename is properly normalized (normalization happens before truncation)
	assert.LessOrEqual(t, len(filename), maxFilenameLength, "Filename should be truncated to max length")
	assert.NotContains(t, filename, " ", "Filename should not contain spaces (should be normalized)")
	assert.NotContains(t, filename, "M", "Filename should not contain uppercase (should be normalized to lowercase)")
	// Note: We don't check filename == NormalizeName(filename) because truncation
	// happens AFTER normalization and may cut mid-pattern, so re-normalizing might change it.

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
	assert.NotEqual(t, result1.Path(), result2.Path(), "Paths should be unique")

	// Verify the suffix is properly appended
	assert.Regexp(t, `-\d{3}$`, filename2, "Second filename should end with suffix like -001")
}

func TestMakeUniquePathUTF8Safety(t *testing.T) {
	t.Parallel()
	s := NewRegistry()

	// Create a long filename with multibyte UTF-8 characters (emoji, accented chars)
	// Each emoji is 4 bytes, so 70 emoji = 280 bytes
	var longUTF8Name strings.Builder
	for range 70 {
		longUTF8Name.WriteString("🎉") // 4-byte UTF-8 character
	}

	key1 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "utf8row1"}
	path1 := NewAbsPath("", "codes/"+longUTF8Name.String())
	result1 := s.ensureUniquePath(key1, path1)

	// Extract filename from "codes/filename"
	relativePath := result1.GetRelativePath()
	filename := relativePath
	if len(relativePath) > len("codes/") {
		filename = relativePath[len("codes/"):]
	}

	// Verify the filename is within limits and is valid UTF-8
	assert.LessOrEqual(t, len(filename), maxFilenameLength, "Filename should be truncated to max length")
	assert.True(t, isValidUTF8(filename), "Truncated filename should be valid UTF-8")

	// Create another path to test suffix with UTF-8
	key2 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "utf8row2"}
	path2 := NewAbsPath("", "codes/"+longUTF8Name.String())
	result2 := s.ensureUniquePath(key2, path2)

	relativePath2 := result2.GetRelativePath()
	filename2 := relativePath2
	if len(relativePath2) > len("codes/") {
		filename2 = relativePath2[len("codes/"):]
	}

	assert.LessOrEqual(t, len(filename2), maxFilenameLength, "Filename with suffix should fit within max length")
	assert.True(t, isValidUTF8(filename2), "Truncated filename with suffix should be valid UTF-8")
	assert.NotEqual(t, result1.Path(), result2.Path(), "Paths should be unique")
}

func TestMakeUniquePathUTF8BoundaryTruncation(t *testing.T) {
	t.Parallel()
	s := NewRegistry()

	// Create a filename where truncation point would fall in the middle of a multibyte char
	// Fill most of the space with ASCII, then add multibyte chars at the boundary
	// maxFilenameLength - suffixReservedLength = 255 - 5 = 250 bytes
	var boundaryName strings.Builder
	// Write 248 ASCII bytes
	for range 248 {
		boundaryName.WriteString("a")
	}
	// Add a 4-byte UTF-8 character - truncation at 250 would split it
	boundaryName.WriteString("🎉") // This makes total 252 bytes

	key1 := ConfigRowKey{BranchID: 123, ComponentID: "test.component", ConfigID: "456", ID: "boundary1"}
	path1 := NewAbsPath("", "codes/"+boundaryName.String())
	result1 := s.ensureUniquePath(key1, path1)

	relativePath := result1.GetRelativePath()
	filename := relativePath
	if len(relativePath) > len("codes/") {
		filename = relativePath[len("codes/"):]
	}

	// The truncation should not split the emoji
	assert.LessOrEqual(t, len(filename), maxFilenameLength, "Filename should be truncated to max length")
	assert.True(t, isValidUTF8(filename), "Filename should be valid UTF-8 even at boundary")
}

func isValidUTF8(s string) bool {
	for i := 0; i < len(s); {
		r, size := rune(s[i]), 1
		if s[i] >= 0x80 {
			r, size = decodeRune(s[i:])
		}
		if r == 0xFFFD && size == 1 {
			return false // Invalid UTF-8 sequence
		}
		i += size
	}
	return true
}

func decodeRune(s string) (rune, int) {
	if len(s) == 0 {
		return 0xFFFD, 0
	}
	// Simple UTF-8 decode for validation
	b := s[0]
	if b < 0x80 {
		return rune(b), 1
	}
	if b < 0xC0 {
		return 0xFFFD, 1
	}
	if b < 0xE0 {
		if len(s) < 2 {
			return 0xFFFD, 1
		}
		return rune(b&0x1F)<<6 | rune(s[1]&0x3F), 2
	}
	if b < 0xF0 {
		if len(s) < 3 {
			return 0xFFFD, 1
		}
		return rune(b&0x0F)<<12 | rune(s[1]&0x3F)<<6 | rune(s[2]&0x3F), 3
	}
	if len(s) < 4 {
		return 0xFFFD, 1
	}
	return rune(b&0x07)<<18 | rune(s[1]&0x3F)<<12 | rune(s[2]&0x3F)<<6 | rune(s[3]&0x3F), 4
}
