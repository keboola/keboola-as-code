//nolint:forbidigo
package testhelper

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
)

type mockedT struct {
	buf *bytes.Buffer
}

// Implements TestingT for mockedT.
func (t *mockedT) Errorf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	t.buf.WriteString(s)
}

func TestAssertDirectoryFileOnlyInExpected(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// Create file
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile("file.txt", "foo\n")))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "^Directories are not same:\nonly in expected \".+file.txt\"$", test.buf.String())
}

func TestAssertDirectoryDirOnlyInExpected(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// Create directory
	assert.NoError(t, expectedFs.Mkdir(`myDir`))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "^Directories are not same:\nonly in expected \".+myDir\"$", test.buf.String())
}

func TestAssertDirectoryFileOnlyInActual(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// Create file
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile("file.txt", "foo\n")))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "^Directories are not same:\nonly in actual \".+file.txt\"$", test.buf.String())
}

func TestAssertDirectoryDirOnlyInActual(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// Create directory
	assert.NoError(t, actualFs.Mkdir(`myDir`))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "^Directories are not same:\nonly in actual \".+myDir\"$", test.buf.String())
}

func TestAssertDirectoryFileDifferentType1(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// Create file in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile("myNode", "foo\n")))

	// Create directory in expected
	assert.NoError(t, expectedFs.Mkdir(`myNode`))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "Directories are not same:\n\"myNode\" is file in actual, but dir in expected")
}

func TestAssertDirectoryFileDifferentType2(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// Create file in expected
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile("myNode", "foo\n")))

	// Create directory in actual
	assert.NoError(t, actualFs.Mkdir(`myNode`))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "Directories are not same:\n\"myNode\" is dir in actual, but file in expected")
}

func TestAssertDirectoryDifferentContent(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// File in expected
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile("file.txt", "foo\n")))

	// File in actual - different content
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile("file.txt", "bar\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "Different content of the file \"file.txt\". Diff:")
}

func TestAssertDirectoryDifferentContentWildcards(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// File in expected
	expected := "%c%c%c%c\n" // 4 chars
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile("file.txt", expected)))

	// File in actual - different content
	actual := "foo\n" // 3 chars
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile("file.txt", actual)))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "Different content of the file \"file.txt\". Diff:")
}

func TestAssertDirectorySameEmpty(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectoryIgnoreHiddenFiles(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// File in expected
	hiddenFilePath := filesystem.Join("myDir", ".hidden")
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile(hiddenFilePath, "foo\n")))

	// File in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile(hiddenFilePath, "bar\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectorySame(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// File in expected
	filePath := filesystem.Join("myDir", "file.txt")
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile(filePath, "foo\n")))

	// File in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile(filePath, "foo\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectorySameWildcards(t *testing.T) {
	t.Parallel()
	expectedFs := testfs.NewMemoryFs()
	actualFs := testfs.NewMemoryFs()

	// File in expected
	filePath := filesystem.Join("myDir", "file.txt")
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewFile(filePath, "%c%c%c\n")))

	// File in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewFile(filePath, "foo\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

// newMockedT - mocked version of *testing.T.
func newMockedT() *mockedT {
	return &mockedT{buf: bytes.NewBuffer(nil)}
}
