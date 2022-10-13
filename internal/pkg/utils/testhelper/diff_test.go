//nolint:forbidigo
package testhelper_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	. "github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
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
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile("file.txt", "foo\n")))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in expected \".+file.txt\"", test.buf.String())
}

func TestAssertDirectoryDirOnlyInExpected(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create directory
	assert.NoError(t, expectedFs.Mkdir(`myDir`))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in expected \".+myDir\"", test.buf.String())
}

func TestAssertDirectoryFileOnlyInActual(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile("file.txt", "foo\n")))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in actual \".+file.txt\"", test.buf.String())
}

func TestAssertDirectoryDirOnlyInActual(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create directory
	assert.NoError(t, actualFs.Mkdir(`myDir`))

	// Assert
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Regexp(t, "only in actual \".+myDir\"", test.buf.String())
}

func TestAssertDirectoryFileDifferentType1(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile("myNode", "foo\n")))

	// Create directory in expected
	assert.NoError(t, expectedFs.Mkdir(`myNode`))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "\"myNode\" is file in actual, but dir in expected")
}

func TestAssertDirectoryFileDifferentType2(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// Create file in expected
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile("myNode", "foo\n")))

	// Create directory in actual
	assert.NoError(t, actualFs.Mkdir(`myNode`))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "\"myNode\" is dir in actual, but file in expected")
}

func TestAssertDirectoryDifferentContent(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile("file.txt", "foo\n")))

	// File in actual - different content
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile("file.txt", "bar\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "different content of the file \"file.txt\"")
}

func TestAssertDirectoryDifferentContentWildcards(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	expected := "%c%c%c%c\n" // 4 chars
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile("file.txt", expected)))

	// File in actual - different content
	actual := "foo\n" // 3 chars
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile("file.txt", actual)))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Contains(t, test.buf.String(), "different content of the file \"file.txt\"")
}

func TestAssertDirectorySameEmpty(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()
	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectoryIgnoreHiddenFiles(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	hiddenFilePath := filesystem.Join("myDir", ".hidden")
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile(hiddenFilePath, "foo\n")))

	// File in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile(hiddenFilePath, "bar\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectorySame(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	filePath := filesystem.Join("myDir", "file.txt")
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile(filePath, "foo\n")))

	// File in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile(filePath, "foo\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

func TestAssertDirectorySameWildcards(t *testing.T) {
	t.Parallel()
	expectedFs := aferofs.NewMemoryFs()
	actualFs := aferofs.NewMemoryFs()

	// File in expected
	filePath := filesystem.Join("myDir", "file.txt")
	assert.NoError(t, expectedFs.WriteFile(filesystem.NewRawFile(filePath, "%c%c%c\n")))

	// File in actual
	assert.NoError(t, actualFs.WriteFile(filesystem.NewRawFile(filePath, "foo\n")))

	test := newMockedT()
	AssertDirectoryContentsSame(test, expectedFs, `/`, actualFs, `/`)
	assert.Equal(t, "", test.buf.String())
}

// newMockedT - mocked version of *testing.T.
func newMockedT() *mockedT {
	return &mockedT{buf: bytes.NewBuffer(nil)}
}
