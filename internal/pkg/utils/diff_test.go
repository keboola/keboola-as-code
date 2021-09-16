package utils

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
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
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// Create file
	file1, err := os.Create(filepath.Join(expectedDir, "file.txt"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// Assert
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Regexp(t, "^Directories are not same:\nonly in expected \".+/file.txt\"$", test.buf.String())
}

func TestAssertDirectoryDirOnlyInExpected(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// Create directory
	assert.NoError(t, os.Mkdir(expectedDir+"/myDir", 0700))

	// Assert
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Regexp(t, "^Directories are not same:\nonly in expected \".+/myDir\"$", test.buf.String())
}

func TestAssertDirectoryFileOnlyInActual(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// Create file
	file1, err := os.Create(filepath.Join(actualDir, "file.txt"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// Assert
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Regexp(t, "^Directories are not same:\nonly in actual \".+/file.txt\"$", test.buf.String())
}

func TestAssertDirectoryDirOnlyInActual(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// Create directory
	assert.NoError(t, os.Mkdir(actualDir+"/myDir", 0700))

	// Assert
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Regexp(t, "^Directories are not same:\nonly in actual \".+/myDir\"$", test.buf.String())
}

func TestAssertDirectoryFileDifferentType1(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// Create file in actual
	file1, err := os.Create(filepath.Join(actualDir, "myNode"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// Create directory in expected
	assert.NoError(t, os.Mkdir(expectedDir+"/myNode", 0700))

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, test.buf.String(), "Directories are not same:\n\"myNode\" is file in actual, but dir in expected")
}

func TestAssertDirectoryFileDifferentType2(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// Create file in expected
	file1, err := os.Create(filepath.Join(expectedDir, "myNode"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// Create directory in actual
	assert.NoError(t, os.Mkdir(actualDir+"/myNode", 0700))

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, test.buf.String(), "Directories are not same:\n\"myNode\" is dir in actual, but file in expected")
}

func TestAssertDirectoryDifferentContent(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// File in expected
	file1, err := os.Create(filepath.Join(expectedDir, "file.txt"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// File in actual - different content
	file2, err := os.Create(filepath.Join(actualDir, "file.txt"))
	assert.NoError(t, err)
	_, err = file2.WriteString("bar\n")
	assert.NoError(t, err)
	assert.NoError(t, file2.Close())

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, test.buf.String(), "Different content of the file \"file.txt\". Diff:")
}

func TestAssertDirectoryDifferentContentWildcards(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// File in expected
	file1, err := os.Create(filepath.Join(expectedDir, "/file.txt"))
	assert.NoError(t, err)
	_, err = file1.WriteString("%c%c%c%c\n") // 4 chars
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// File in actual - different content
	file2, err := os.Create(filepath.Join(actualDir, "file.txt"))
	assert.NoError(t, err)
	_, err = file2.WriteString("foo\n") // 3 chars
	assert.NoError(t, err)
	assert.NoError(t, file2.Close())

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, test.buf.String(), "Different content of the file \"file.txt\". Diff:")
}

func TestAssertDirectorySameEmpty(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, "", test.buf.String())
}

func TestAssertDirectoryIgnoreHiddenFiles(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// File in expected
	assert.NoError(t, os.Mkdir(expectedDir+"/myDir", 0700))
	file1, err := os.Create(filepath.Join(expectedDir, "myDir", ".hidden"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// File in actual
	assert.NoError(t, os.Mkdir(actualDir+"/myDir", 0700))
	file2, err := os.Create(filepath.Join(actualDir, "myDir", ".hidden"))
	assert.NoError(t, err)
	_, err = file2.WriteString("bar\n")
	assert.NoError(t, err)
	assert.NoError(t, file2.Close())

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, "", test.buf.String())
}

func TestAssertDirectorySame(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// File in expected
	assert.NoError(t, os.Mkdir(expectedDir+"/myDir", 0700))
	file1, err := os.Create(filepath.Join(expectedDir, "myDir", "file.txt"))
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// File in actual
	assert.NoError(t, os.Mkdir(actualDir+"/myDir", 0700))
	file2, err := os.Create(filepath.Join(actualDir, "myDir", "file.txt"))
	assert.NoError(t, err)
	_, err = file2.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file2.Close())

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, "", test.buf.String())
}

func TestAssertDirectorySameWildcards(t *testing.T) {
	expectedDir := t.TempDir()
	actualDir := t.TempDir()

	// File in expected
	assert.NoError(t, os.Mkdir(expectedDir+"/myDir", 0700))
	file1, err := os.Create(filepath.Join(expectedDir, "myDir", "file.txt"))
	assert.NoError(t, err)
	_, err = file1.WriteString("%c%c%c\n")
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// File in actual
	assert.NoError(t, os.Mkdir(actualDir+"/myDir", 0700))
	file2, err := os.Create(filepath.Join(actualDir, "myDir", "file.txt"))
	assert.NoError(t, err)
	_, err = file2.WriteString("foo\n")
	assert.NoError(t, err)
	assert.NoError(t, file2.Close())

	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertDirectoryContentsSame(test, expectedDir, actualDir)
	assert.Contains(t, "", test.buf.String())
}
