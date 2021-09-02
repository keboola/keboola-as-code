package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/stretchr/testify/assert"
)

// fileNode is one file/dir in expected or actual directory.
type fileNode struct {
	isDir   bool
	absPath string
}

// fileNodeState in expected and actual directory.
type fileNodeState struct {
	relPath  string
	expected *fileNode
	actual   *fileNode
}

// AssertDirectoryContentsSame compares two directories, in expected file content can be used wildcards.
func AssertDirectoryContentsSame(t assert.TestingT, expectedDir string, actualDir string) {
	nodesState := compareDirectories(expectedDir, actualDir)
	var errors []string
	for _, node := range nodesState {
		// Check if present if both dirs (actual/expected) and if has same type (file/dir)
		if node.actual == nil {
			errors = append(errors, fmt.Sprintf("only in expected \"%s\"", node.expected.absPath))
		} else if node.expected == nil {
			errors = append(errors, fmt.Sprintf("only in actual \"%s\"", node.actual.absPath))
		} else if node.actual.isDir != node.expected.isDir {
			if node.actual.isDir {
				errors = append(errors, fmt.Sprintf("\"%s\" is dir in actual, but file in expected", node.relPath))
			} else {
				errors = append(errors, fmt.Sprintf("\"%s\" is file in actual, but dir in expected", node.relPath))
			}
		} else {
			// Compare content
			if !node.actual.isDir {
				AssertWildcards(
					t,
					GetFileContent(node.expected.absPath),
					GetFileContent(node.actual.absPath),
					fmt.Sprintf("Different content of the file \"%s\".", node.relPath),
				)
			}
		}
	}

	if len(errors) > 0 {
		t.Errorf("Directories are not same:\n" + strings.Join(errors, "\n"))
	}
}

func compareDirectories(expectedDir string, actualDir string) map[string]*fileNodeState {
	// relative path -> state
	hashMap := map[string]*fileNodeState{}
	expectedDirAbs, _ := filepath.Abs(expectedDir)
	actualDirAbs, _ := filepath.Abs(actualDir)
	var err error

	// Process actual dir
	err = filepath.WalkDir(actualDirAbs, func(path string, d os.DirEntry, err error) error {
		relPath := RelPath(actualDirAbs, path)

		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == actualDirAbs {
			return nil
		}

		// Ignore hidden files, except .env*, .gitignore
		if IsIgnoredFile(path, d) {
			return nil
		}

		// Create node
		hashMap[relPath] = &fileNodeState{
			relPath: relPath,
			actual:  &fileNode{d.IsDir(), path},
		}

		return nil
	})

	if err != nil {
		panic(fmt.Errorf("cannot iterate over directory \"%s\": %w", actualDirAbs, err))
	}

	// Process expected dir
	err = filepath.WalkDir(expectedDirAbs, func(path string, d os.DirEntry, err error) error {
		relPath := RelPath(expectedDirAbs, path)

		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == expectedDirAbs {
			return nil
		}

		// Ignore hidden files, except .env*, .gitignore
		if IsIgnoredFile(path, d) {
			return nil
		}

		// Create node if not exists
		if _, ok := hashMap[relPath]; !ok {
			hashMap[relPath] = &fileNodeState{}
		}
		hashMap[relPath].relPath = relPath
		hashMap[relPath].expected = &fileNode{d.IsDir(), path}

		return nil
	})

	if err != nil {
		panic(fmt.Errorf("cannot iterate over directory \"%s\": %w", actualDirAbs, err))
	}

	return hashMap
}

func IsIgnoredFile(path string, d os.DirEntry) bool {
	base := filepath.Base(path)
	return !d.IsDir() &&
		strings.HasPrefix(base, ".") &&
		!strings.HasPrefix(base, ".env") &&
		base != ".gitignore"
}

func IsIgnoredDir(path string, d os.DirEntry) bool {
	base := filepath.Base(path)
	return d.IsDir() && strings.HasPrefix(base, ".")
}
