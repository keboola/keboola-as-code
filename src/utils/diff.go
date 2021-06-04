package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strings"
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
	err = filepath.Walk(actualDirAbs, func(path string, info os.FileInfo, err error) error {
		relPath := strings.TrimPrefix(path, actualDirAbs)

		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == actualDirAbs {
			return nil
		}

		// Ignore hidden files
		if strings.HasPrefix(filepath.Base(path), ".") {
			return nil
		}

		// Create node
		hashMap[relPath] = &fileNodeState{
			relPath: relPath,
			actual:  &fileNode{info.IsDir(), path},
		}

		return nil
	})

	if err != nil {
		panic(fmt.Errorf("cannot iterate over directory \"%s\": %s", actualDirAbs, err))
	}

	// Process expected dir
	err = filepath.Walk(expectedDirAbs, func(path string, info os.FileInfo, err error) error {
		relPath := strings.TrimPrefix(path, expectedDirAbs)

		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == expectedDirAbs {
			return nil
		}

		// Ignore hidden files
		if strings.HasPrefix(filepath.Base(path), ".") {
			return nil
		}

		// Create node if not exists
		if _, ok := hashMap[relPath]; !ok {
			hashMap[relPath] = &fileNodeState{}
		}
		hashMap[relPath].relPath = relPath
		hashMap[relPath].expected = &fileNode{info.IsDir(), path}

		return nil
	})

	if err != nil {
		panic(fmt.Errorf("cannot iterate over directory \"%s\": %s", actualDirAbs, err))
	}

	return hashMap
}
