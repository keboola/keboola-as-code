package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type FileLine struct {
	Line   string
	Regexp string
}

// FileExists returns true if file exists.
func FileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsFile returns true if fle exists.
func IsFile(path string) bool {
	if s, err := os.Stat(path); err == nil {
		return !s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsDir returns true if dir exists.
func IsDir(path string) bool {
	if s, err := os.Stat(path); err == nil {
		return s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// GetFileContent in test.
func GetFileContent(path string) string {
	// Check if file exists
	if !IsFile(path) {
		panic(fmt.Errorf("file \"%s\" not found", path))
	}

	// Read content, handle error
	contentBytes, err := ioutil.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("cannot get file \"%s\" content: %w", path, err))
	}

	return string(contentBytes)
}

func AbsPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		panic(fmt.Errorf("cannot get absolute path: %w", err))
	}
	return abs
}

func RelPath(base string, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		panic(fmt.Errorf("cannot get relative path: %w", err))
	}
	return rel
}

func CreateOrUpdateFile(path string, lines []FileLine) (updated bool, err error) {
	// Read file if exists
	bytes, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	updated = err == nil

	// Process expected lines
	content := string(bytes)
	for _, line := range lines {
		newValue := strings.TrimSuffix(line.Line, "\n") + "\n"
		regExpStr := "(?m)" + line.Regexp // multi-line mode, ^ match line start
		if len(line.Regexp) == 0 {
			// No regexp specified, search fo line if already present
			regExpStr = regexp.QuoteMeta(newValue)
		}

		regExpStr = strings.TrimSuffix(regExpStr, "$") + ".*$" // match whole line
		regExp := regexp.MustCompile(regExpStr)
		if regExp.MatchString(content) {
			// Replace
			content = regExp.ReplaceAllString(content, strings.TrimSuffix(newValue, "\n"))
		} else {
			// Append
			if len(content) > 0 {
				// Add new line, if file has some content
				content = strings.TrimSuffix(content, "\n") + "\n"
			}
			content = fmt.Sprintf("%s%s", content, newValue)
		}
	}

	// Write file
	return updated, os.WriteFile(path, []byte(content), 0644)
}
