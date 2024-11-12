package ignore

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (f *File) IgnoreConfigsOrRows() error {
	return f.applyIgnoredPatterns()
}

func (f *File) parseIgnoredPatterns() []string {
	var ignorePatterns []string
	lines := strings.Split(f.rawStringPattern, "\n")
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "#") {
			ignorePatterns = append(ignorePatterns, trimmedLine)
		}
	}

	return ignorePatterns
}

// applyIgnorePattern applies a single ignore pattern, marking the appropriate config or row as ignored.
func (f *File) applyIgnorePattern(ignoreConfig string) error {
	parts := strings.Split(ignoreConfig, "/")

	switch len(parts) {
	case 2:
		// Ignore config by ID and name.
		configID, componentID := parts[1], parts[0]
		f.state.IgnoreConfig(configID, componentID)
	case 3:
		// Ignore specific config row.
		configID, rowID := parts[1], parts[2]
		f.state.IgnoreConfigRow(configID, rowID)
	default:
		return errors.Errorf("invalid ignore ignoreConfig format: %s", ignoreConfig)
	}

	return nil
}

// applyIgnoredPatterns parses the content for ignore patterns and applies them to configurations or rows.
func (f *File) applyIgnoredPatterns() error {
	for _, pattern := range f.parseIgnoredPatterns() {
		if err := f.applyIgnorePattern(pattern); err != nil {
			continue
		}
	}
	return nil
}
