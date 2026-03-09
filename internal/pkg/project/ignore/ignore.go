package ignore

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (f *File) IgnoreConfigsOrRows() error {
	return f.applyIgnoredPatterns()
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

func (f *File) parseIgnoredPatterns() []string {
	var ignorePatterns []string
	lines := strings.SplitSeq(f.rawStringPattern, "\n")
	for line := range lines {
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
	// Branch pattern: "branch/<name>" — name may itself contain "/".
	if strings.HasPrefix(ignoreConfig, "branch/") {
		branchName := strings.TrimPrefix(ignoreConfig, "branch/")
		f.state.IgnoreBranch(branchName)
		return nil
	}

	// Field-level ignore: "componentID/configID:fieldName"
	if colonIdx := strings.Index(ignoreConfig, ":"); colonIdx != -1 {
		objectPath := ignoreConfig[:colonIdx]
		fieldName := ignoreConfig[colonIdx+1:]
		parts := strings.Split(objectPath, "/")
		if len(parts) == 2 {
			configID, componentID := parts[1], parts[0]
			f.state.IgnoreConfigField(configID, componentID, fieldName)
			return nil
		}
		return errors.Errorf("invalid field-ignore format: %s", ignoreConfig)
	}

	parts := strings.Split(ignoreConfig, "/")
	switch len(parts) {
	case 2:
		// Ignore config by componentID/configID.
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
