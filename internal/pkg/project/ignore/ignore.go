package ignore

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (f *File) IgnoreObjects() error {
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

	parts := strings.Split(ignoreConfig, "/")
	switch {
	case len(parts) == 2:
		// Ignore config by componentID/configID.
		configID, componentID := parts[1], parts[0]
		f.state.IgnoreConfig(configID, componentID)
	case len(parts) == 3 && parts[2] == "notifications":
		// Ignore all notifications for a config: componentID/configID/notifications.
		componentID, configID := parts[0], parts[1]
		f.state.IgnoreNotificationsForConfig(configID, componentID)
	case len(parts) == 4 && parts[2] == "notifications":
		// Ignore a specific notification: componentID/configID/notifications/notificationID.
		componentID, configID, notificationID := parts[0], parts[1], parts[3]
		f.state.IgnoreNotification(configID, componentID, notificationID)
	case len(parts) == 3:
		// Ignore specific config row: componentID/configID/rowID.
		configID, rowID := parts[1], parts[2]
		f.state.IgnoreConfigRow(configID, rowID)
	default:
		return errors.Errorf("invalid ignore ignoreConfig format: %s", ignoreConfig)
	}

	return nil
}
