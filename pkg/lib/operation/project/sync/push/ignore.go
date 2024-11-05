package push

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func parseIgnoredPatterns(content string) []string {
	var ignorePatterns []string
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "#") {
			ignorePatterns = append(ignorePatterns, trimmedLine)
		}
	}

	return ignorePatterns
}

func ignoreConfigsAndRows(projectState *state.Registry) {
	if len(projectState.IgnoredConfigs()) > 0 {
		for _, v := range projectState.IgnoredConfigs() {
			v.SetLocalState(nil)
		}
	}

	if len(projectState.IgnoredConfigRows()) > 0 {
		for _, v := range projectState.IgnoredConfigRows() {
			v.SetLocalState(nil)
		}
	}
}

func setIgnoredConfigsOrRows(ctx context.Context, projectState *state.Registry, fs filesystem.Fs, path string) error {
	content, err := fs.ReadFile(ctx, filesystem.NewFileDef(path))
	if err != nil {
		return err
	}

	if content.Content == "" {
		return nil
	}

	for _, val := range parseIgnoredPatterns(content.Content) {
		ignoredConfigOrRows := strings.Split(val, "/")

		switch len(ignoredConfigOrRows) {
		case 3:
			projectState.IgnoreConfigRow(ignoredConfigOrRows[2])
		case 2:
			projectState.IgnoreConfig(ignoredConfigOrRows[1])
		}
	}

	return nil
}
