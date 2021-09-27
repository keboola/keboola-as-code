package sharedcode

import (
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const TargetComponentConfigJsonKey = `componentId`
const CodeContentRowJsonKey = `code_content`

func normalizeContent(m *orderedmap.OrderedMap) {
	// Add empty line to the end of the code file
	if raw, found := m.Get(CodeContentRowJsonKey); found {
		if content, ok := raw.(string); ok {
			content = strings.TrimRight(content, "\r\n") + "\n"
			m.Set(CodeContentRowJsonKey, content)
		}
	}
}

func getTargetComponentId(config *model.Config) (string, error) {
	// Load content from config row JSON
	raw, found := config.Content.Get(TargetComponentConfigJsonKey)
	if !found {
		return "", fmt.Errorf(`key "%s" not found in %s`, TargetComponentConfigJsonKey, config.Desc())
	}

	return cast.ToString(raw), nil
}
