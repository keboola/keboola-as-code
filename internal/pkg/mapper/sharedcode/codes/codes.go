package codes

import (
	"strings"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// mapper saves shared codes (config rows) to "codes" local dir.
type mapper struct {
	model.MapperContext
	*helper.SharedCodeHelper
}

func NewMapper(context model.MapperContext) *mapper {
	return &mapper{MapperContext: context, SharedCodeHelper: helper.New(context.State, context.Naming)}
}

func normalizeContent(m *orderedmap.OrderedMap) {
	// Add empty line to the end of the code file
	if raw, found := m.Get(model.ShareCodeContentKey); found {
		if content, ok := raw.(string); ok {
			content = strings.TrimRight(content, "\r\n") + "\n"
			m.Set(model.ShareCodeContentKey, content)
		}
	}
}
