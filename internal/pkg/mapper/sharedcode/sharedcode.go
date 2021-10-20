package sharedcode

import (
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type sharedCodeMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *sharedCodeMapper {
	return &sharedCodeMapper{MapperContext: context}
}

func (m *sharedCodeMapper) isSharedCodeConfigRow(object interface{}) (bool, error) {
	v, ok := object.(*model.ConfigRow)
	if !ok {
		return false, nil
	}

	component, err := m.State.Components().Get(*v.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsSharedCode(), nil
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

func getTargetComponentId(config *model.Config) (string, error) {
	// Load content from config row JSON
	raw, found := config.Content.Get(model.ShareCodeTargetComponentKey)
	if !found {
		return "", fmt.Errorf(`key "%s" not found in %s`, model.ShareCodeTargetComponentKey, config.Desc())
	}

	return cast.ToString(raw), nil
}
