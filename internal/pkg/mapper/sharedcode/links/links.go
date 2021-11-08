package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// mapper replaces "shared_code_id" with "shared_code_path" in local fs.
type mapper struct {
	model.MapperContext
	*local.Manager
}

func NewMapper(localManager *local.Manager, context model.MapperContext) *mapper {
	return &mapper{MapperContext: context}
}

// getSharedCodeTargetComponentId returns the component for which the shared code is intended.
func (m *mapper) getSharedCodeTargetComponentId(sharedCodeConfig *model.Config) (string, error) {
	componentIdRaw, found := sharedCodeConfig.Content.Get(model.SharedCodeComponentIdContentKey)
	if !found {
		return "", fmt.Errorf(`missing "%s" in %s`, model.SharedCodeComponentIdContentKey, sharedCodeConfig.Desc())
	}

	componentId, ok := componentIdRaw.(string)
	if !ok {
		return "", fmt.Errorf(`key "%s" must be string, found %T, in %s`, model.SharedCodeComponentIdContentKey, componentIdRaw, sharedCodeConfig.Desc())
	}

	return componentId, nil
}
