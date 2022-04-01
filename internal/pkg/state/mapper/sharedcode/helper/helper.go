package helper

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type dependencies interface {
	Components() (*model.ComponentsMap, error)
}

// SharedCodeHelper gets some values from shared codes.
type SharedCodeHelper struct {
	dependencies
	objects model.Objects
}

func New(objects model.Objects, d dependencies) *SharedCodeHelper {
	return &SharedCodeHelper{dependencies: d, objects: objects}
}

func (h *SharedCodeHelper) IsSharedCodeKey(key model.Key) (bool, error) {
	// Is config?
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}

	// Get components
	components, err := h.Components()
	if err != nil {
		return false, err
	}

	// Is shared code?
	component, err := components.Get(configKey.ComponentKey())
	if err != nil || !component.IsSharedCode() {
		return false, err
	}
	return true, nil
}

func (h *SharedCodeHelper) IsSharedCodeRowKey(key model.Key) (bool, error) {
	// Is config row?
	configRowKey, ok := key.(model.ConfigRowKey)
	if !ok {
		return false, nil
	}

	// Get components
	components, err := h.Components()
	if err != nil {
		return false, err
	}

	// Is shared code?
	component, err := components.Get(configRowKey.ComponentKey())
	if err != nil || !component.IsSharedCode() {
		return false, err
	}
	return true, nil
}

func (h *SharedCodeHelper) IsTransformation(key model.Key) (bool, error) {
	// Is config?
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}

	// Get components
	components, err := h.Components()
	if err != nil {
		return false, err
	}

	// Is shared code?
	component, err := components.Get(configKey.ComponentKey())
	if err != nil || !component.IsTransformation() {
		return false, err
	}
	return true, nil
}

func (h *SharedCodeHelper) CheckTargetComponent(sharedCodeConfig *model.Config, transformation model.ConfigKey) error {
	if sharedCodeConfig.SharedCode == nil {
		panic(fmt.Errorf(`shared code value is not set`))
	}
	if sharedCodeConfig.SharedCode.Target != transformation.ComponentId {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`unexpected shared code "%s" in %s`, model.ShareCodeTargetComponentKey, sharedCodeConfig.String()))
		errors.Append(fmt.Errorf(`  - expected "%s"`, transformation.ComponentId))
		errors.Append(fmt.Errorf(`  - found "%s"`, sharedCodeConfig.SharedCode.Target))
		return errors
	}
	return nil
}

func (h *SharedCodeHelper) GetSharedCodeVariablesId(configRow *model.ConfigRow) (string, bool) {
	// Variables ID is stored in configuration
	variablesIdRaw, found := configRow.Content.Get(model.SharedCodeVariablesIdContentKey)
	if !found {
		return "", false
	}

	// Variables ID must be string
	variablesId, ok := variablesIdRaw.(string)
	if !ok {
		return "", false
	}

	return variablesId, true
}
