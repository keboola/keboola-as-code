package helper

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// SharedCodeHelper gets some values from shared codes.
type SharedCodeHelper struct {
	state *state.State
}

func New(s *state.State) *SharedCodeHelper {
	return &SharedCodeHelper{state: s}
}

func (h *SharedCodeHelper) IsSharedCodeKey(key model.Key) (bool, error) {
	// Is config?
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}

	// Is shared code?
	component, err := h.state.Components().Get(configKey.ComponentKey())
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

	// Is shared code?
	component, err := h.state.Components().Get(configRowKey.ComponentKey())
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

	// Is shared code?
	component, err := h.state.Components().Get(configKey.ComponentKey())
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
		errors.Append(fmt.Errorf(`unexpected shared code "%s" in %s`, model.ShareCodeTargetComponentKey, sharedCodeConfig.Desc()))
		errors.Append(fmt.Errorf(`  - expected "%s"`, transformation.ComponentId))
		errors.Append(fmt.Errorf(`  - found "%s"`, sharedCodeConfig.SharedCode.Target))
		return errors
	}
	return nil
}

func (h *SharedCodeHelper) GetSharedCodeByPath(branchKey model.BranchKey, path string) (*model.ConfigState, error) {
	// Get branch
	branch, found := h.state.Get(branchKey)
	if !found {
		return nil, fmt.Errorf(`missing %s`, branchKey.Desc())
	}

	// Get key by path
	path = filesystem.Join(branch.Path(), path)
	configStateRaw, found := h.state.GetByPath(path)
	if !found {
		return nil, fmt.Errorf(`missing shared code "%s"`, path)
	}

	// Is config?
	configState, ok := configStateRaw.(*model.ConfigState)
	if !ok {
		return nil, fmt.Errorf(`path "%s" is not shared code config`, path)
	}

	// Is from right parent?
	if branchKey != configState.BranchKey() {
		return nil, fmt.Errorf(`config "%s" is not from branch "%s"`, path, branch.Path())
	}

	// Shared code?
	if configState.ComponentId != model.SharedCodeComponentId {
		return nil, fmt.Errorf(`config "%s" is not shared code`, path)
	}

	// Ok
	return configState, nil
}

func (h *SharedCodeHelper) GetSharedCodeRowByPath(sharedCode *model.ConfigState, path string) (*model.ConfigRowState, error) {
	// Get key by path
	path = filesystem.Join(sharedCode.Path(), path)
	configRowStateRaw, found := h.state.GetByPath(path)
	if !found {
		return nil, fmt.Errorf(`missing shared code "%s"`, path)
	}

	// Is config row?
	configRowState, ok := configRowStateRaw.(*model.ConfigRowState)
	if !ok {
		return nil, fmt.Errorf(`path "%s" is not config row`, path)
	}

	// Is from parent?
	if sharedCode.Key() != configRowState.ConfigKey() {
		return nil, fmt.Errorf(`row "%s" is not from shared code "%s"`, path, sharedCode.Path())
	}

	// Ok
	return configRowState, nil
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
