package helper

import (
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	component, err := h.state.Components().GetOrErr(configKey.ComponentId)
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
	component, err := h.state.Components().GetOrErr(configRowKey.ComponentId)
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
	component, err := h.state.Components().GetOrErr(configKey.ComponentId)
	if err != nil || !component.IsTransformation() {
		return false, err
	}
	return true, nil
}

func (h *SharedCodeHelper) CheckTargetComponent(sharedCodeConfig *model.Config, transformation model.ConfigKey) error {
	if sharedCodeConfig.SharedCode == nil {
		panic(errors.New(`shared code value is not set`))
	}
	if sharedCodeConfig.SharedCode.Target != transformation.ComponentId {
		return errors.NewNestedError(
			errors.Errorf(`unexpected shared code "%s" in %s`, model.ShareCodeTargetComponentKey, sharedCodeConfig.Desc()),
			errors.Errorf(`expected "%s"`, transformation.ComponentId),
			errors.Errorf(`found "%s"`, sharedCodeConfig.SharedCode.Target),
		)
	}
	return nil
}

func (h *SharedCodeHelper) GetSharedCodeByPath(parentPath, codePath string) (*model.ConfigState, error) {
	// Get key by path
	codePath = filesystem.Join(parentPath, codePath)
	configStateRaw, found := h.state.GetByPath(codePath)
	if !found {
		return nil, errors.Errorf(`missing shared code "%s"`, codePath)
	}

	// Is config?
	configState, ok := configStateRaw.(*model.ConfigState)
	if !ok {
		return nil, errors.Errorf(`path "%s" is not shared code config`, codePath)
	}

	// Shared code?
	if configState.ComponentId != storageapi.SharedCodeComponentID {
		return nil, errors.Errorf(`config "%s" is not shared code`, codePath)
	}

	// Ok
	return configState, nil
}

func (h *SharedCodeHelper) GetSharedCodeRowByPath(sharedCode *model.ConfigState, path string) (*model.ConfigRowState, error) {
	// Get key by path
	path = filesystem.Join(sharedCode.Path(), path)
	configRowStateRaw, found := h.state.GetByPath(path)
	if !found {
		return nil, errors.Errorf(`missing shared code "%s"`, path)
	}

	// Is config row?
	configRowState, ok := configRowStateRaw.(*model.ConfigRowState)
	if !ok {
		return nil, errors.Errorf(`path "%s" is not config row`, path)
	}

	// Is from parent?
	if sharedCode.Key() != configRowState.ConfigKey() {
		return nil, errors.Errorf(`row "%s" is not from shared code "%s"`, path, sharedCode.Path())
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
