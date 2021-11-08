package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) OnObjectsLoad(event model.OnObjectsLoadEvent) error {
	// Only on local load
	if event.StateType != model.StateTypeLocal {
		return nil
	}

	errors := utils.NewMultiError()
	for _, object := range event.NewObjects {
		if err := m.replaceSharedCodePathById(object); err != nil {
			errors.Append(err)
		}
	}
	return errors.ErrorOrNil()
}

// MapAfterLocalLoad - replace shared code path by id.
func (m *mapper) replaceSharedCodePathById(object model.Object) error {
	// Shared code is used by config
	config, ok := object.(*model.Config)
	if !ok {
		return nil
	}

	// Config have to be from transformation component
	component, err := m.State.Components().Get(config.ComponentKey())
	if err != nil {
		return err
	}
	if !component.IsTransformation() {
		return nil
	}

	// Path is stored in configuration
	sharedCodePathRaw, found := config.Content.Get(model.SharedCodePathContentKey)
	if !found {
		return nil
	}

	// Path must be string
	sharedCodePath, ok := sharedCodePathRaw.(string)
	if !ok {
		return fmt.Errorf(`key "%s" must be string, found %T`, model.SharedCodePathContentKey, sharedCodePathRaw)
	}

	// Remove shared code id
	defer func() {
		config.Content.Delete(model.SharedCodePathContentKey)
	}()

	// Get shared code config
	sharedCodeState, err := m.getSharedCodeByPath(config.BranchId, sharedCodePath)
	if err != nil {
		return err
	} else if sharedCodeState == nil {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`shared code "%s" not found`, sharedCodePath))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, config.Desc()))
		return errors
	}
	sharedCode := sharedCodeState.LocalOrRemoteState().(*model.Config)
	targetComponentId, err := m.getSharedCodeTargetComponentId(sharedCode)
	if err != nil {
		return err
	}

	// Check componentId
	if targetComponentId != config.ComponentId {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`unexpected shared code "%s" in %s`, model.SharedCodeComponentIdContentKey, sharedCodeState.Desc()))
		errors.AppendRaw(fmt.Sprintf(`  - expected "%s"`, config.ComponentId))
		errors.AppendRaw(fmt.Sprintf(`  - found "%s"`, targetComponentId))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, config.Desc()))
		return errors
	}

	// Replace Shared Code Path -> Shared Code ID
	config.Content.Set(model.SharedCodeIdContentKey, sharedCodeState.Id)
	return nil
}

func (m *mapper) getSharedCodeByPath(branchId int, sharedCodePath string) (*model.ConfigState, error) {
	for _, objectState := range m.State.All() {
		// Same path?
		if objectState.GetObjectPath() != sharedCodePath {
			continue
		}

		// Config?
		sharedCode, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		// Same branch?
		if sharedCode.BranchId != branchId {
			continue
		}

		// Shared code?
		component, err := m.State.Components().Get(sharedCode.ComponentKey())
		if err != nil {
			return nil, err
		}
		if !component.IsSharedCode() {
			continue
		}

		return sharedCode, nil
	}
	return nil, nil
}
