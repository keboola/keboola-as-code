package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *mapper) MapBeforePersist(recipe *model.PersistRecipe) error {
	// Variables are represented by config
	configManifest, ok := recipe.Manifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	// Component must be "variables"
	variablesComponent, err := m.state.Components().Get(configManifest.ComponentKey())
	if err != nil {
		return err
	}
	if !variablesComponent.IsVariables() {
		return nil
	}

	// Parent is shared code
	sharedCodeRowKey, ok := recipe.ParentKey.(model.ConfigRowKey)
	if !ok {
		return nil
	}

	// Parent component must be "variables"
	parentComponent, err := m.state.Components().Get(sharedCodeRowKey.ComponentKey())
	if err != nil {
		return err
	}
	if !parentComponent.IsSharedCode() {
		return nil
	}

	// Branch must be same
	if sharedCodeRowKey.BranchKey() != configManifest.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.Desc(), sharedCodeRowKey.Desc()))
	}

	// Add relation
	configManifest.Relations.Add(&model.SharedCodeVariablesForRelation{
		ConfigId: sharedCodeRowKey.ConfigId,
		RowId:    sharedCodeRowKey.Id,
	})

	return nil
}
