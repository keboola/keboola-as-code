package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *variablesMapper) MapBeforePersist(recipe *model.PersistRecipe) error {
	// Variables are represented by config
	configManifest, ok := recipe.Manifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	// Parent of the variables must be config that using variables
	parentKey, ok := recipe.ParentKey.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Get component
	component, err := m.State.Components().Get(configManifest.ComponentKey())
	if err != nil {
		return err
	}

	// Component must be "variables"
	if !component.IsVariables() {
		return nil
	}

	// Branch must be same
	if parentKey.BranchKey() != configManifest.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.Desc(), parentKey.Desc()))
	}

	// Add relation
	configManifest.Relations.Add(&model.VariablesForRelation{
		Target: parentKey.ConfigKeySameBranch(),
	})

	return nil
}

func (m *variablesMapper) OnObjectsPersist(_ model.OnObjectsPersistEvent) error {
	return nil
}
