package variables

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *variablesMapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	// Variables are used by config
	object, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	m.loadVariables(object)
	m.loadVariablesValues(object)
	return nil
}

func (m *variablesMapper) loadVariables(object *model.Config) {
	// Variables ID is stored in configuration
	variablesIDRaw, found := object.Content.Get(model.VariablesIDContentKey)
	if !found {
		return
	}

	// Variables ID must be string
	variablesID, ok := variablesIDRaw.(string)
	if !ok {
		return
	}

	// Create relation
	object.AddRelation(&model.VariablesFromRelation{
		VariablesID: keboola.ConfigID(variablesID),
	})

	// Remove variables ID from configuration content
	object.Content.Delete(model.VariablesIDContentKey)
}

func (m *variablesMapper) loadVariablesValues(object *model.Config) {
	// Values ID is stored in configuration
	valuesIDRaw, found := object.Content.Get(model.VariablesValuesIDContentKey)
	if !found {
		return
	}

	// Values ID must be string
	valuesID, ok := valuesIDRaw.(string)
	if !ok {
		return
	}

	// Config must have define variables config
	variablesRelations := object.Relations.GetByType(model.VariablesFromRelType)
	if len(variablesRelations) != 1 {
		return
	}

	// Create relation
	object.AddRelation(&model.VariablesValuesFromRelation{
		VariablesValuesID: keboola.RowID(valuesID),
	})

	// Remove variables ID from configuration content
	object.Content.Delete(model.VariablesValuesIDContentKey)
}
