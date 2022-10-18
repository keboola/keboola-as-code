package variables

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

func (m *mapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	// Variables are used by config
	object, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	m.loadVariables(object)
	m.loadVariablesValues(object)
	return nil
}

func (m *mapper) loadVariables(object *model.Config) {
	// Variables ID is stored in configuration
	variablesIdRaw, found := object.Content.Get(model.VariablesIdContentKey)
	if !found {
		return
	}

	// Variables ID must be string
	variablesId, ok := variablesIdRaw.(string)
	if !ok {
		return
	}

	// Create relation
	object.AddRelation(&model.VariablesFromRelation{
		VariablesId: storageapi.ConfigID(variablesId),
	})

	// Remove variables ID from configuration content
	object.Content.Delete(model.VariablesIdContentKey)
}

func (m *mapper) loadVariablesValues(object *model.Config) {
	// Values ID is stored in configuration
	valuesIdRaw, found := object.Content.Get(model.VariablesValuesIdContentKey)
	if !found {
		return
	}

	// Values ID must be string
	valuesId, ok := valuesIdRaw.(string)
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
		VariablesValuesId: storageapi.RowID(valuesId),
	})

	// Remove variables ID from configuration content
	object.Content.Delete(model.VariablesValuesIdContentKey)
}
