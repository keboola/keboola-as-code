package schema

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/xeipuuv/gojsonschema"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
	"strings"
)

func ValidateSchemas(projectState *state.State) error {
	errors := &utils.Error{}
	for _, config := range projectState.Configs() {
		// Validate only local files
		if config.Local == nil {
			continue
		}

		component := projectState.GetComponent(*config.ComponentKey())
		if err := ValidateConfig(component, config.Local); err != nil {
			errors.AddSubError(fmt.Sprintf("config \"%s\" doesn't match schema", config.ConfigFilePath()), err)
		}
	}

	for _, row := range projectState.ConfigRows() {
		// Validate only local files
		if row.Local == nil {
			continue
		}

		component := projectState.GetComponent(*row.ComponentKey())
		if err := ValidateConfigRow(component, row.Local); err != nil {
			errors.AddSubError(fmt.Sprintf("config row \"%s\" doesn't match schema", row.ConfigFilePath()), err)
		}
	}

	if errors.Len() > 0 {
		return errors
	}

	return nil
}

func ValidateConfig(component *model.Component, config *model.Config) error {
	return validateJsonSchema(component.Schema, config.Content)
}

func ValidateConfigRow(component *model.Component, configRow *model.ConfigRow) error {
	return validateJsonSchema(component.SchemaRow, configRow.Content)
}

func validateJsonSchema(schema map[string]interface{}, content *orderedmap.OrderedMap) error {
	// Get parameters key
	var parametersMap *orderedmap.OrderedMap
	parameters, found := content.Get("parameters")
	if found {
		if v, ok := parameters.(orderedmap.OrderedMap); ok {
			parametersMap = &v
		} else {
			parametersMap = utils.NewOrderedMap()
		}
	} else {
		parametersMap = utils.NewOrderedMap()
	}

	// Load
	schemaJson, err := json.EncodeString(schema, true)
	if err != nil {
		return utils.WrapError("cannot encode component schema JSON", err)
	}

	documentJson, err := json.EncodeString(parametersMap, true)
	if err != nil {
		return utils.WrapError("cannot encode JSON", err)
	}

	schemaLoader := gojsonschema.NewStringLoader(schemaJson)
	documentLoader := gojsonschema.NewStringLoader(documentJson)

	// Validate
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return utils.WrapError("schema validation error", err)
	}

	if !result.Valid() {
		errors := &utils.Error{}
		for _, desc := range result.Errors() {
			errors.Add(fmt.Errorf("%s", strings.TrimPrefix(desc.String(), "(root): ")))
		}
		return errors
	}

	return nil
}
