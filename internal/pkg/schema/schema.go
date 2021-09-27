package schema

import (
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/xeipuuv/gojsonschema"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func ValidateSchemas(projectState *state.State) error {
	errors := utils.NewMultiError()
	for _, config := range projectState.Configs() {
		// Validate only local files
		if config.Local == nil {
			continue
		}

		component, err := projectState.Components().Get(*config.ComponentKey())
		if err != nil {
			return err
		}

		if err := ValidateConfig(component, config.Local); err != nil {
			errors.AppendWithPrefix(fmt.Sprintf("config \"%s\" doesn't match schema", projectState.Naming().ConfigFilePath(config.RelativePath())), err)
		}
	}

	for _, row := range projectState.ConfigRows() {
		// Validate only local files
		if row.Local == nil {
			continue
		}

		component, err := projectState.Components().Get(*row.ComponentKey())
		if err != nil {
			return err
		}

		if err := ValidateConfigRow(component, row.Local); err != nil {
			errors.AppendWithPrefix(fmt.Sprintf("config row \"%s\" doesn't match schema", projectState.Naming().ConfigFilePath(row.RelativePath())), err)
		}
	}

	return errors.ErrorOrNil()
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

	// Skip empty configurations.
	// Users often just create configuration in UI, but leaves it unconfigured.
	if len(parametersMap.Keys()) == 0 {
		return nil
	}

	// Load
	schemaJson, err := json.EncodeString(schema, true)
	if err != nil {
		return utils.PrefixError("cannot encode component schema JSON", err)
	}

	documentJson, err := json.EncodeString(parametersMap, true)
	if err != nil {
		return utils.PrefixError("cannot encode JSON", err)
	}

	schemaLoader := gojsonschema.NewStringLoader(schemaJson)
	documentLoader := gojsonschema.NewStringLoader(documentJson)

	// Validate
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return utils.PrefixError("schema validation error", err)
	}

	if !result.Valid() {
		errors := utils.NewMultiError()
		for _, desc := range result.Errors() {
			errors.Append(fmt.Errorf("%s", strings.TrimPrefix(desc.String(), "(root): ")))
		}
		return errors
	}

	return nil
}
