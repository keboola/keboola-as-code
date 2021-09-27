package schema

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/qri-io/jsonschema"

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
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	return validateContent(component.Schema, config.Content)
}

func ValidateConfigRow(component *model.Component, configRow *model.ConfigRow) error {
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	return validateContent(component.SchemaRow, configRow.Content)
}

func validateContent(schema map[string]interface{}, content *orderedmap.OrderedMap) error {
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

	// Validate
	schemaErrs, err := validateDocument(schema, parametersMap)

	// Internal error?
	if err != nil {
		return err
	}

	// All OK?
	if len(schemaErrs) == 0 {
		return nil
	}

	// Sort errors
	sort.Slice(schemaErrs, func(i, j int) bool {
		return schemaErrs[i].PropertyPath < schemaErrs[j].PropertyPath
	})

	// Process schema errors
	errors := utils.NewMultiError()
	for _, err := range schemaErrs {
		propertyPath := strings.TrimLeft(err.PropertyPath, "/")
		propertyPath = strings.ReplaceAll(propertyPath, "/", ".")
		if propertyPath == "" {
			errors.Append(fmt.Errorf(`%s`, err.Message))
		} else {
			errors.Append(fmt.Errorf(`"%s": %s`, propertyPath, err.Message))
		}
	}
	return errors
}

func validateDocument(schemaMap map[string]interface{}, document interface{}) ([]jsonschema.KeyError, error) {
	// Set root level type to object if it is missing.
	if _, ok := schemaMap["type"]; !ok {
		schemaMap["type"] = "object"
	}

	// Convert schema to struct
	schemaJson := json.MustEncodeString(schemaMap, false)
	schema := &jsonschema.Schema{}
	err := json.DecodeString(schemaJson, schema)
	if err != nil {
		return nil, fmt.Errorf(`invalid JSON schema: %w`, err)
	}
	schemaErrs, err := schema.ValidateBytes(context.Background(), json.MustEncode(document, true))
	if err != nil {
		return nil, fmt.Errorf(`invalid JSON schema: %w`, err)
	}
	return schemaErrs, nil
}
