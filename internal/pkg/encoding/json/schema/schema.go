package schema

import (
	"context"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// pseudoSchemaFile - the validated schema is registered as this resource.
const pseudoSchemaFile = "file:///schema.json"

func ValidateObjects(ctx context.Context, logger log.Logger, objects model.ObjectStates) error {
	errs := errors.NewMultiError()
	for _, config := range objects.Configs() {
		// Validate only local files
		if config.Local == nil {
			continue
		}

		component, err := objects.Components().GetOrErr(config.ComponentID)
		if err != nil {
			return err
		}

		var schemaErr *SchemaError
		if err := ValidateConfig(component, config.Local); errors.As(err, &schemaErr) {
			logger.Warn(ctx, errors.PrefixErrorf(schemaErr.Unwrap(), `config JSON schema of the component "%s" is invalid, please contact support`, component.ID).Error())
		} else if err != nil {
			errs.AppendWithPrefixf(err, "config \"%s\" doesn't match schema", filesystem.Join(config.Path(), naming.ConfigFile))
		}
	}

	for _, row := range objects.ConfigRows() {
		// Validate only local files
		if row.Local == nil {
			continue
		}

		component, err := objects.Components().GetOrErr(row.ComponentID)
		if err != nil {
			return err
		}

		var schemaErr *SchemaError
		if err := ValidateConfigRow(component, row.Local); errors.As(err, &schemaErr) {
			logger.Warn(ctx, errors.PrefixErrorf(schemaErr.Unwrap(), `config row JSON schema of the component "%s" is invalid, please contact support`, component.ID).Error())
		} else if err != nil {
			errs.AppendWithPrefixf(err, "config row \"%s\" doesn't match schema", filesystem.Join(row.Path(), naming.ConfigFile))
		}
	}

	return errs.ErrorOrNil()
}

func ValidateConfig(component *keboola.Component, config *model.Config) error {
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	// Skip components with custom schema handling
	if skipSchemaValidation(component.ID) {
		return nil
	}
	return ValidateContent(component.Schema, config.Content)
}

func ValidateConfigRow(component *keboola.Component, configRow *model.ConfigRow) error {
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	// Skip components with custom schema handling
	if skipSchemaValidation(component.ID) {
		return nil
	}
	return ValidateContent(component.SchemaRow, configRow.Content)
}

func ValidateContent(schema []byte, content *orderedmap.OrderedMap) error {
	schema, err := NormalizeSchema(schema)
	if err != nil {
		return err
	}

	// Get parameters key
	var parametersMap *orderedmap.OrderedMap
	parameters, found := content.Get("parameters")
	if found {
		if v, ok := parameters.(*orderedmap.OrderedMap); ok {
			parametersMap = v
		} else {
			parametersMap = orderedmap.New()
		}
	} else {
		parametersMap = orderedmap.New()
	}

	// Skip empty configurations.
	// Users often just create configuration in UI, but leaves it unconfigured.
	if len(parametersMap.Keys()) == 0 {
		return nil
	}

	// Validate
	err = validateDocument(schema, parametersMap)

	// Process schema errors
	validationError := &jsonschema.ValidationError{}
	if errors.As(err, &validationError) {
		return processErrors(validationError.DetailedOutput().Errors)
	} else if err != nil {
		return err
	}
	return nil
}

// conditionalRequirement represents a field that is conditionally required based on options.dependencies.
type conditionalRequirement struct {
	fieldName    string         // The field that is conditionally required
	dependencies map[string]any // The dependency conditions (e.g., {"append_date": 1})
}

func NormalizeSchema(schema []byte) ([]byte, error) {
	// Decode JSON
	m := orderedmap.New()
	if err := json.Decode(schema, &m); err != nil {
		return nil, err
	}

	// Collect conditional requirements per parent object path
	// Key is the string representation of the parent path (the object containing the properties)
	conditionalReqs := make(map[string][]conditionalRequirement)

	m.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		lastStep := path.Last()

		// Required field in a JSON schema should be an array of required nested fields.
		// But, for historical reasons, in Keboola components, "required: true" and "required: false" are also used.
		// In the UI, this causes the drop-down list to not have an empty value, so the error should be ignored.
		if lastStep == orderedmap.MapStep("required") {
			if _, ok := value.(bool); ok {
				if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
					parentMap.Delete("required")
				}
			}
			return
		}

		// Empty enums are removed, we're using those for asynchronously loaded enums.
		if lastStep == orderedmap.MapStep("enum") {
			if arr, ok := value.([]any); ok && len(arr) == 0 {
				if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
					parentMap.Delete("enum")
				}
			}
			return
		}

		// Handle options.dependencies - collect conditional requirements
		// Path pattern: .../properties/<fieldName>/options/dependencies
		if lastStep != orderedmap.MapStep("dependencies") {
			return
		}
		pathLen := len(path)
		if pathLen < 4 {
			return
		}
		if path[pathLen-2] != orderedmap.MapStep("options") {
			return
		}
		fieldStep, ok := path[pathLen-3].(orderedmap.MapStep)
		if !ok {
			return
		}
		if path[pathLen-4] != orderedmap.MapStep("properties") {
			return
		}
		depsMap, ok := value.(*orderedmap.OrderedMap)
		if !ok {
			return
		}
		deps := make(map[string]any)
		for _, key := range depsMap.Keys() {
			depValue, _ := depsMap.Get(key)
			deps[key] = depValue
		}
		if len(deps) == 0 {
			return
		}
		parentPath := path[:pathLen-4]
		parentPathStr := parentPath.String()
		conditionalReqs[parentPathStr] = append(conditionalReqs[parentPathStr], conditionalRequirement{
			fieldName:    fieldStep.Key(),
			dependencies: deps,
		})
	})

	// Process conditional requirements: remove from required arrays and add if/then/else constructs
	for parentPathStr, reqs := range conditionalReqs {
		parentObj := getObjectAtPath(m, parentPathStr)
		if parentObj == nil {
			continue
		}

		// Remove conditionally required fields from the required array
		if requiredVal, found := parentObj.Get("required"); found {
			if requiredArr, ok := requiredVal.([]any); ok {
				newRequired := make([]any, 0, len(requiredArr))
				conditionalFields := make(map[string]bool)
				for _, req := range reqs {
					conditionalFields[req.fieldName] = true
				}
				for _, field := range requiredArr {
					if fieldStr, ok := field.(string); ok {
						if !conditionalFields[fieldStr] {
							newRequired = append(newRequired, field)
						}
					} else {
						newRequired = append(newRequired, field)
					}
				}
				if len(newRequired) > 0 {
					parentObj.Set("required", newRequired)
				} else {
					parentObj.Delete("required")
				}
			}
		}

		// Generate if/then/else constructs for each conditional requirement
		allOfItems := make([]any, 0, len(reqs))
		for _, req := range reqs {
			ifThenElse := buildIfThenElse(req)
			if ifThenElse != nil {
				allOfItems = append(allOfItems, ifThenElse)
			}
		}

		// Add allOf with if/then/else constructs to the parent object
		if len(allOfItems) > 0 {
			// Check if allOf already exists
			if existingAllOf, found := parentObj.Get("allOf"); found {
				if existingArr, ok := existingAllOf.([]any); ok {
					allOfItems = append(existingArr, allOfItems...)
				}
			}
			parentObj.Set("allOf", allOfItems)
		}
	}

	// Encode back to JSON
	normalized, err := json.Encode(m, false)
	if err != nil {
		return nil, err
	}

	return normalized, nil
}

// getObjectAtPath returns the orderedmap at the given path string.
func getObjectAtPath(m *orderedmap.OrderedMap, pathStr string) *orderedmap.OrderedMap {
	if pathStr == "" {
		return m
	}

	// Parse path string and navigate to the object
	// Path format: key1.key2.key3 (dot-separated)
	parts := strings.Split(pathStr, ".")
	current := m
	for _, part := range parts {
		if part == "" {
			continue
		}
		val, found := current.Get(part)
		if !found {
			return nil
		}
		nextMap, ok := val.(*orderedmap.OrderedMap)
		if !ok {
			return nil
		}
		current = nextMap
	}
	return current
}

// buildIfThenElse creates an if/then/else construct for a conditional requirement.
func buildIfThenElse(req conditionalRequirement) *orderedmap.OrderedMap {
	if len(req.dependencies) == 0 {
		return nil
	}

	// Build the "if" condition properties
	ifProperties := orderedmap.New()
	for depField, depValue := range req.dependencies {
		condition := orderedmap.New()
		// Handle array values (e.g., "protocol": ["FTP", "FTPS"]) using enum
		// Handle single values using const
		if arr, ok := depValue.([]any); ok {
			condition.Set("enum", arr)
		} else {
			condition.Set("const", depValue)
		}
		ifProperties.Set(depField, condition)
	}

	// Build the "if" clause
	ifClause := orderedmap.New()
	ifClause.Set("properties", ifProperties)

	// Build the "then" clause with required field
	thenClause := orderedmap.New()
	thenClause.Set("required", []any{req.fieldName})

	// Build the complete if/then construct
	result := orderedmap.New()
	result.Set("if", ifClause)
	result.Set("then", thenClause)

	return result
}

func validateDocument(schemaStr []byte, document *orderedmap.OrderedMap) error {
	schema, err := compileSchema(schemaStr, false)
	if err != nil {
		msg := strings.TrimPrefix(err.Error(), "jsonschema: invalid json "+pseudoSchemaFile+": ")
		// nolint: govet
		return &SchemaError{error: errors.Wrap(err, msg)}
	}
	return schema.Validate(document.ToMap())
}

func processErrors(errs []jsonschema.OutputUnit) error {
	// Sort errors
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].InstanceLocation < errs[j].InstanceLocation
	})

	docErrs := errors.NewMultiError()
	for _, e := range errs {
		errMsg := ""
		if e.Error != nil {
			errMsg = e.Error.String()
		}

		path := strings.TrimLeft(e.InstanceLocation, "/")
		path = strings.ReplaceAll(path, "/", ".")
		msg := strings.ReplaceAll(strings.ReplaceAll(errMsg, `'`, `"`), `n"t`, `n't`)

		var formattedErr error
		switch {
		case len(e.Errors) > 0:
			// Process nested errors.
			if err := processErrors(e.Errors); err != nil {
				formattedErr = err
			}
		default:
			// Format error
			if path == "" {
				formattedErr = &ValidationError{message: msg}
			} else {
				formattedErr = &FieldValidationError{path: path, message: msg}
			}
		}

		if formattedErr != nil {
			docErrs.Append(formattedErr)
		}
	}

	return docErrs.ErrorOrNil()
}

func compileSchema(s []byte, savePropertyOrder bool) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	if savePropertyOrder {
		vocabulary, err := buildPropertyOrderVocabulary()
		if err != nil {
			return nil, err
		}
		c.RegisterVocabulary(vocabulary)
	}

	// Decode JSON
	m := orderedmap.New()
	if err := json.Decode(s, &m); err != nil {
		return nil, err
	}

	// Remove non-standard definitions, where type = button.
	// It is used to test_connection and sync_action definitions.
	// It has no value for validation, it is a UI definition only.
	m.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		if path.Last() == orderedmap.MapStep("type") && value == "button" {
			if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
				parentMap.Delete("type")
			}
		}
	})

	decoded, err := jsonschema.UnmarshalJSON(strings.NewReader(json.MustEncodeString(m, false)))
	if err != nil {
		return nil, err
	}

	if err := c.AddResource(pseudoSchemaFile, decoded); err != nil {
		return nil, err
	}

	schema, err := c.Compile(pseudoSchemaFile)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

// skipSchemaValidation returns true for components where schema validation should be skipped.
func skipSchemaValidation(componentID keboola.ComponentID) bool {
	switch componentID {
	case "keboola.python-transformation-v2",
		"keboola.snowflake-transformation",
		"keboola.google-bigquery-transformation":
		return true
	default:
		return false
	}
}
