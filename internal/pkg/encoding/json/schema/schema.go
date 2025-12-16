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

// Components with schema validation skipped
var skipSchemaValidationComponents = map[keboola.ComponentID]bool{
	"keboola.python-transformation-v2":       true,
	"keboola.snowflake-transformation":       true,
	"keboola.google-bigquery-transformation": true,
}

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
	if skipSchemaValidationComponents[component.ID] {
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
	if skipSchemaValidationComponents[component.ID] {
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

func NormalizeSchema(schema []byte) ([]byte, error) {
	// Decode JSON
	m := orderedmap.New()
	if err := json.Decode(schema, &m); err != nil {
		return nil, err
	}

	m.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		// Required field in a JSON schema should be an array of required nested fields.
		// But, for historical reasons, in Keboola components, "required: true" and "required: false" are also used.
		// In the UI, this causes the drop-down list to not have an empty value, so the error should be ignored.
		if path.Last() == orderedmap.MapStep("required") {
			if _, ok := value.(bool); ok {
				if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
					parentMap.Delete("required")
				}
			}
		}

		// Empty enums are removed, we're using those for asynchronously loaded enums.
		if path.Last() == orderedmap.MapStep("enum") {
			if arr, ok := value.([]any); ok && len(arr) == 0 {
				if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
					parentMap.Delete("enum")
				}
			}
		}
	})

	// Encode back to JSON
	normalized, err := json.Encode(m, false)
	if err != nil {
		return nil, err
	}

	return normalized, nil
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
