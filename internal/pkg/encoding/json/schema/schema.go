package schema

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/santhosh-tekuri/jsonschema/v5"

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
	return ValidateContent(component.Schema, config.Content)
}

func ValidateConfigRow(component *keboola.Component, configRow *model.ConfigRow) error {
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	return ValidateContent(component.SchemaRow, configRow.Content)
}

func ValidateContent(schema []byte, content *orderedmap.OrderedMap) error {
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
	err := validateDocument(schema, parametersMap)

	// Process schema errors
	validationErrors := &jsonschema.ValidationError{}
	if errors.As(err, &validationErrors) {
		return processErrors(validationErrors.Causes, false)
	} else if err != nil {
		return err
	}
	return nil
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

func processErrors(errs []*jsonschema.ValidationError, parentIsSchemaErr bool) error {
	// Sort errors
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].InstanceLocation < errs[j].InstanceLocation
	})

	schemaErrs := errors.NewMultiError()
	docErrs := errors.NewMultiError()
	for _, e := range errs {
		// Schema error does not start with our pseudo schema file.
		isSchemaErr := !strings.HasPrefix(e.AbsoluteKeywordLocation, pseudoSchemaFile)
		path := strings.TrimLeft(e.InstanceLocation, "/")
		path = strings.ReplaceAll(path, "/", ".")
		msg := strings.ReplaceAll(strings.ReplaceAll(e.Message, `'`, `"`), `n"t`, `n't`)

		var formattedErr error
		switch {
		case len(e.Causes) > 0:
			// Process nested errors.
			if err := processErrors(e.Causes, isSchemaErr || parentIsSchemaErr); err != nil {
				if e.Message == "" || e.Message == "doesn't validate with ''" || e.Message == `'' is invalid:` {
					formattedErr = err
				} else {
					formattedErr = errors.PrefixError(err, msg)
				}
			}
		case isSchemaErr:
			// Required field in a JSON schema should be an array of required nested fields.
			// But, for historical reasons, in Keboola components, "required: true" is also used.
			// In the UI, this causes the drop-down list to not have an empty value, so the error should be ignored.
			if strings.HasSuffix(e.InstanceLocation, "/required") && e.Message == "expected array, but got boolean" {
				continue
			}
			// JSON schema may contain empty enums, in dynamic selects.
			if strings.HasSuffix(e.InstanceLocation, "/enum") && e.Message == "minimum 1 items required, but found 0 items" {
				continue
			}
			formattedErr = errors.Wrapf(e, `"%s" is invalid: %s`, path, e.Message)
		default:
			// Format error
			if path == "" {
				formattedErr = &ValidationError{message: msg}
			} else {
				formattedErr = &FieldValidationError{path: path, message: msg}
			}
		}

		if formattedErr != nil {
			if isSchemaErr {
				schemaErrs.Append(formattedErr)
			} else {
				docErrs.Append(formattedErr)
			}
		}
	}

	// Errors in the schema have priority, they will be written to the user as a warning.
	if schemaErrs.Len() > 0 {
		if parentIsSchemaErr {
			// Only parent schema error is wrapped to the SchemaError type, nested errors are not.
			return schemaErrs
		}
		return &SchemaError{error: schemaErrs}
	}

	return docErrs.ErrorOrNil()
}

func compileSchema(s []byte, savePropertyOrder bool) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.ExtractAnnotations = true
	if savePropertyOrder {
		registerPropertyOrderExt(c)
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

	if err := c.AddResource(pseudoSchemaFile, bytes.NewReader(json.MustEncode(m, false))); err != nil {
		return nil, err
	}

	schema, err := c.Compile(pseudoSchemaFile)
	if err != nil {
		return nil, err
	}

	return schema, nil
}
