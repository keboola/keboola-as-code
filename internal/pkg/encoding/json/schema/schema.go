package schema

import (
	"bytes"
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// pseudoSchemaFile - the validated schema is registered as this resource
const pseudoSchemaFile = "/schema.json"

func ValidateObjects(logger log.Logger, objects model.ObjectStates) error {
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
			logger.Warn(errors.PrefixErrorf(schemaErr.Unwrap(), `config JSON schema of the component "%s" is invalid, please contact support`, component.ID))
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
			logger.Warn(errors.PrefixErrorf(schemaErr.Unwrap(), `config row JSON schema of the component "%s" is invalid, please contact support`, component.ID))
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
		return processErrors(validationErrors.Causes)
	} else if err != nil {
		return err
	}
	return nil
}

func validateDocument(schemaStr []byte, document *orderedmap.OrderedMap) error {
	schema, err := compileSchema(schemaStr, false)
	if err != nil {
		msg := strings.TrimPrefix(err.Error(), "jsonschema: invalid json "+pseudoSchemaFile+": ")
		return &SchemaError{error: errors.Wrapf(err, msg)}
	}
	return schema.Validate(document.ToMap())
}

func processErrors(errs []*jsonschema.ValidationError) error {
	// Sort errors
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].InstanceLocation < errs[j].InstanceLocation
	})

	schemaErrs := errors.NewMultiError()
	docErrs := errors.NewMultiError()
	for _, e := range errs {
		isSchemaErr := !strings.HasPrefix(e.AbsoluteKeywordLocation, "file://"+pseudoSchemaFile)
		path := strings.TrimLeft(e.InstanceLocation, "/")
		path = strings.ReplaceAll(path, "/", ".")
		msg := strings.ReplaceAll(e.Message, `'`, `"`)

		switch {
		case len(e.Causes) > 0:
			// Process nested errors
			if err := processErrors(e.Causes); err != nil {
				if e.Message == "" {
					docErrs.Append(err)
				} else {
					docErrs.Append(errors.PrefixError(err, e.Message))
				}
			}
		case isSchemaErr:
			// Required field in a JSON schema should be an array of required nested fields.
			// But, for historical reasons, in Keboola components, "required: true" is also used.
			// In the UI, this causes the drop-down list to not have an empty value.
			// For this reason, we can ignore the error.
			if strings.HasSuffix(e.InstanceLocation, "/required") && e.Message == "expected array, but got boolean" {
				continue
			}
			schemaErrs.Append(errors.Wrapf(e, `"%s" is invalid: %s`, path, e.Message))
		default:
			// Format error
			if path == "" {
				docErrs.Append(&ValidationError{message: msg})
			} else {
				docErrs.Append(&FieldValidationError{path: path, message: msg})
			}
		}
	}

	if schemaErrs.Len() > 0 {
		return &SchemaError{error: schemaErrs}
	}

	return docErrs.ErrorOrNil()
}

func compileSchema(schemaStr []byte, savePropertyOrder bool) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.ExtractAnnotations = true
	if savePropertyOrder {
		registerPropertyOrderExt(c)
	}

	if err := c.AddResource(pseudoSchemaFile, bytes.NewReader(schemaStr)); err != nil {
		return nil, err
	}

	return c.Compile(pseudoSchemaFile)
}
