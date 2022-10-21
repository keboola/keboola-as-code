package schema

import (
	"bytes"
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func ValidateSchemas(objects model.ObjectStates) error {
	errs := errors.NewMultiError()
	for _, config := range objects.Configs() {
		// Validate only local files
		if config.Local == nil {
			continue
		}

		component, err := objects.Components().GetOrErr(config.ComponentId)
		if err != nil {
			return err
		}

		if err := ValidateConfig(component, config.Local); err != nil {
			errs.AppendWithPrefixf(err, "config \"%s\" doesn't match schema", filesystem.Join(config.Path(), naming.ConfigFile))
		}
	}

	for _, row := range objects.ConfigRows() {
		// Validate only local files
		if row.Local == nil {
			continue
		}

		component, err := objects.Components().GetOrErr(row.ComponentId)
		if err != nil {
			return err
		}

		if err := ValidateConfigRow(component, row.Local); err != nil {
			errs.AppendWithPrefixf(err, "config row \"%s\" doesn't match schema", filesystem.Join(row.Path(), naming.ConfigFile))
		}
	}

	return errs.ErrorOrNil()
}

func ValidateConfig(component *storageapi.Component, config *model.Config) error {
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	return validateContent(component.Schema, config.Content)
}

func ValidateConfigRow(component *storageapi.Component, configRow *model.ConfigRow) error {
	// Skip deprecated component
	if component.IsDeprecated() {
		return nil
	}
	return validateContent(component.SchemaRow, configRow.Content)
}

func validateContent(schema []byte, content *orderedmap.OrderedMap) error {
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
		return errors.Errorf(`invalid JSON schema: %w`, err)
	}
	return schema.Validate(document.ToMap())
}

func processErrors(schemaErrs []*jsonschema.ValidationError) error {
	// Sort errors
	sort.Slice(schemaErrs, func(i, j int) bool {
		return schemaErrs[i].InstanceLocation < schemaErrs[j].InstanceLocation
	})

	out := errors.NewMultiError()
	for _, e := range schemaErrs {
		// Process nested errors
		if len(e.Causes) > 0 {
			if err := processErrors(e.Causes); err != nil {
				out.Append(err)
			}
			continue
		}

		// Format error
		path := strings.TrimLeft(e.InstanceLocation, "/")
		path = strings.ReplaceAll(path, "/", ".")
		msg := strings.ReplaceAll(e.Message, `'`, `"`)
		if path == "" {
			out.Append(&ValidationError{message: msg})
		} else {
			out.Append(&FieldValidationError{path: path, message: msg})
		}
	}
	return out.ErrorOrNil()
}

func compileSchema(schemaStr []byte, savePropertyOrder bool) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.ExtractAnnotations = true
	if savePropertyOrder {
		registerPropertyOrderExt(c)
	}

	if err := c.AddResource("schema.json", bytes.NewReader(schemaStr)); err != nil {
		return nil, err
	}

	return c.Compile("schema.json")
}
