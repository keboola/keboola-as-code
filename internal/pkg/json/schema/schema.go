package schema

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func ValidateSchemas(objects model.ObjectStates) error {
	errs := utils.NewMultiError()
	for _, config := range objects.Configs() {
		// Validate only local files
		if config.Local == nil {
			continue
		}

		component, found := objects.Components().Get(config.ComponentId)
		if !found {
			return fmt.Errorf(`component "%s" not found`, config.ComponentId)
		}

		if err := ValidateConfig(component, config.Local); err != nil {
			errs.AppendWithPrefix(fmt.Sprintf("config \"%s\" doesn't match schema", filesystem.Join(config.Path(), naming.ConfigFile)), err)
		}
	}

	for _, row := range objects.ConfigRows() {
		// Validate only local files
		if row.Local == nil {
			continue
		}

		component, found := objects.Components().Get(row.ComponentId)
		if !found {
			return fmt.Errorf(`component "%s" not found`, row.ComponentId)
		}

		if err := ValidateConfigRow(component, row.Local); err != nil {
			errs.AppendWithPrefix(fmt.Sprintf("config row \"%s\" doesn't match schema", filesystem.Join(row.Path(), naming.ConfigFile)), err)
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
	errs := utils.NewMultiError()
	if errors.As(err, &validationErrors) {
		processErrors(validationErrors.Causes, errs)
	} else if err != nil {
		errs.Append(err)
	}

	return errs.ErrorOrNil()
}

func validateDocument(schemaStr []byte, document *orderedmap.OrderedMap) error {
	schema, err := compileSchema(schemaStr, false)
	if err != nil {
		return fmt.Errorf(`invalid JSON schema: %w`, err)
	}
	return schema.Validate(document.ToMap())
}

func processErrors(errs []*jsonschema.ValidationError, output *utils.MultiError) {
	// Sort errors
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].InstanceLocation < errs[j].InstanceLocation
	})

	for _, e := range errs {
		// Process nested errors
		if len(e.Causes) > 0 {
			processErrors(e.Causes, output)
			continue
		}

		// Format error
		path := strings.TrimLeft(e.InstanceLocation, "/")
		path = strings.ReplaceAll(path, "/", ".")
		msg := strings.ReplaceAll(e.Message, `'`, `"`)
		if path == "" {
			output.Append(fmt.Errorf(`%s`, msg))
		} else {
			output.Append(fmt.Errorf(`"%s": %s`, path, msg))
		}
	}
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
