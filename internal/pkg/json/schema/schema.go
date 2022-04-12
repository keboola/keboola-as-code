package schema

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func ValidateSchemas(objects model.Objects, components *model.ComponentsMap, namingRegistry *naming.Registry) error {
	errs := errors.NewMultiError()

	for _, config := range objects.ConfigsWithRows() {
		component, err := components.Get(config.ComponentKey())
		if err != nil {
			return err
		}

		if err := ValidateConfig(component, config.Config); err != nil {
			desc := config.String()
			if path, found := namingRegistry.PathByKey(config.Key()); found {
				desc = fmt.Sprintf(`%s "%s"`, config.Kind().Name, filesystem.Join(path.String(), naming.ConfigFile))
			}
			errs.AppendWithPrefix(fmt.Sprintf("%s doesn't match schema", desc), err)
		}

		for _, row := range config.Rows {
			if err := ValidateConfigRow(component, row); err != nil {
				desc := row.String()
				if path, found := namingRegistry.PathByKey(row.Key()); found {
					desc = fmt.Sprintf(`%s "%s"`, row.Kind().Name, filesystem.Join(path.String(), naming.ConfigFile))
				}
				errs.AppendWithPrefix(fmt.Sprintf("%s doesn't match schema", desc), err)
			}
		}
	}

	return errs.ErrorOrNil()
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
	errs := errors.NewMultiError()
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

func processErrors(errs []*jsonschema.ValidationError, output *errors.MultiError) {
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
