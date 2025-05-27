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
	validationError := &jsonschema.ValidationError{}
	if errors.As(err, &validationError) {
		return processErrors(validationError.DetailedOutput().Errors)
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
