package schema

import (
	"context"
	"reflect"
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

func ValidateConfig(component *keboola.Component, cfg *model.Config) error {
	if component.Schema == nil {
		return nil
	}
	if err := validateDocument(component.Schema, cfg.Content); err != nil {
		return err
	}
	return nil
}

func ValidateConfigRow(component *keboola.Component, row *model.ConfigRow) error {
	if len(component.SchemaRow) == 0 {
		return nil
	}
	if err := validateDocument(component.SchemaRow, row.Content); err != nil {
		return err
	}
	return nil
}

// ValidateContent validates the given content using provided JSON schema.
// It keeps compatibility with historical behavior: validates only the "parameters" object
// and ignores empty parameters maps.
func ValidateContent(sch []byte, content *orderedmap.OrderedMap) error {
	// Normalize schema first
	normalized, err := NormalizeSchema(sch)
	if err != nil {
		return err
	}

	// Get parameters map
	parametersMap := orderedmap.New()
	if v, found := content.Get("parameters"); found {
		if m, ok := v.(*orderedmap.OrderedMap); ok {
			parametersMap = m
		}
	}

	// Skip empty configurations
	if len(parametersMap.Keys()) == 0 {
		return nil
	}

	// Validate document
	err = validateDocument(normalized, parametersMap)
	if err == nil {
		return nil
	}

	// Process schema errors
	var vErr *jsonschema.ValidationError
	if errors.As(err, &vErr) {
		return processErrors(vErr.DetailedOutput().Errors)
	}
	return err
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
				} else if parentStd, ok := parent.(map[string]any); ok {
					delete(parentStd, "required")
				}
			}
		}

		// Empty enums are removed, we're using those for asynchronously loaded enums.
		if path.Last() == orderedmap.MapStep("enum") {
			isEmptySlice := false
			if arr, ok := value.([]any); ok && len(arr) == 0 {
				isEmptySlice = true
			} else {
				rv := reflect.ValueOf(value)
				if rv.IsValid() && rv.Kind() == reflect.Slice && rv.Len() == 0 {
					isEmptySlice = true
				}
			}
			if isEmptySlice {
				if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
					parentMap.Delete("enum")
				} else if parentStd, ok := parent.(map[string]any); ok {
					delete(parentStd, "enum")
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
		// Normalize trailing punctuation from upstream errors
		msg = strings.TrimSuffix(msg, ".")

		var formattedErr error
		switch {
		case len(e.Errors) > 0:
			// Process nested errors.
			if err := processErrors(e.Errors); err != nil {
				formattedErr = err
			}
		default:
			// Format error: prefer "missing property \"name\"" exact wording
			if strings.HasPrefix(msg, "at '") || strings.HasPrefix(msg, "at \"") {
				// remove any leading location like: at '': ... or at 'path': ...
				if idx := strings.Index(msg, ": "); idx != -1 {
					msg = msg[idx+2:]
				}
			}
			if strings.HasPrefix(msg, "missing property ") {
				// when missing property and path empty -> keep simple form
				if path == "" {
					formattedErr = &ValidationError{message: msg}
				} else {
					formattedErr = &FieldValidationError{path: path, message: msg}
				}
			} else if path == "" {
				formattedErr = &ValidationError{message: msg}
			} else {
				formattedErr = &FieldValidationError{path: path, message: msg}
			}
		}

		if formattedErr != nil {
			docErrs.Append(formattedErr)
		}
	}

	if docErrs.Len() == 0 {
		return nil
	}
	return docErrs
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

	// Remove non-standard definitions
	m.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		// UI only: remove fields with type "button"
		if path.Last() == orderedmap.MapStep("type") && value == "button" {
			if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
				parentMap.Delete("type")
			} else if parentStd, ok := parent.(map[string]any); ok {
				delete(parentStd, "type")
			}
		}

		// Non-standard keyword used in some schemas; ignore it for validation
		if path.Last() == orderedmap.MapStep("jsonType") {
			if parentMap, ok := parent.(*orderedmap.OrderedMap); ok {
				parentMap.Delete("jsonType")
			} else if parentStd, ok := parent.(map[string]any); ok {
				delete(parentStd, "jsonType")
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
