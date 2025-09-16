package input

import (
	"math"
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// ObjectField - potential user input in an object.
type ObjectField struct {
	Input
	ObjectKey model.Key
	Path      orderedmap.Path
	Example   string // example value convert to string
	Selected  bool   // pre-selected in the dialog
}

// Find potential user inputs in config or config row.
func Find(objectKey model.Key, component *keboola.Component, content *orderedmap.OrderedMap) []ObjectField {
	var out []ObjectField
	content.VisitAllRecursive(func(fieldPath orderedmap.Path, value any, parent any) {
		// Root key must be "parameters"
		if len(fieldPath) < 2 || fieldPath.First() != orderedmap.MapStep("parameters") {
			return
		}

		// Must be object field
		fieldKey, isObjectField := fieldPath.Last().(orderedmap.MapStep)
		if !isObjectField {
			return
		}

		// Generate input ID
		inputID := strhelper.NormalizeName(component.ID.WithoutVendor() + "-" + fieldPath.WithoutFirst().String())

		// Detect type, kind and default value
		var inputType Type
		var inputKind Kind
		var inputOptions Options
		var defaultValue any
		isSecret := keboola.IsKeyToEncrypt(string(fieldKey))
		valRef := reflect.ValueOf(value)
		switch valRef.Kind() {
		case reflect.String:
			inputType = TypeString
			if isSecret {
				inputKind = KindHidden
			} else {
				inputKind = KindInput
			}

			// Use as default value, if it is not a secret
			if !isSecret && len(value.(string)) > 0 {
				defaultValue = value
			}
		case reflect.Int:
			inputType = TypeInt
			inputKind = KindInput
			if !isSecret && value.(int) != 0 {
				defaultValue = value
			}
		case reflect.Float64:
			valueFloat := value.(float64)
			isWholeNumber := math.Trunc(valueFloat) == valueFloat
			if isWholeNumber {
				// Whole number? Use TypeInt.
				// All numeric values from a JSON are float64.
				inputType = TypeInt
				inputKind = KindInput
				if !isSecret && valueFloat != 0.0 {
					defaultValue = int(valueFloat)
				}
			} else {
				inputType = TypeDouble
				inputKind = KindInput
				if !isSecret && valueFloat != 0.0 {
					defaultValue = value
				}
			}
		case reflect.Bool:
			inputType = TypeBool
			inputKind = KindConfirm
			defaultValue = value
		case reflect.Slice:
			inputType = TypeStringArray
			inputKind = KindMultiSelect
			// Each element must be string
			for i := range valRef.Len() {
				item := valRef.Index(i)
				// Unwrap interface
				if item.Type().Kind() == reflect.Interface {
					item = item.Elem()
				}
				// Check item type
				if itemKind := item.Kind(); itemKind != reflect.String {
					// Item is not string -> value is not array of strings
					return
				}
				inputOptions = append(inputOptions, Option{
					Value: item.String(),
					Label: item.String(),
				})
			}
			if !isSecret && valRef.Len() > 0 {
				// Normalize default to []any of strings for consistency across the codebase
				defaults := make([]any, 0, valRef.Len())
				for i := 0; i < valRef.Len(); i++ {
					item := valRef.Index(i)
					if item.Type().Kind() == reflect.Interface {
						item = item.Elem()
					}
					defaults = append(defaults, item.String())
				}
				defaultValue = defaults
			}
		default:
			return
		}

		// Example
		example := ""
		if !isSecret {
			example = strhelper.Truncate(cast.ToString(value), 20, "...")
		}

		// Create input definition
		inputDef := Input{
			ID:      inputID,
			Type:    inputType,
			Kind:    inputKind,
			Default: defaultValue,
			Options: inputOptions,
		}

		// Fill in metadata from component JSON schema
		if meta, found, _ := schema.FieldMeta(component.Schema, fieldPath); found {
			inputDef.Name = meta.Title
			inputDef.Description = meta.Description
			if v, _ := inputDef.Type.ParseValue(meta.Default); v != "" {
				inputDef.Default = v
			}
		}

		// Use generic name if needed
		if inputDef.Name == "" {
			var parts []string
			for _, step := range fieldPath.WithoutFirst() {
				if v, ok := step.(orderedmap.MapStep); ok {
					part := v.Key()
					part = regexpcache.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(part, " ") // remove special chars
					part = strings.TrimSpace(part)
					part = strhelper.FirstUpper(part)
					parts = append(parts, part)
				}
			}
			inputDef.Name = strings.TrimSpace(strings.Join(parts, " "))
		}

		// Add
		out = append(out, ObjectField{
			Input:     inputDef,
			ObjectKey: objectKey,
			Path:      fieldPath,
			Example:   example,
			Selected:  inputKind == KindHidden,
		})
	})

	return out
}
