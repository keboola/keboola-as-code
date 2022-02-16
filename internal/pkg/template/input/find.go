package input

import (
	"math"
	"reflect"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// ObjectField - potential user input in an object.
type ObjectField struct {
	Input
	ObjectKey model.Key
	Path      orderedmap.Key
	Example   string // example value convert to string
	Selected  bool   // pre-selected in the dialog
}

// Find potential user inputs in config or config row.
func Find(objectKey model.Key, componentKey model.ComponentKey, content *orderedmap.OrderedMap) []ObjectField {
	var out []ObjectField
	content.VisitAllRecursive(func(fieldPath orderedmap.Key, value interface{}, parent interface{}) {
		// Root key must be "parameters"
		if len(fieldPath) < 2 || fieldPath.First() != orderedmap.MapStep("parameters") {
			return
		}

		// Must be object field
		fieldKey, isObjectField := fieldPath.Last().(orderedmap.MapStep)
		if !isObjectField {
			return
		}

		isSecret := encryptionapi.IsKeyToEncrypt(string(fieldKey))

		// Detect type, kind and default value
		var inputType Type
		var inputKind Kind
		var inputOptions Options
		var defaultValue interface{}
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
			for i := 0; i < valRef.Len(); i++ {
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
					Id:   item.String(),
					Name: item.String(),
				})
			}
			if !isSecret && valRef.Len() > 0 {
				defaultValue = value
			}
		default:
			return
		}

		// Example
		example := ""
		if !isSecret {
			example = strhelper.Truncate(cast.ToString(value), 20, "...")
		}

		// Add
		out = append(out, ObjectField{
			Input: Input{
				Id:      strhelper.NormalizeName(componentKey.Id.WithoutVendor() + "-" + fieldPath[1:].String()),
				Type:    inputType,
				Kind:    inputKind,
				Default: defaultValue,
				Options: inputOptions,
			},
			ObjectKey: objectKey,
			Path:      fieldPath,
			Example:   example,
			Selected:  inputKind == KindHidden,
		})
	})

	return out
}
