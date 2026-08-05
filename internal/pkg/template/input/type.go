package input

import (
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	TypeString      = Type("string")
	TypeInt         = Type("int")
	TypeDouble      = Type("double")
	TypeBool        = Type("bool")
	TypeStringArray = Type("string[]")
	TypeObject      = Type("object")
)

// Type of the template user
// This corresponds to the data type that will be used in the Jsonnet template.
type Type string

type Types []Type

func allTypes() Types {
	return Types{TypeString, TypeInt, TypeDouble, TypeBool, TypeStringArray, TypeObject}
}

func (v Types) String() string {
	return strings.Join(v.Strings(), ", ")
}

func (v Types) Strings() []string {
	out := make([]string, len(v))
	for i, item := range v {
		out[i] = string(item)
	}
	return out
}

// EmptyValue returns empty value for the type.
func (t Type) EmptyValue() any {
	switch t {
	case TypeString:
		return ""
	case TypeInt:
		return 0
	case TypeDouble:
		return 0.0
	case TypeBool:
		return false
	case TypeStringArray:
		return []any{}
	case TypeObject:
		return make(map[string]any)
	default:
		panic(errors.Errorf(`unexpected input type "%s"`, t))
	}
}

// ValidateValue user input or defined default value.
func (t Type) ValidateValue(value reflect.Value) error {
	valueKind := value.Kind()
	kindStr := reflectKindToStr(valueKind)

	switch t {
	case TypeString:
		if valueKind != reflect.String {
			return errors.Errorf("should be string, got %s", kindStr)
		}
	case TypeInt:
		if valueKind != reflect.Int && !(valueKind == reflect.Float64 && math.Trunc(value.Float()) == value.Float()) {
			return errors.Errorf("should be int, got %s", kindStr)
		}
	case TypeDouble:
		if valueKind != reflect.Float64 {
			return errors.Errorf("should be double, got %s", kindStr)
		}
	case TypeBool:
		if valueKind != reflect.Bool {
			return errors.Errorf("should be bool, got %s", kindStr)
		}
	case TypeStringArray:
		if valueKind != reflect.Slice {
			// Must be a slice
			return errors.Errorf("should be array, got %s", kindStr)
		} else {
			// Each element must be string
			for i := range value.Len() {
				item := value.Index(i)
				// Unwrap interface
				if item.Type().Kind() == reflect.Interface {
					item = item.Elem()
				}
				// Check item type
				if itemKind := item.Kind(); itemKind != reflect.String {
					return errors.Errorf("all items should be string, got %s, index %d", reflectKindToStr(itemKind), i)
				}
			}
		}
	case TypeObject:
		if valueKind != reflect.Map {
			return errors.Errorf("should be object, got %s", kindStr)
		}
	default:
		panic(errors.Errorf(`unexpected input type "%s"`, t))
	}

	return nil
}

func (t Type) ParseValue(value any) (any, error) {
	switch t {
	case TypeInt:
		// Empty string
		if value == "" {
			return 0, nil
		}
		// Int
		if v, ok := value.(int); ok {
			return v, nil
		}
		// Float whole number to int
		if v, ok := value.(float64); ok && math.Trunc(value.(float64)) == value.(float64) {
			return int(v), nil
		}
		// String to int
		if v, ok := value.(string); ok {
			if v, err := strconv.Atoi(v); err == nil {
				return v, nil
			}
		}
		return nil, errors.Errorf(`value "%v" is not integer`, value)
	case TypeDouble:
		// Empty string
		if value == "" {
			return 0.0, nil
		}
		// Float
		if v, ok := value.(float64); ok {
			return v, nil
		}
		// Int -> float
		if v, ok := value.(int); ok {
			return float64(v), nil
		}
		// String to float
		if v, ok := value.(string); ok {
			if v, err := strconv.ParseFloat(v, 64); err == nil {
				return v, nil
			}
		}
		return nil, errors.Errorf(`value "%v" is not float`, value)
	case TypeBool:
		if value == "" {
			return false, nil
		}
		if v, ok := value.(bool); ok {
			return v, nil
		}
		if v, err := strconv.ParseBool(cast.ToString(value)); err == nil {
			return v, nil
		}
		return nil, errors.Errorf(`value "%v" is not bool`, value)
	case TypeString:
		return cast.ToString(value), nil
	case TypeStringArray:
		if v, ok := value.(string); ok {
			// Split items by comma, if needed
			var items []string
			if v != "" {
				for item := range strings.SplitSeq(v, ",") {
					items = append(items, strings.TrimSpace(item))
				}
			}
			value = items
		}

		// Accepts either []string (Go-native default) or []any (JSON/YAML-decoded default,
		// via OrderedMap - which never auto-converts, see orderedmap.ToStringSlice).
		items, err := orderedmap.ToStringSlice(value)
		if err != nil {
			return nil, errors.Errorf(`value "%v" is not a string array: %w`, value, err)
		}

		// Return only unique values, as []any (JSON type, used in Jsonnet template).
		slice := make([]any, 0, len(items))
		seen := make(map[string]bool, len(items))
		for _, item := range items {
			if !seen[item] {
				slice = append(slice, item)
				seen[item] = true
			}
		}
		return slice, nil
	case TypeObject:
		return value, nil
	}
	return value, nil
}

func reflectKindToStr(k reflect.Kind) string {
	if k == reflect.Invalid {
		return "null"
	}
	return k.String()
}
