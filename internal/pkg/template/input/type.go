package input

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cast"
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
// This corresponds to the data type that will be used in the JsonNet template.
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

func (t Type) IsValid() bool {
	for _, v := range allTypes() {
		if v == t {
			return true
		}
	}
	return false
}

// EmptyValue returns empty value for the type.
func (t Type) EmptyValue() interface{} {
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
		return []interface{}{}
	case TypeObject:
		return make(map[string]interface{})
	default:
		panic(fmt.Errorf(`unexpected input type "%s"`, t))
	}
}

// ValidateValue user input or defined default value.
func (t Type) ValidateValue(value reflect.Value) error {
	valueKind := value.Kind()
	kindStr := reflectKindToStr(valueKind)

	switch t {
	case TypeString:
		if valueKind != reflect.String {
			return fmt.Errorf("should be string, got %s", kindStr)
		}
	case TypeInt:
		if valueKind != reflect.Int && !(valueKind == reflect.Float64 && math.Trunc(value.Float()) == value.Float()) {
			return fmt.Errorf("should be int, got %s", kindStr)
		}
	case TypeDouble:
		if valueKind != reflect.Float64 {
			return fmt.Errorf("should be double, got %s", kindStr)
		}
	case TypeBool:
		if valueKind != reflect.Bool {
			return fmt.Errorf("should be bool, got %s", kindStr)
		}
	case TypeStringArray:
		if valueKind != reflect.Slice {
			// Must be a slice
			return fmt.Errorf("should be array, got %s", kindStr)
		} else {
			// Each element must be string
			for i := 0; i < value.Len(); i++ {
				item := value.Index(i)
				// Unwrap interface
				if item.Type().Kind() == reflect.Interface {
					item = item.Elem()
				}
				// Check item type
				if itemKind := item.Kind(); itemKind != reflect.String {
					return fmt.Errorf("all items should be string, got %s, index %d", reflectKindToStr(itemKind), i)
				}
			}
		}
	case TypeObject:
		if valueKind != reflect.Map {
			return fmt.Errorf("should be object, got %s", kindStr)
		}
	default:
		panic(fmt.Errorf(`unexpected input type "%s"`, t))
	}

	return nil
}

func (t Type) ParseValue(value interface{}) (interface{}, error) {
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
		return nil, fmt.Errorf(`value "%v" is not integer`, value)
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
		return nil, fmt.Errorf(`value "%v" is not float`, value)
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
		return nil, fmt.Errorf(`value "%v" is not bool`, value)
	case TypeString:
		return cast.ToString(value), nil
	case TypeStringArray:
		slice := make([]interface{}, 0)
		values := make(map[string]bool)

		if v, ok := value.(string); ok {
			// Split items by comma, if needed
			var items []string
			if v != "" {
				for _, item := range strings.Split(v, ",") {
					items = append(items, strings.TrimSpace(item))
				}
			}
			value = items
		}

		if items, ok := value.([]string); ok {
			// Convert []string (Go type) -> []interface{} (JSON type, used in JsonNet template)
			// And return only unique values.
			for _, item := range items {
				if !values[item] {
					slice = append(slice, item)
					values[item] = true
				}
			}
			return slice, nil
		} else if items, ok := value.([]interface{}); ok {
			// Return only unique values.
			for _, itemRaw := range items {
				item := itemRaw.(string)
				if !values[item] {
					slice = append(slice, item)
					values[item] = true
				}
			}
			return slice, nil
		} else {
			return nil, fmt.Errorf("unexpected type \"%T\"", value)
		}
	}
	return value, nil
}

func reflectKindToStr(k reflect.Kind) string {
	if k == reflect.Invalid {
		return "null"
	}
	return k.String()
}
