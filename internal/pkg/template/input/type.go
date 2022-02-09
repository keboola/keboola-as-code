package input

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

const (
	TypeString      = Type("string")
	TypeInt         = Type("int")
	TypeDouble      = Type("double")
	TypeBool        = Type("bool")
	TypeStringArray = Type("string[]")
)

// Type of the template user input.
// This corresponds to the data type that will be used in the JsonNet template.
type Type string

type Types []Type

func allTypes() Types {
	return Types{TypeString, TypeInt, TypeDouble, TypeBool, TypeStringArray}
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
	default:
		panic(fmt.Errorf(`unexpected input type "%s"`, t))
	}

	return nil
}

func reflectKindToStr(k reflect.Kind) string {
	if k == reflect.Invalid {
		return "null"
	}
	return k.String()
}
