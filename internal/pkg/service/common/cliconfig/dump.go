package cliconfig

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/cast"
)

type KVs []KV

type KV struct {
	Key   string
	Value string
}

func (v KVs) String() string {
	var out strings.Builder
	for i, kv := range v {
		if i > 0 {
			out.WriteString(" ")
		}
		out.WriteString(kv.Key)
		out.WriteString("=")
		out.WriteString(kv.Value)
		out.WriteString(";")
	}
	return out.String()
}

// Dump a configuration structure as key-value pairs.
func Dump(config any) (KVs, error) {
	// Dereference pointer
	v := reflect.ValueOf(config)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	// Dump fields
	out := make(KVs, 0)
	err := dump(v, "", &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func dump(v reflect.Value, parent string, out *KVs) error {
	if v.Kind() != reflect.Struct {
		return nil
	}

	// Iterate struct fields
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		// Get field tags
		fieldName, other, _ := strings.Cut(t.Field(i).Tag.Get("mapstructure"), ",")
		squash := strings.Contains(other, "squash")
		isSensitive, _, _ := strings.Cut(t.Field(i).Tag.Get("sensitive"), ",")
		if (fieldName == "" && !squash) || isSensitive == "true" {
			continue
		}

		// Prefix with parent name
		key := parent
		if parent != "" && fieldName != "" {
			key += "."
		}
		if fieldName != "" {
			key += fieldName
		}

		if err := dumpStructField(key, v.Field(i), out); err != nil {
			return err
		}
	}
	return nil
}

func dumpStructField(key string, v reflect.Value, out *KVs) error {
	t := v.Type()

	var str string
	switch {
	case t.Kind() == reflect.Invalid:
		// Untyped nil
		str = "<nil>"
	case (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil():
		// Nil pointer or interface
		str = "<nil>"
	default:
		// Some methods may be defined on a pointer type, convert value to pointer
		if t.Kind() != reflect.Pointer {
			ptr := reflect.New(t)
			ptr.Elem().Set(v)
			v = ptr
			t = v.Type()
		}

		// Try some common method for "to string" conversion
		switch value := v.Interface().(type) {
		case *string:
			str = *value
		case fmt.Stringer:
			str = value.String()
		case encoding.TextMarshaler:
			if v, err := value.MarshalText(); err != nil {
				return err
			} else {
				str = string(v)
			}
		case json.Marshaler:
			if v, err := value.MarshalJSON(); err != nil {
				return err
			} else {
				str = string(v)
			}
		default:
			if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct {
				// Dump nested struct
				return dump(v.Elem(), key, out)
			}

			// Fallback
			str = cast.ToString(v.Interface())
		}
	}

	if key != "" {
		*out = append(*out, KV{Key: key, Value: str})
	}

	return nil
}
