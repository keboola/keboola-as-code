package configmap

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"time"
)

type NoTextTypeError struct{}

func (v NoTextTypeError) Error() string {
	return "value is not convertible from/to a text"
}

// MarshalText provides conversion of a value to a text via common marshal interfaces.
func MarshalText(typ reflect.Type, value reflect.Value) (text []byte, err error) {
	// Ensure, that the value is a pointer, marshal method may be defined on the pointer
	original := value
	if typ.Kind() != reflect.Pointer {
		typ = reflect.PointerTo(typ)
		value = reflect.New(typ.Elem())
		if original.IsValid() {
			value.Elem().Set(original)
		}
	}

	// Initialize invalid value
	if !value.IsValid() {
		value = reflect.Zero(typ)
	}

	// Find a marshaller
	var fn func() ([]byte, error)
	switch v := value.Interface().(type) {
	case *time.Duration:
		fn = marshalDuration(v)
	case *[]byte:
		fn = marshalByteSlice(v)
	case json.Marshaler:
		fn = v.MarshalJSON
	case encoding.TextMarshaler:
		fn = v.MarshalText
	case encoding.BinaryMarshaler:
		fn = v.MarshalBinary
	default:
		// No marshaller found
		return nil, NoTextTypeError{}
	}

	// Handle zero value as an empty text.
	// Invalid means that a parent value is a nil pointer.
	if !original.IsValid() || (original.Kind() == reflect.Pointer && original.IsZero()) {
		return nil, nil
	}

	// Use marshaller
	return fn()
}

// UnmarshalText provides conversion of a text to a value via common unmarshal interfaces.
func UnmarshalText(text []byte, target reflect.Value) (err error) {
	// Ensure, that the target is a pointer, unmarshal method may be defined on the pointer
	var toPtr reflect.Value
	if target.Kind() == reflect.Pointer {
		toPtr = target
		toPtr.Set(reflect.New(toPtr.Type().Elem()))
	} else {
		toPtr = target.Addr()
	}

	// Find an unmarshaler
	var fn func([]byte) error
	switch v := toPtr.Interface().(type) {
	case *time.Duration:
		fn = unmarshalDuration(v)
	case *[]byte:
		fn = unmarshalByteSlice(v)
	case encoding.TextUnmarshaler:
		fn = v.UnmarshalText
	case encoding.BinaryUnmarshaler:
		fn = v.UnmarshalBinary
	case json.Unmarshaler:
		fn = v.UnmarshalJSON
	default:
		// No unmarshaler found
		return NoTextTypeError{}
	}

	// Handle empty text as a zero value
	if len(text) == 0 {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}

	// Use unmarshaller
	return fn(text)
}

func marshalDuration(from *time.Duration) func() ([]byte, error) {
	return func() ([]byte, error) {
		return []byte(from.String()), nil
	}
}

func unmarshalDuration(target *time.Duration) func([]byte) error {
	return func(text []byte) (err error) {
		*target, err = time.ParseDuration(string(text))
		return err
	}
}

func marshalByteSlice(from *[]byte) func() ([]byte, error) {
	return func() ([]byte, error) {
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(*from)))
		base64.StdEncoding.Encode(dst, *from)
		return dst, nil
	}
}

func unmarshalByteSlice(target *[]byte) func([]byte) error {
	return func(text []byte) (err error) {
		dst := make([]byte, base64.StdEncoding.DecodedLen(len(text)))
		n, err := base64.StdEncoding.Decode(dst, text)
		if err != nil {
			return err
		}
		*target = dst[:n]
		return nil
	}
}
