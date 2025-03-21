package json

import (
	"bytes"
	"encoding/json"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RawMessage = json.RawMessage

func Encode(v any, pretty bool) ([]byte, error) {
	var data []byte
	var err error
	if pretty {
		data, err = json.MarshalIndent(v, "", "  ")
		data = append(data, '\n')
	} else {
		data, err = json.Marshal(v)
	}
	if err != nil {
		return nil, processJSONEncodeError(err)
	}
	return data, nil
}

func MustEncode(v any, pretty bool) []byte {
	data, err := Encode(v, pretty)
	if err != nil {
		panic(err)
	}
	return data
}

func EncodeString(v any, pretty bool) (string, error) {
	data, err := Encode(v, pretty)
	return string(data), err
}

func MustEncodeString(v any, pretty bool) string {
	data, err := EncodeString(v, pretty)
	if err != nil {
		panic(err)
	}
	return data
}

func Decode(data []byte, m any) error {
	if err := json.Unmarshal(data, m); err != nil {
		return processJSONDecodeError(data, err)
	}
	return nil
}

func MustDecode(data []byte, m any) {
	if err := Decode(data, m); err != nil {
		panic(err)
	}
}

func DecodeString(data string, m any) error {
	return Decode([]byte(data), m)
}

func MustDecodeString(data string, m any) {
	if err := DecodeString(data, m); err != nil {
		panic(err)
	}
}

// DecodePreserveNumber decodes JSON data while preserving number types instead of converting all numbers to float64.
func DecodePreserveNumber(data []byte, m any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(m); err != nil {
		return processJSONDecodeError(data, err)
	}
	return nil
}

func ConvertByJSON(input, target any) error {
	data, err := Encode(input, false)
	if err != nil {
		return errors.Errorf(`encode error: %w`, err)
	}
	if err := Decode(data, target); err != nil {
		return errors.Errorf(`decode error: %w`, err)
	}
	return nil
}

func processJSONEncodeError(err error) error {
	var typeError *json.UnmarshalTypeError
	var syntaxError *json.SyntaxError

	switch {
	// Custom error message
	case errors.As(err, &typeError):
		return errors.Errorf("key \"%s\" has invalid type \"%s\"", typeError.Field, typeError.Value)
	case errors.As(err, &syntaxError):
		return errors.Errorf("%w, offset: %d", err, syntaxError.Offset)
	default:
		return err
	}
}

func processJSONDecodeError(data []byte, err error) error {
	var typeError *json.UnmarshalTypeError
	var syntaxError *json.SyntaxError

	switch {
	// Custom error message
	case errors.As(err, &typeError):
		return errors.Errorf("key \"%s\" has invalid type \"%s\"", typeError.Field, typeError.Value)
	case errors.As(err, &syntaxError):
		if syntaxError.Error() == "unexpected end of JSON input" && len(bytes.TrimSpace(data)) == 0 {
			return errors.New(`empty, please use "{}" for an empty JSON`)
		}
		return errors.Errorf("%w, offset: %d", err, syntaxError.Offset)
	default:
		return err
	}
}
