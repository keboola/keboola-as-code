package json

import (
	"encoding/json"
	"fmt"
	"keboola-as-code/src/utils"
)

func Encode(v interface{}, pretty bool) ([]byte, error) {
	var data []byte
	var err error
	if pretty {
		data, err = json.MarshalIndent(v, "", "  ")
		data = append(data, '\n')
	} else {
		data, err = json.Marshal(v)
	}
	if err != nil {
		return nil, processJsonError(err)
	}
	return data, nil
}

func EncodeString(v interface{}, pretty bool) (string, error) {
	data, err := Encode(v, pretty)
	return string(data), err
}

func Decode(data []byte, m interface{}) error {
	err := json.Unmarshal(data, m)
	if err != nil {
		return processJsonError(err)
	}
	return nil
}

func DecodeString(data string, m interface{}) error {
	return Decode([]byte(data), m)
}

func processJsonError(err error) error {
	result := &utils.Error{}

	switch err := err.(type) {
	// Custom error message
	case *json.UnmarshalTypeError:
		result.Add(fmt.Errorf("key \"%s\" has invalid type \"%s\"", err.Field, err.Value))
	case *json.SyntaxError:
		result.Add(fmt.Errorf("%s, offset: %d", err, err.Offset))
	default:
		result.Add(err)
	}

	return result
}
