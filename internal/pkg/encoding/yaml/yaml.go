package yaml

import (
	"gopkg.in/yaml.v3"
)

func Encode(v any) ([]byte, error) {
	var data []byte
	var err error
	data, err = yaml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func MustEncode(v any) []byte {
	data, err := Encode(v)
	if err != nil {
		panic(err)
	}
	return data
}

func EncodeString(v any) (string, error) {
	data, err := Encode(v)
	return string(data), err
}

func MustEncodeString(v any) string {
	data, err := EncodeString(v)
	if err != nil {
		panic(err)
	}
	return data
}

func Decode(data []byte, m any) error {
	if err := yaml.Unmarshal(data, m); err != nil {
		return err
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
