package yaml

import (
	"gopkg.in/yaml.v3"
)

func Encode(v interface{}) ([]byte, error) {
	var data []byte
	var err error
	data, err = yaml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func MustEncode(v interface{}) []byte {
	data, err := Encode(v)
	if err != nil {
		panic(err)
	}
	return data
}

func EncodeString(v interface{}) (string, error) {
	data, err := Encode(v)
	return string(data), err
}

func MustEncodeString(v interface{}) string {
	data, err := EncodeString(v)
	if err != nil {
		panic(err)
	}
	return data
}

func Decode(data []byte, m interface{}) error {
	if err := yaml.Unmarshal(data, m); err != nil {
		return err
	}
	return nil
}

func MustDecode(data []byte, m interface{}) {
	if err := Decode(data, m); err != nil {
		panic(err)
	}
}

func DecodeString(data string, m interface{}) error {
	return Decode([]byte(data), m)
}

func MustDecodeString(data string, m interface{}) {
	if err := DecodeString(data, m); err != nil {
		panic(err)
	}
}
