package utils

import (
	"keboola-as-code/src/json"

	"github.com/iancoleman/orderedmap"
)

type Pair struct {
	Key   string
	Value interface{}
}

func ConvertByJson(input, target interface{}) {
	data, err := json.Encode(input, false)
	if err != nil {
		panic(err)
	}
	if err := json.Decode(data, target); err != nil {
		panic(err)
	}
}

func NewOrderedMap() *orderedmap.OrderedMap {
	ordered := orderedmap.New()
	ordered.SetEscapeHTML(false)
	return ordered
}

func PairsToOrderedMap(pairs []Pair) *orderedmap.OrderedMap {
	ordered := orderedmap.New()
	ordered.SetEscapeHTML(false)
	for _, pair := range pairs {
		ordered.Set(pair.Key, pair.Value)
	}
	return ordered
}

func OrderedMapToMap(in *orderedmap.OrderedMap) map[string]interface{} {
	if in == nil {
		return nil
	}

	out := make(map[string]interface{})
	keys := in.Keys()
	for _, key := range keys {
		value, _ := in.Get(key)
		out[key] = convertValue(value)
	}

	return out
}

func convertValue(value interface{}) interface{} {
	switch v := value.(type) {
	case orderedmap.OrderedMap:
		return OrderedMapToMap(&v)
	case *orderedmap.OrderedMap:
		return OrderedMapToMap(v)
	case []interface{}:
		for index, item := range v {
			v[index] = convertValue(item)
		}
		return v
	default:
		return value
	}
}
