package utils

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"keboola-as-code/src/json"
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

// updates *orderedMap nested value stored on path and returns the updated map.
func UpdateIn(content *orderedmap.OrderedMap, keyPath KeyPath, value interface{}) *orderedmap.OrderedMap {
	return updateInElement(content, keyPath, value).(*orderedmap.OrderedMap)
}

// recursively updates nested structure element with value stored on the specified path and returns new or updated element.
func updateInElement(element interface{}, path KeyPath, value interface{}) interface{} {
	switch currentElement := element.(type) {
	case *orderedmap.OrderedMap:

		childStep := path[0].String()
		childElement, ok := currentElement.Get(childStep)
		if !ok {
			panic(fmt.Errorf("orderedMap \"%v\" missing key %v", element, childStep))
		}
		// currentElement is map so we update it recursively on key defined as path[0]
		newChildElement := updateInElement(childElement, path[1:], value)
		currentElement.Set(childStep, newChildElement)
		return currentElement
	case orderedmap.OrderedMap:
		childStep := path[0].String()
		childElement, ok := currentElement.Get(childStep)
		if !ok {
			panic(fmt.Errorf("orderedMap \"%v\" missing key %v", element, childStep))
		}
		// currentElement is map so we update it recursively on key defined as path[0]
		newChildElement := updateInElement(childElement, path[1:], value)
		currentElement.Set(childStep, newChildElement)
		return currentElement
	case []interface{}:
		childStep := path[0].(SliceStep)
		if int(childStep) >= len(currentElement) {
			panic(fmt.Errorf("slice \"%v\" index %v out of bouds", element, childStep))
		}
		childElement := currentElement[childStep]
		// currentElement is array so we update it recursively on index defined as as path[0]
		newChildElement := updateInElement(childElement, path[1:], value)
		currentElement[childStep] = newChildElement
		return currentElement
	// other value, expecting scalar without key to update recursively so we just return the value
	default:
		if len(path) > 0 {
			panic(fmt.Errorf(`unexpected value "%v" (%T) at path "%s"`, element, element, path))
		}
		return value
	}
}
