package encryption

import (
	"fmt"

	"github.com/iancoleman/orderedmap"
)

func updateContentElement(element interface{}, keyPath path, value interface{}) interface{} {

	switch currentElement := element.(type) {
	case *orderedmap.OrderedMap:
		childStep := keyPath[0].String()
		childElement, ok := currentElement.Get(childStep)
		if !ok {
			panic(fmt.Errorf("orderedMap \"%v\" missing key %v", element, childStep))
		}
		newChildElement := updateContentElement(childElement, keyPath[1:], value)
		currentElement.Set(childStep, newChildElement)
		return currentElement
	case orderedmap.OrderedMap:
		childStep := keyPath[0].String()
		childElement, ok := currentElement.Get(childStep)
		if !ok {
			panic(fmt.Errorf("orderedMap \"%v\" missing key %v", element, childStep))
		}
		newChildElement := updateContentElement(childElement, keyPath[1:], value)
		currentElement.Set(childStep, newChildElement)
		return currentElement
	case []interface{}:
		childStep := keyPath[0].(sliceStep)
		if int(childStep) >= len(currentElement) {
			panic(fmt.Errorf("slice \"%v\" index %v out of bouds", element, childStep))
		}
		childElement := currentElement[childStep]
		newChildElement := updateContentElement(childElement, keyPath[1:], value)
		currentElement[childStep] = newChildElement
		return currentElement
	// scalar value
	default:
		if len(keyPath) > 0 {
			panic(fmt.Errorf("scalar value update with non-empty path found %v ", keyPath))
		}
		return value
	}
}

func UpdateContent(content *orderedmap.OrderedMap, keyPath path, value interface{}) *orderedmap.OrderedMap {
	return updateContentElement(content, keyPath, value).(*orderedmap.OrderedMap)
}
