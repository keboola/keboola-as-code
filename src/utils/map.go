package utils

import "github.com/iancoleman/orderedmap"

type Pair struct {
	Key   string
	Value interface{}
}

func EmptyOrderedMap() *orderedmap.OrderedMap {
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
