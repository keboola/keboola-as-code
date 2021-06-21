package utils

import "github.com/iancoleman/orderedmap"

type Pair struct {
	Key   string
	Value interface{}
}

func PairsToOrderedMap(pairs []Pair) *orderedmap.OrderedMap {
	ordered := orderedmap.New()
	for _, pair := range pairs {
		ordered.Set(pair.Key, pair.Value)
	}
	return ordered
}
