// Package orderedmap is modified version of: https://github.com/iancoleman/orderedmap
package orderedmap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type Pair struct {
	Key   string
	Value interface{}
}

type ByPair struct {
	Pairs    []*Pair
	LessFunc func(a *Pair, j *Pair) bool
}

func (a ByPair) Len() int           { return len(a.Pairs) }
func (a ByPair) Swap(i, j int)      { a.Pairs[i], a.Pairs[j] = a.Pairs[j], a.Pairs[i] }
func (a ByPair) Less(i, j int) bool { return a.LessFunc(a.Pairs[i], a.Pairs[j]) }

type OrderedMap struct {
	keys   []string
	values map[string]interface{}
}

type visitCallback func(path Key, value interface{}, parent interface{})

func New() *OrderedMap {
	o := OrderedMap{}
	o.keys = []string{}
	o.values = map[string]interface{}{}
	return &o
}

func FromPairs(pairs []Pair) *OrderedMap {
	ordered := New()
	for _, pair := range pairs {
		ordered.Set(pair.Key, pair.Value)
	}
	return ordered
}

func (o *OrderedMap) Clone() *OrderedMap {
	return deepcopy.Copy(o).(*OrderedMap)
}

func (o *OrderedMap) DeepCopy(callback deepcopy.TranslateFunc, steps deepcopy.Steps, visited deepcopy.VisitedMap) *OrderedMap {
	if o == nil {
		return nil
	}

	out := New()
	for _, k := range o.Keys() {
		v, _ := o.Get(k)
		steps := steps.Add(``, k)
		out.Set(k, deepcopy.CopyTranslateSteps(v, callback, steps, visited))
	}
	return out
}

func (o *OrderedMap) ToMap() map[string]interface{} {
	if o == nil {
		return nil
	}

	out := make(map[string]interface{})
	for k, v := range o.values {
		out[k] = convertToMap(v)
	}

	return out
}

func (o *OrderedMap) Get(key string) (interface{}, bool) {
	val, exists := o.values[key]
	return val, exists
}

func (o *OrderedMap) GetOrNil(key string) interface{} {
	return o.values[key]
}

func (o *OrderedMap) Set(key string, value interface{}) {
	_, exists := o.values[key]
	if !exists {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

// SetNested value defined by key, eg. "parameters.foo[123]".
func (o *OrderedMap) SetNested(keysStr string, value interface{}) error {
	return o.SetNestedPath(KeyFromStr(keysStr), value)
}

// SetNestedPath value defined by key, eg. Key{MapStep("parameters), MapStep("foo"), SliceStep(123)}.
func (o *OrderedMap) SetNestedPath(keys Key, value interface{}) error {
	if len(keys) == 0 {
		panic(fmt.Errorf(`keys cannot be empty`))
	}

	currentKey := make(Key, 0)
	var current interface{} = o

	parentKeys := keys.WithoutLast()
	lastKey := keys.Last()

	// Get nested map
	for _, key := range parentKeys {
		currentKey = append(currentKey, key)
		switch key := key.(type) {
		case MapStep:
			if m, ok := current.(*OrderedMap); ok {
				if v, found := m.Get(string(key)); found {
					current = v
					continue
				} else {
					newMap := New()
					current = newMap
					m.Set(string(key), newMap)
				}
			} else {
				return fmt.Errorf(`key "%s": expected object found "%T"`, currentKey, current)
			}
		case SliceStep:
			if s, ok := current.([]interface{}); ok {
				if len(s) >= int(key) {
					current = s[key]
					continue
				} else {
					return fmt.Errorf(`key "%s" not found`, currentKey)
				}
			} else {
				return fmt.Errorf(`key "%s": expected array found "%T"`, currentKey.WithoutLast(), current)
			}
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, key))
		}
	}

	// Set value to map
	if key, ok := lastKey.(MapStep); ok {
		if m, ok := current.(*OrderedMap); ok {
			m.Set(string(key), value)
			return nil
		} else {
			return fmt.Errorf(`key "%s": expected object found "%T"`, currentKey, current)
		}
	}

	panic(fmt.Errorf(`last key must be MapStep, found "%T"`, lastKey))
}

// GetNestedOrNil returns nil if values is not found or an error occurred.
func (o *OrderedMap) GetNestedOrNil(keysStr string) interface{} {
	return o.GetNestedPathOrNil(KeyFromStr(keysStr))
}

func (o *OrderedMap) GetNestedMap(keysStr string) (m *OrderedMap, found bool, err error) {
	return o.GetNestedPathMap(KeyFromStr(keysStr))
}

// GetNestedPathOrNil returns nil if values is not found or an error occurred.
func (o *OrderedMap) GetNestedPathOrNil(keys Key) interface{} {
	value, found, err := o.GetNestedPath(keys)
	if !found {
		return nil
	} else if err != nil {
		panic(err)
	}
	return value
}

func (o *OrderedMap) GetNestedPathMap(keys Key) (m *OrderedMap, found bool, err error) {
	value, found, err := o.GetNestedPath(keys)
	if !found {
		return nil, false, nil
	} else if err != nil {
		return nil, true, err
	}
	if v, ok := value.(*OrderedMap); ok {
		return v, true, nil
	}
	return nil, true, fmt.Errorf(`key "%s": expected object, found "%T"`, keys, value)
}

func (o *OrderedMap) GetNested(keysStr string) (value interface{}, found bool, err error) {
	return o.GetNestedPath(KeyFromStr(keysStr))
}

func (o *OrderedMap) GetNestedPath(keys Key) (value interface{}, found bool, err error) {
	if len(keys) == 0 {
		panic(fmt.Errorf(`keys cannot be empty`))
	}

	currentKey := make(Key, 0)
	var current interface{} = o

	for _, key := range keys {
		currentKey = append(currentKey, key)
		switch key := key.(type) {
		case MapStep:
			if m, ok := current.(*OrderedMap); ok {
				if v, found := m.Get(string(key)); found {
					current = v
					continue
				} else {
					return nil, false, fmt.Errorf(`key "%s" not found`, currentKey)
				}
			} else {
				return nil, true, fmt.Errorf(`key "%s": expected object found "%T"`, currentKey.WithoutLast(), current)
			}
		case SliceStep:
			if s, ok := current.([]interface{}); ok {
				if len(s) >= int(key) {
					current = s[key]
					continue
				} else {
					return nil, false, fmt.Errorf(`key "%s" not found`, currentKey)
				}
			} else {
				return nil, true, fmt.Errorf(`key "%s": expected array found "%T"`, currentKey.WithoutLast(), current)
			}
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, key))
		}
	}
	return current, true, nil
}

// VisitAllRecursive calls callback for each nested key in OrderedMap or []interface{}.
func (o *OrderedMap) VisitAllRecursive(callback visitCallback) {
	visit(Key{}, o, nil, callback)
}

func (o *OrderedMap) Delete(key string) {
	// check key is in use
	_, ok := o.values[key]
	if !ok {
		return
	}
	// remove from keys
	for i, k := range o.keys {
		if k == key {
			o.keys = append(o.keys[:i], o.keys[i+1:]...)
			break
		}
	}
	// remove from values
	delete(o.values, key)
}

func (o *OrderedMap) Len() int {
	return len(o.keys)
}

func (o *OrderedMap) Keys() []string {
	return o.keys
}

// SortKeys Sort the map keys using your sort func.
func (o *OrderedMap) SortKeys(sortFunc func(keys []string)) {
	sortFunc(o.keys)
}

// Sort Sort the map using your sort func.
func (o *OrderedMap) Sort(lessFunc func(a *Pair, b *Pair) bool) {
	pairs := make([]*Pair, len(o.keys))
	for i, key := range o.keys {
		pairs[i] = &Pair{key, o.values[key]}
	}

	sort.Sort(ByPair{pairs, lessFunc})

	for i, pair := range pairs {
		o.keys[i] = pair.Key
	}
}

func (o *OrderedMap) UnmarshalJSON(b []byte) error {
	if o.values == nil {
		o.values = map[string]interface{}{}
	}
	err := json.Unmarshal(b, &o.values)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if _, err = dec.Token(); err != nil { // skip '{'
		return err
	}
	o.keys = make([]string, 0, len(o.values))
	return decodeOrderedMap(dec, o)
}

func decodeOrderedMap(dec *json.Decoder, o *OrderedMap) error {
	hasKey := make(map[string]bool, len(o.values))
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok && delim == '}' {
			return nil
		}
		key := token.(string)
		if hasKey[key] {
			// duplicate key
			for j, k := range o.keys {
				if k == key {
					copy(o.keys[j:], o.keys[j+1:])
					break
				}
			}
			o.keys[len(o.keys)-1] = key
		} else {
			hasKey[key] = true
			o.keys = append(o.keys, key)
		}

		token, err = dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				if values, ok := o.values[key].(map[string]interface{}); ok {
					newMap := &OrderedMap{
						keys:   make([]string, 0, len(values)),
						values: values,
					}
					if err = decodeOrderedMap(dec, newMap); err != nil {
						return err
					}
					o.values[key] = newMap
				} else if oldMap, ok := o.values[key].(*OrderedMap); ok {
					newMap := &OrderedMap{
						keys:   make([]string, 0, len(oldMap.values)),
						values: oldMap.values,
					}
					if err = decodeOrderedMap(dec, newMap); err != nil {
						return err
					}
					o.values[key] = newMap
				} else if err = decodeOrderedMap(dec, &OrderedMap{}); err != nil {
					return err
				}
			case '[':
				if values, ok := o.values[key].([]interface{}); ok {
					if err = decodeSlice(dec, values); err != nil {
						return err
					}
				} else if err = decodeSlice(dec, []interface{}{}); err != nil {
					return err
				}
			}
		}
	}
}

func decodeSlice(dec *json.Decoder, s []interface{}) error {
	for index := 0; ; index++ {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				if index < len(s) {
					if values, ok := s[index].(map[string]interface{}); ok {
						newMap := &OrderedMap{
							keys:   make([]string, 0, len(values)),
							values: values,
						}
						if err = decodeOrderedMap(dec, newMap); err != nil {
							return err
						}
						s[index] = newMap
					} else if oldMap, ok := s[index].(*OrderedMap); ok {
						newMap := &OrderedMap{
							keys:   make([]string, 0, len(oldMap.values)),
							values: oldMap.values,
						}
						if err = decodeOrderedMap(dec, newMap); err != nil {
							return err
						}
						s[index] = newMap
					} else if err = decodeOrderedMap(dec, &OrderedMap{}); err != nil {
						return err
					}
				} else if err = decodeOrderedMap(dec, &OrderedMap{}); err != nil {
					return err
				}
			case '[':
				if index < len(s) {
					if values, ok := s[index].([]interface{}); ok {
						if err = decodeSlice(dec, values); err != nil {
							return err
						}
					} else if err = decodeSlice(dec, []interface{}{}); err != nil {
						return err
					}
				} else if err = decodeSlice(dec, []interface{}{}); err != nil {
					return err
				}
			case ']':
				return nil
			}
		}
	}
}

func (o OrderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	encoder := json.NewEncoder(&buf)
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		// add key
		if err := encoder.Encode(k); err != nil {
			return nil, err
		}
		buf.WriteByte(':')
		// add value
		if err := encoder.Encode(o.values[k]); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func visit(key Key, valueRaw interface{}, parent interface{}, callback visitCallback) {
	// Call callback for not-root item
	if len(key) != 0 {
		callback(key, valueRaw, parent)
	}

	// Go deep
	switch parent := valueRaw.(type) {
	case *OrderedMap:
		for _, k := range parent.Keys() {
			subValue, _ := parent.Get(k)
			subKey := append(make(Key, 0), key...)
			subKey = append(subKey, MapStep(k))
			visit(subKey, subValue, parent, callback)
		}
	case []interface{}:
		for index, subValue := range parent {
			subKey := append(make(Key, 0), key...)
			subKey = append(subKey, SliceStep(index))
			visit(subKey, subValue, parent, callback)
		}
	}
}

func convertToMap(value interface{}) interface{} {
	switch v := value.(type) {
	case *OrderedMap:
		return v.ToMap()
	case []interface{}:
		mapped := make([]interface{}, 0)
		for _, item := range v {
			mapped = append(mapped, convertToMap(item))
		}
		return mapped
	default:
		return value
	}
}
