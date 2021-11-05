package model

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/go-cmp/cmp"
	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	VariablesForRelType        = RelationType(`variablesFor`)
	VariablesFromRelType       = RelationType(`variablesFrom`)
	VariablesValuesForRelType  = RelationType(`variablesValuesFor`)
	VariablesValuesFromRelType = RelationType(`variablesValuesFrom`)
)

// OneToXRelations gets relations that can be defined on an object only once.
func OneToXRelations() []RelationType {
	return []RelationType{
		VariablesForRelType,
		VariablesFromRelType,
		VariablesValuesForRelType,
		VariablesValuesFromRelType,
	}
}

type RelationType string

func (t RelationType) String() string {
	return string(t)
}

func (t RelationType) Type() RelationType {
	return t
}

// Relation between objects, eg. config <-> config.
type Relation interface {
	Type() RelationType
	Desc() string                             // human-readable description
	Key() string                              // unique key for sorting and comparing
	ParentKey(relationOwner Key) (Key, error) // if relation type is parent <-> child, then parent key is returned, otherwise nil
	OtherSideKey(owner Key) Key               // get key of the other side
	IsDefinedInManifest() bool                // if true, relation will be present in the manifest
	IsDefinedInApi() bool                     // if true, relation will be present in API calls
	NewOtherSideRelation(owner Key) Relation  // create the new other side relation, for example VariablesFor -> VariablesFrom
}

type Relations []Relation

func (v Relations) Equal(v2 Relations) bool {
	onlyIn1, onlyIn2 := v.Diff(v2)
	return onlyIn1 == nil && onlyIn2 == nil
}

func (v Relations) Diff(v2 Relations) (onlyIn1 Relations, onlyIn2 Relations) {
	v1Map := make(map[string]bool)
	v2Map := make(map[string]bool)
	for _, r := range v {
		v1Map[r.Key()] = true
	}
	for _, r := range v2 {
		v2Map[r.Key()] = true
	}
	for _, r := range v {
		if !v2Map[r.Key()] {
			onlyIn1 = append(onlyIn1, r)
		}
	}
	for _, r := range v2 {
		if !v1Map[r.Key()] {
			onlyIn2 = append(onlyIn2, r)
		}
	}
	onlyIn1.Sort()
	onlyIn2.Sort()
	return onlyIn1, onlyIn2
}

func (v Relations) Sort() {
	sort.SliceStable(v, func(i, j int) bool { return v[i].Key() < v[j].Key() })
}

func (v Relations) Has(t RelationType) bool {
	for _, relation := range v {
		if relation.Type() == t {
			return true
		}
	}
	return false
}

func (v Relations) GetByType(t RelationType) Relations {
	var out Relations
	for _, relation := range v {
		if relation.Type() == t {
			out = append(out, relation)
		}
	}
	return out
}

func (v Relations) GetAllByType() map[RelationType]Relations {
	out := make(map[RelationType]Relations)
	for _, relation := range v {
		out[relation.Type()] = append(out[relation.Type()], relation)
	}
	return out
}

func (v *Relations) Add(relation Relation) {
	for _, item := range *v {
		if cmp.Equal(item, relation) {
			// Relation is already present
			return
		}
	}
	*v = append(*v, relation)
}

func (v *Relations) RemoveByType(t RelationType) {
	var out Relations
	for _, relation := range *v {
		if relation.Type() != t {
			out = append(out, relation)
		}
	}
	*v = out
}

func (v *Relations) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	for _, item := range raw {
		var obj map[string]interface{}
		if err := json.Unmarshal(item, &obj); err != nil {
			return err
		}

		// Get type value
		typeRaw, ok := obj["type"]
		if !ok {
			return fmt.Errorf(`missing "type" field in relation definition`)
		}

		typeStr, ok := typeRaw.(string)
		if !ok {
			return fmt.Errorf(`field "type" must be string in relation definition, "%T" given`, typeStr)
		}

		// Create instance from type
		value, err := newEmptyRelation(RelationType(typeStr))
		if err != nil {
			return fmt.Errorf(`invalid "type" value "%s" in relation definition`, typeStr)
		}

		// Unmarshal to concrete sub-type of the Relation
		if err := json.Unmarshal(item, value); err != nil {
			return err
		}

		// Validate, only owning side should be present in JSON
		if !value.IsDefinedInManifest() {
			return fmt.Errorf(`unexpected state: relation "%T" should not be present in JSON, it is not an owning side`, value)
		}

		*v = append(*v, value)
	}
	return nil
}

func (v Relations) MarshalJSON() ([]byte, error) {
	var out []*orderedmap.OrderedMap
	for _, relation := range v {
		// Validate, only owning side should be serialized to JSON
		if !relation.IsDefinedInManifest() {
			return nil, fmt.Errorf(`unexpected state: relation "%T" should not be serialized to JSON, it is not an owning side`, relation)
		}

		// Convert struct -> map
		relationMap := utils.NewOrderedMap()
		if err := utils.ConvertByJson(relation, &relationMap); err != nil {
			return nil, err
		}
		relationMap.Set(`type`, relation.Type().String())
		out = append(out, relationMap)
	}
	return json.Marshal(out)
}

func newEmptyRelation(t RelationType) (Relation, error) {
	switch t {
	case VariablesForRelType:
		return &VariablesForRelation{}, nil
	case VariablesFromRelType:
		return &VariablesFromRelation{}, nil
	case VariablesValuesForRelType:
		return &VariablesValuesForRelation{}, nil
	case VariablesValuesFromRelType:
		return &VariablesValuesFromRelation{}, nil
	default:
		return nil, fmt.Errorf(`unexpected RelationType "%s"`, t)
	}
}

func (v Relations) ParentKey(source Key) (Key, error) {
	var parents []Key
	for _, r := range v {
		if parent, err := r.ParentKey(source); err != nil {
			return nil, err
		} else if parent != nil {
			parents = append(parents, parent)
		}
	}

	// Found parent defined via Relations
	if len(parents) == 1 {
		return parents[0], nil
	}

	// Multiple parents are forbidden
	if len(parents) > 1 {
		return nil, fmt.Errorf(`unexpected state: multiple parents defined by "relations" in %s`, source.Desc())
	}

	return nil, nil
}

func (v Relations) OnlyStoredInApi() Relations {
	var out Relations
	for _, relation := range v {
		if relation.IsDefinedInApi() {
			out = append(out, relation)
		}
	}
	return out
}

func (v Relations) OnlyStoredInManifest() Relations {
	var out Relations
	for _, relation := range v {
		if relation.IsDefinedInManifest() {
			out = append(out, relation)
		}
	}
	return out
}
