package model

import (
	"encoding/json"
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	VariablesForRelType  = RelationType(`variablesFor`)
	VariablesFromRelType = RelationType(`variablesFrom`)
)

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
	ParentKey(relationOwner Key) (Key, error) // if relation type is parent <-> child, then parent key is returned, otherwise nil
	OtherSideKey(owner Key) Key               // get key of the other side
	IsOwningSide() bool                       // if true, relation will be stored in the manifest
	NewOtherSideRelation(owner Key) Relation  // create the new other side relation, for example VariablesFor -> VariablesFrom
}

type Relations []Relation

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
			return fmt.Errorf(`missing "type" field`)
		}

		typeStr, ok := typeRaw.(string)
		if !ok {
			return fmt.Errorf(`field "type" must be string, "%T" given`, typeStr)
		}

		// Create instance from type
		value, err := newEmptyRelation(RelationType(typeStr))
		if err != nil {
			return fmt.Errorf(`missing "type" field`)
		}

		// Unmarshal to concrete sub-type of the Relation
		if err := json.Unmarshal(item, value); err != nil {
			return err
		}

		// Validate, only owning side should be present to JSON
		if !value.IsOwningSide() {
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
		if !relation.IsOwningSide() {
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
		return nil, fmt.Errorf(`unexpected state: multiple parents defined by "relations" in "%s"`, source.Desc())
	}

	return nil, nil
}

func (v Relations) OnlyOwningSides() Relations {
	var out Relations
	for _, relation := range v {
		if relation.IsOwningSide() {
			out = append(out, relation)
		}
	}
	return out
}
