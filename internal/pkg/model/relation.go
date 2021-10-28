package model

import (
	"encoding/json"
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	VariablesForRelType = RelationType(`variablesFor`)
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
	TargetKey(source Key) (Key, error) // source is where the relation is defined, target is other side
	ParentKey(source Key) (Key, error) // if relation type is parent <-> child, then parent key is returned, otherwise nil
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
		*v = append(*v, value)
	}
	return nil
}

func (v Relations) MarshalJSON() ([]byte, error) {
	var out []*orderedmap.OrderedMap
	for _, relation := range v {
		// Get type string
		t, err := relationToType(relation)
		if err != nil {
			return nil, err
		}

		// Convert struct -> map
		relationMap := utils.NewOrderedMap()
		if err := utils.ConvertByJson(relation, &relationMap); err != nil {
			return nil, err
		}
		relationMap.Set(`type`, t)
		out = append(out, relationMap)
	}
	return json.Marshal(out)
}

func newEmptyRelation(t RelationType) (Relation, error) {
	switch t {
	case VariablesForRelType:
		return &VariablesForRelation{}, nil
	default:
		return nil, fmt.Errorf(`unexpected RelationType "%s"`, t)
	}
}

func relationToType(relation Relation) (RelationType, error) {
	switch relation.(type) {
	case *VariablesForRelation:
		return VariablesForRelType, nil
	default:
		return "", fmt.Errorf(`unexpected Relation "%T"`, relation)
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

// VariablesForRelation - variables for target configuration.
type VariablesForRelation struct {
	Target ConfigKeySameBranch `json:"target" validate:"required"`
}

func (t *VariablesForRelation) TargetKey(source Key) (Key, error) {
	return t.Target.ConfigKey(source.(ConfigKey).BranchKey()), nil
}

func (t *VariablesForRelation) ParentKey(source Key) (Key, error) {
	return t.TargetKey(source)
}
