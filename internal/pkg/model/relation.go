package model

import (
	"encoding/json"
	"fmt"
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

func newEmptyRelation(t RelationType) (Relation, error) {
	switch t {
	case VariablesForRelType:
		return &VariablesForRelation{RelationType: VariablesForRelType}, nil
	default:
		return nil, fmt.Errorf(`unexpected RelationType "%s"`, t)
	}
}

// Relation between objects, eg. config <-> config.
type Relation interface {
	String() string
	Type() RelationType
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

// VariablesForRelation - variables for target configuration.
type VariablesForRelation struct {
	RelationType `json:"type" validate:"required"`
	Target       ConfigKeySameBranch `json:"target" validate:"required"`
}

func (t *VariablesForRelation) TargetKey(source Key) (Key, error) {
	return t.Target.ConfigKey(*source.(ConfigKey).BranchKey()), nil
}

func (t *VariablesForRelation) ParentKey(source Key) (Key, error) {
	return t.TargetKey(source)
}
