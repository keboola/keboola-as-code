package model

import (
	"encoding/json"
	"fmt"
)

const (
	VariablesIdRelType = RelationType(`variables_id`)
)

// VariablesIdRelation - configuration has defined variables (in variables configuration).
type VariablesIdRelation struct {
	RelationType `json:"type" validate:"required" diff:"true" metaFile:"true"`
	ConfigKey    ConfigKey `json:"config" validate:"required,dive"`
	VariablesId  string    `json:"variablesId" validate:"required"` // id of the variables configuration
}

type RelationType string

func (t RelationType) String() string {
	return string(t)
}

func (t RelationType) Type() RelationType {
	return t
}

func newEmptyRelation(t RelationType) (Relation, error) {
	switch t {
	case VariablesIdRelType:
		return &VariablesIdRelation{RelationType: VariablesIdRelType}, nil
	default:
		return nil, fmt.Errorf(`unexpected RelationType "%s"`, t)
	}
}

// Relation between objects, eg. config <-> config.
type Relation interface {
	Type() RelationType
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
