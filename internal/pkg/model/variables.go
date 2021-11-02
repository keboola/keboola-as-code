package model

import (
	"fmt"
)

// VariablesForRelation - variables for target configuration.
type VariablesForRelation struct {
	Target ConfigKeySameBranch `json:"target" validate:"required"`
}

// VariablesFromRelation - variables from source configuration.
type VariablesFromRelation struct {
	Source ConfigKeySameBranch `json:"source" validate:"required"`
}

func (t *VariablesForRelation) Type() RelationType {
	return VariablesForRelType
}

func (t *VariablesForRelation) ParentKey(owner Key) (Key, error) {
	return t.TargetKey(owner)
}

func (t *VariablesForRelation) OtherSideKey(owner Key) Key {
	return t.Target.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesForRelation) IsOwningSide() bool {
	return true
}

func (t *VariablesForRelation) IgnoreMissingOtherSide() bool {
	return true
}

// NewOtherSideRelation create the other side relation, for example VariablesFor -> VariablesFrom.
func (t *VariablesForRelation) NewOtherSideRelation(owner Key) Relation {
	return &VariablesFromRelation{
		Source: t.ownerKey(owner).ConfigKeySameBranch(),
	}
}

func (t *VariablesForRelation) TargetKey(owner Key) (Key, error) {
	return t.Target.ConfigKey(t.ownerKey(owner).BranchKey()), nil
}

func (t *VariablesForRelation) ownerKey(relationOwner Key) ConfigKey {
	if configKey, ok := relationOwner.(ConfigKey); ok {
		return configKey
	} else {
		panic(fmt.Errorf(`VariablesForRelation must be defined on Config`))
	}
}

func (t *VariablesFromRelation) Type() RelationType {
	return VariablesFromRelType
}

func (t *VariablesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesFromRelation) OtherSideKey(owner Key) Key {
	return t.Source.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesFromRelation) IsOwningSide() bool {
	return false
}

func (t *VariablesFromRelation) IgnoreMissingOtherSide() bool {
	return true
}

func (t *VariablesFromRelation) NewOtherSideRelation(owner Key) Relation {
	return &VariablesForRelation{
		Target: t.ownerKey(owner).ConfigKeySameBranch(),
	}
}

func (t *VariablesFromRelation) SourceKey(owner Key) (Key, error) {
	return t.Source.ConfigKey(owner.(ConfigKey).BranchKey()), nil
}

func (t *VariablesFromRelation) ownerKey(owner Key) ConfigKey {
	if configKey, ok := owner.(ConfigKey); ok {
		return configKey
	} else {
		panic(fmt.Errorf(`VariablesFromRelation must be defined on Config`))
	}
}
