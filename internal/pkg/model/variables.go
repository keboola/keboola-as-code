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

// VariablesValuesForRelation - variables default values for target configuration.
type VariablesValuesForRelation struct {
	Target ConfigKeySameBranch `json:"target" validate:"required"`
}

// VariablesValuesFromRelation - variables default values from source config row.
type VariablesValuesFromRelation struct {
	Source ConfigRowKeySameBranch `json:"source" validate:"required"`
}

func (t *VariablesForRelation) Type() RelationType {
	return VariablesForRelType
}

func (t *VariablesForRelation) ParentKey(owner Key) (Key, error) {
	return t.OtherSideKey(owner), nil
}

func (t *VariablesForRelation) OtherSideKey(owner Key) Key {
	return t.Target.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesForRelation) IsOwningSide() bool {
	return true
}

// NewOtherSideRelation create the other side relation, for example VariablesFor -> VariablesFrom.
func (t *VariablesForRelation) NewOtherSideRelation(owner Key) Relation {
	return &VariablesFromRelation{
		Source: t.ownerKey(owner).ConfigKeySameBranch(),
	}
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

func (t *VariablesFromRelation) NewOtherSideRelation(owner Key) Relation {
	return &VariablesForRelation{
		Target: t.ownerKey(owner).ConfigKeySameBranch(),
	}
}

func (t *VariablesFromRelation) ownerKey(owner Key) ConfigKey {
	if configKey, ok := owner.(ConfigKey); ok {
		return configKey
	} else {
		panic(fmt.Errorf(`VariablesFromRelation must be defined on Config`))
	}
}

func (t *VariablesValuesForRelation) Type() RelationType {
	return VariablesValuesForRelType
}

func (t *VariablesValuesForRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesForRelation) OtherSideKey(owner Key) Key {
	return t.Target.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesValuesForRelation) IsOwningSide() bool {
	return true
}

func (t *VariablesValuesForRelation) NewOtherSideRelation(owner Key) Relation {
	return &VariablesValuesFromRelation{
		Source: t.ownerKey(owner).ConfigRowKeySameBranch(),
	}
}

func (t *VariablesValuesForRelation) ownerKey(owner Key) ConfigRowKey {
	if configRowKey, ok := owner.(ConfigRowKey); ok {
		return configRowKey
	} else {
		panic(fmt.Errorf(`VariablesValuesForRelation must be defined on ConfigRow`))
	}
}

func (t *VariablesValuesFromRelation) Type() RelationType {
	return VariablesValuesFromRelType
}

func (t *VariablesValuesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesFromRelation) OtherSideKey(owner Key) Key {
	return t.Source.ConfigRowKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesValuesFromRelation) IsOwningSide() bool {
	return false
}

func (t *VariablesValuesFromRelation) NewOtherSideRelation(owner Key) Relation {
	return &VariablesValuesForRelation{
		Target: t.ownerKey(owner).ConfigKeySameBranch(),
	}
}

func (t *VariablesValuesFromRelation) ownerKey(owner Key) ConfigKey {
	if configKey, ok := owner.(ConfigKey); ok {
		return configKey
	} else {
		panic(fmt.Errorf(`VariablesValuesForRelation must be defined on ConfigRow`))
	}
}
