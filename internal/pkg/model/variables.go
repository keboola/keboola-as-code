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

func (t *VariablesForRelation) Desc() string {
	return `variables for`
}

func (t *VariablesForRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.Target.String())
}

func (t *VariablesForRelation) ParentKey(owner Key) (Key, error) {
	return t.OtherSideKey(owner), nil
}

func (t *VariablesForRelation) OtherSideKey(owner Key) Key {
	return t.Target.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *VariablesForRelation) IsDefinedInApi() bool {
	return false
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

func (t *VariablesFromRelation) Desc() string {
	return `variables from`
}

func (t *VariablesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.Source.String())
}

func (t *VariablesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesFromRelation) OtherSideKey(owner Key) Key {
	return t.Source.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *VariablesFromRelation) IsDefinedInApi() bool {
	return true
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

func (t *VariablesValuesForRelation) Desc() string {
	return `variables values for`
}

func (t *VariablesValuesForRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.Target.String())
}

func (t *VariablesValuesForRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesForRelation) OtherSideKey(owner Key) Key {
	return t.Target.ConfigKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesValuesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *VariablesValuesForRelation) IsDefinedInApi() bool {
	return false
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

func (t *VariablesValuesFromRelation) Desc() string {
	return `variables values from`
}

func (t *VariablesValuesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.Source.String())
}

func (t *VariablesValuesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesFromRelation) OtherSideKey(owner Key) Key {
	return t.Source.ConfigRowKey(t.ownerKey(owner).BranchKey())
}

func (t *VariablesValuesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *VariablesValuesFromRelation) IsDefinedInApi() bool {
	return true
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
