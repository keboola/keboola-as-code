package model

import (
	"fmt"
)

// SharedCodeVariablesForRelation - variables for shared code.
type SharedCodeVariablesForRelation struct {
	ConfigId ConfigId    `json:"configId" validate:"required"`
	RowId    ConfigRowId `json:"rowId" validate:"required"`
}

// SharedCodeVariablesFromRelation - variables from source configuration.
type SharedCodeVariablesFromRelation struct {
	VariablesId ConfigId `json:"variablesId" validate:"required"`
}

func (t *SharedCodeVariablesForRelation) Type() RelationType {
	return SharedCodeVariablesForRelType
}

func (t *SharedCodeVariablesForRelation) String() string {
	return `shared code variables for`
}

func (t *SharedCodeVariablesForRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.ConfigId)
}

func (t *SharedCodeVariablesForRelation) ParentKey(relationDefinedOn Key) (Key, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn)
	if err != nil {
		return nil, err
	}
	return ConfigRowKey{
		ConfigKey: ConfigKey{
			BranchKey:   BranchKey{BranchId: variables.BranchId},
			ComponentId: SharedCodeComponentId,
			ConfigId:    t.ConfigId,
		},
		ConfigRowId: t.RowId,
	}, nil
}

func (t *SharedCodeVariablesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *SharedCodeVariablesForRelation) IsDefinedInApi() bool {
	return false
}

func (t *SharedCodeVariablesForRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigRowKey{
		ConfigKey: ConfigKey{
			BranchKey:   BranchKey{BranchId: variables.BranchId},
			ComponentId: SharedCodeComponentId,
			ConfigId:    t.ConfigId,
		},
		ConfigRowId: t.RowId,
	}
	otherSideRelation := &SharedCodeVariablesFromRelation{
		VariablesId: variables.ConfigId,
	}
	return otherSide, otherSideRelation, nil
}

func (t *SharedCodeVariablesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	variables, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return variables, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.String())
	}
	if variables.ComponentId != VariablesComponentId {
		return variables, fmt.Errorf(`relation "%s" must be defined on config from "%s" component, found %s`, t.Type(), VariablesComponentId, relationDefinedOn.String())
	}
	return variables, nil
}

func (t *SharedCodeVariablesFromRelation) Type() RelationType {
	return SharedCodeVariablesFromRelType
}

func (t *SharedCodeVariablesFromRelation) String() string {
	return `shared code variables from`
}

func (t *SharedCodeVariablesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.VariablesId)
}

func (t *SharedCodeVariablesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *SharedCodeVariablesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *SharedCodeVariablesFromRelation) IsDefinedInApi() bool {
	return true
}

func (t *SharedCodeVariablesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	row, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchKey:   BranchKey{BranchId: row.BranchId},
		ComponentId: VariablesComponentId,
		ConfigId:    t.VariablesId,
	}
	otherSideRelation := &SharedCodeVariablesForRelation{
		ConfigId: row.ConfigId,
		RowId:    row.ConfigRowId,
	}
	return otherSide, otherSideRelation, nil
}

func (t *SharedCodeVariablesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigRowKey, error) {
	row, ok := relationDefinedOn.(ConfigRowKey)
	if !ok {
		return row, fmt.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.String())
	}
	if row.ComponentId != SharedCodeComponentId {
		return row, fmt.Errorf(`relation "%s" must be defined on config row from "%s" component, found %s`, t.Type(), SharedCodeComponentId, relationDefinedOn.String())
	}
	return row, nil
}
