package model

import (
	"fmt"
)

type SharedCodeConfig struct {
	Target ComponentId `validate:"required"`
}

type SharedCodeRow struct {
	Target  ComponentId `validate:"required"`
	Scripts Scripts     `validate:"required"`
}

// LinkScript is reference to shared code used in transformation.
type LinkScript struct {
	Target ConfigRowKey
}

func (v LinkScript) Content() string {
	return fmt.Sprintf(`shared code "%s"`, v.Target.Id.String())
}

func (v SharedCodeConfig) String() string {
	return v.Target.String()
}

func (v SharedCodeRow) String() string {
	return v.Scripts.String(v.Target)
}

// SharedCodeVariablesForRelation - variables for shared code.
type SharedCodeVariablesForRelation struct {
	ConfigId ConfigId `json:"configId" validate:"required"`
	RowId    RowId    `json:"rowId" validate:"required"`
}

// SharedCodeVariablesFromRelation - variables from source configuration.
type SharedCodeVariablesFromRelation struct {
	VariablesId ConfigId `json:"variablesId" validate:"required"`
}

func (t *SharedCodeVariablesForRelation) Type() RelationType {
	return SharedCodeVariablesForRelType
}

func (t *SharedCodeVariablesForRelation) Desc() string {
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
		BranchId:    variables.BranchId,
		ComponentId: SharedCodeComponentId,
		ConfigId:    t.ConfigId,
		Id:          t.RowId,
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
		BranchId:    variables.BranchId,
		ComponentId: SharedCodeComponentId,
		ConfigId:    t.ConfigId,
		Id:          t.RowId,
	}
	otherSideRelation := &SharedCodeVariablesFromRelation{
		VariablesId: variables.Id,
	}
	return otherSide, otherSideRelation, nil
}

func (t *SharedCodeVariablesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	variables, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return variables, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if variables.ComponentId != VariablesComponentId {
		return variables, fmt.Errorf(`relation "%s" must be defined on config from "%s" component, found %s`, t.Type(), VariablesComponentId, relationDefinedOn.Desc())
	}
	return variables, nil
}

func (t *SharedCodeVariablesFromRelation) Type() RelationType {
	return SharedCodeVariablesFromRelType
}

func (t *SharedCodeVariablesFromRelation) Desc() string {
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
		BranchId:    row.BranchId,
		ComponentId: VariablesComponentId,
		Id:          t.VariablesId,
	}
	otherSideRelation := &SharedCodeVariablesForRelation{
		ConfigId: row.ConfigId,
		RowId:    row.Id,
	}
	return otherSide, otherSideRelation, nil
}

func (t *SharedCodeVariablesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigRowKey, error) {
	row, ok := relationDefinedOn.(ConfigRowKey)
	if !ok {
		return row, fmt.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if row.ComponentId != SharedCodeComponentId {
		return row, fmt.Errorf(`relation "%s" must be defined on config row from "%s" component, found %s`, t.Type(), SharedCodeComponentId, relationDefinedOn.Desc())
	}
	return row, nil
}
