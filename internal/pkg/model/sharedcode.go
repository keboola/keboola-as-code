package model

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SharedCodeConfig struct {
	Target keboola.ComponentID `validate:"required"`
}

type SharedCodeRow struct {
	Target  keboola.ComponentID `validate:"required"`
	Scripts Scripts
}

// LinkScript is reference to shared code used in transformation.
type LinkScript struct {
	Target ConfigRowKey
}

func (v LinkScript) Content() string {
	return fmt.Sprintf(`shared code "%s"`, v.Target.ID.String())
}

func (v SharedCodeConfig) String() string {
	return v.Target.String()
}

func (v SharedCodeRow) String() string {
	return v.Scripts.String(v.Target)
}

// SharedCodeVariablesForRelation - variables for shared code.
type SharedCodeVariablesForRelation struct {
	ConfigID keboola.ConfigID `json:"configId" validate:"required"`
	RowID    keboola.RowID    `json:"rowId" validate:"required"`
}

// SharedCodeVariablesFromRelation - variables from source configuration.
type SharedCodeVariablesFromRelation struct {
	VariablesID keboola.ConfigID `json:"variablesId" validate:"required"`
}

func (t *SharedCodeVariablesForRelation) Type() RelationType {
	return SharedCodeVariablesForRelType
}

func (t *SharedCodeVariablesForRelation) Desc() string {
	return `shared code variables for`
}

func (t *SharedCodeVariablesForRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.ConfigID)
}

func (t *SharedCodeVariablesForRelation) ParentKey(relationDefinedOn Key) (Key, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn)
	if err != nil {
		return nil, err
	}
	return ConfigRowKey{
		BranchID:    variables.BranchID,
		ComponentID: keboola.SharedCodeComponentID,
		ConfigID:    t.ConfigID,
		ID:          t.RowID,
	}, nil
}

func (t *SharedCodeVariablesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *SharedCodeVariablesForRelation) IsDefinedInAPI() bool {
	return false
}

func (t *SharedCodeVariablesForRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigRowKey{
		BranchID:    variables.BranchID,
		ComponentID: keboola.SharedCodeComponentID,
		ConfigID:    t.ConfigID,
		ID:          t.RowID,
	}
	otherSideRelation := &SharedCodeVariablesFromRelation{
		VariablesID: variables.ID,
	}
	return otherSide, otherSideRelation, nil
}

func (t *SharedCodeVariablesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	variables, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return variables, errors.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if variables.ComponentID != keboola.VariablesComponentID {
		return variables, errors.Errorf(`relation "%s" must be defined on config from "%s" component, found %s`, t.Type(), keboola.VariablesComponentID, relationDefinedOn.Desc())
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
	return fmt.Sprintf(`%s_%s`, t.Type(), t.VariablesID)
}

func (t *SharedCodeVariablesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *SharedCodeVariablesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *SharedCodeVariablesFromRelation) IsDefinedInAPI() bool {
	return true
}

func (t *SharedCodeVariablesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	row, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchID:    row.BranchID,
		ComponentID: keboola.VariablesComponentID,
		ID:          t.VariablesID,
	}
	otherSideRelation := &SharedCodeVariablesForRelation{
		ConfigID: row.ConfigID,
		RowID:    row.ID,
	}
	return otherSide, otherSideRelation, nil
}

func (t *SharedCodeVariablesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigRowKey, error) {
	row, ok := relationDefinedOn.(ConfigRowKey)
	if !ok {
		return row, errors.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if row.ComponentID != keboola.SharedCodeComponentID {
		return row, errors.Errorf(`relation "%s" must be defined on config row from "%s" component, found %s`, t.Type(), keboola.SharedCodeComponentID, relationDefinedOn.Desc())
	}
	return row, nil
}
