package model

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// VariablesForRelation - variables for target configuration.
type VariablesForRelation struct {
	ComponentID keboola.ComponentID `json:"componentId" validate:"required"`
	ConfigID    keboola.ConfigID    `json:"configId" validate:"required"`
}

// VariablesFromRelation - variables from source configuration.
type VariablesFromRelation struct {
	VariablesID keboola.ConfigID `json:"variablesId" validate:"required"`
}

// VariablesValuesForRelation - variables default values for target configuration.
type VariablesValuesForRelation struct {
	// Prevent empty struct,
	// because two distinct zero-size variables may have the same address in memory.
	// And it fails with deepcopy, because pointers are same.
	X bool `json:"-"`
}

// VariablesValuesFromRelation - variables default values from source config row.
type VariablesValuesFromRelation struct {
	VariablesValuesID keboola.RowID `json:"variablesValuesId" validate:"required" `
}

func (t *VariablesForRelation) Type() RelationType {
	return VariablesForRelType
}

func (t *VariablesForRelation) Desc() string {
	return `variables for`
}

func (t *VariablesForRelation) Key() string {
	return fmt.Sprintf(`%s_%s_%s`, t.Type(), t.ComponentID, t.ConfigID)
}

func (t *VariablesForRelation) ParentKey(relationDefinedOn Key) (Key, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn)
	if err != nil {
		return nil, err
	}
	return ConfigKey{
		BranchID:    variables.BranchID,
		ComponentID: t.ComponentID,
		ID:          t.ConfigID,
	}, nil
}

func (t *VariablesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *VariablesForRelation) IsDefinedInAPI() bool {
	return false
}

func (t *VariablesForRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchID:    variables.BranchID,
		ComponentID: t.ComponentID,
		ID:          t.ConfigID,
	}
	otherSideRelation := &VariablesFromRelation{
		VariablesID: variables.ID,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	variables, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return variables, errors.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if variables.ComponentID != keboola.VariablesComponentID {
		return variables, errors.Errorf(`relation "%s" must be defined on config of the "%s" component`, t.Type(), keboola.VariablesComponentID)
	}
	return variables, nil
}

func (t *VariablesFromRelation) Type() RelationType {
	return VariablesFromRelType
}

func (t *VariablesFromRelation) Desc() string {
	return `variables from`
}

func (t *VariablesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.VariablesID)
}

func (t *VariablesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *VariablesFromRelation) IsDefinedInAPI() bool {
	return true
}

func (t *VariablesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	config, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchID:    config.BranchID,
		ComponentID: keboola.VariablesComponentID,
		ID:          t.VariablesID,
	}
	otherSideRelation := &VariablesForRelation{
		ComponentID: config.ComponentID,
		ConfigID:    config.ID,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, errors.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	return config, nil
}

func (t *VariablesValuesForRelation) Type() RelationType {
	return VariablesValuesForRelType
}

func (t *VariablesValuesForRelation) Desc() string {
	return `variables values for`
}

func (t *VariablesValuesForRelation) Key() string {
	return t.Type().String()
}

func (t *VariablesValuesForRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *VariablesValuesForRelation) IsDefinedInAPI() bool {
	return false
}

func (t *VariablesValuesForRelation) NewOtherSideRelation(relationDefinedOn Object, allObjects Objects) (Key, Relation, error) {
	valuesRowKey, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	variablesConfigKey, err := valuesRowKey.ParentKey()
	if err != nil {
		return nil, nil, err
	}
	variablesConfigRaw, found := allObjects.Get(variablesConfigKey)
	if !found {
		return nil, nil, errors.Errorf(`%s not found, referenced from %s, by relation "%s"`, variablesConfigKey.Desc(), relationDefinedOn.Desc(), t.Type())
	}
	variablesConfig := variablesConfigRaw.(*Config)
	variablesForRaw, err := variablesConfig.Relations.GetOneByType(VariablesForRelType)
	if err != nil {
		return nil, nil, errors.PrefixErrorf(err, "invalid %s", variablesConfig.Desc())
	}
	if variablesForRaw == nil {
		return nil, nil, errors.NewNestedError(
			errors.Errorf(`missing relation "%s" in %s`, VariablesForRelType, variablesConfig.Desc()),
			errors.Errorf(`referenced from %s`, relationDefinedOn.Desc()),
			errors.Errorf(`by relation "%s"`, t.Type()),
		)
	}
	variablesForRelation := variablesForRaw.(*VariablesForRelation)
	otherSide := ConfigKey{
		BranchID:    variablesConfig.BranchID,
		ComponentID: variablesForRelation.ComponentID,
		ID:          variablesForRelation.ConfigID,
	}
	otherSideRelation := &VariablesValuesFromRelation{
		VariablesValuesID: valuesRowKey.ID,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesValuesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigRowKey, error) {
	values, ok := relationDefinedOn.(ConfigRowKey)
	if !ok {
		return values, errors.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if values.ComponentID != keboola.VariablesComponentID {
		return values, errors.Errorf(`relation "%s" must be defined on config of the "%s" component`, t.Type(), keboola.VariablesComponentID)
	}
	return values, nil
}

func (t *VariablesValuesFromRelation) Type() RelationType {
	return VariablesValuesFromRelType
}

func (t *VariablesValuesFromRelation) Desc() string {
	return `variables values from`
}

func (t *VariablesValuesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.VariablesValuesID)
}

func (t *VariablesValuesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *VariablesValuesFromRelation) IsDefinedInAPI() bool {
	return true
}

func (t *VariablesValuesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	_, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	config := relationDefinedOn.(*Config)
	variablesFromRel, err := config.Relations.GetOneByType(VariablesFromRelType)
	if err != nil {
		return nil, nil, errors.PrefixErrorf(err, "invalid %s", config.Desc())
	} else if variablesFromRel == nil {
		return nil, nil, errors.NewNestedError(
			errors.Errorf(`missing relation "%s" in %s`, VariablesFromRelType, config.Desc()),
			errors.Errorf(`referenced from %s`, relationDefinedOn.Desc()),
			errors.Errorf(`by relation "%s"`, t.Type()),
		)
	}

	otherSideKey := ConfigRowKey{
		BranchID:    config.BranchID,
		ComponentID: keboola.VariablesComponentID,
		ConfigID:    variablesFromRel.(*VariablesFromRelation).VariablesID,
		ID:          t.VariablesValuesID,
	}
	otherSideRelation := &VariablesValuesForRelation{}
	return otherSideKey, otherSideRelation, nil
}

func (t *VariablesValuesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, errors.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	return config, nil
}
