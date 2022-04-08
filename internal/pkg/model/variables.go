package model

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
)

// VariablesForRelation - variables for target configuration.
type VariablesForRelation struct {
	ComponentId ComponentId `json:"componentId" validate:"required"`
	ConfigId    ConfigId    `json:"configId" validate:"required"`
}

// VariablesFromRelation - variables from source configuration.
type VariablesFromRelation struct {
	VariablesId ConfigId `json:"variablesId" validate:"required"`
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
	VariablesValuesId ConfigRowId `json:"variablesValuesId" validate:"required" `
}

func (t *VariablesForRelation) Type() RelationType {
	return VariablesForRelType
}

func (t *VariablesForRelation) String() string {
	return `variables for`
}

func (t *VariablesForRelation) Key() string {
	return fmt.Sprintf(`%s_%s_%s`, t.Type(), t.ComponentId, t.ConfigId)
}

func (t *VariablesForRelation) ParentKey(relationDefinedOn Key) (Key, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn)
	if err != nil {
		return nil, err
	}
	return ConfigKey{
		BranchKey:   BranchKey{BranchId: variables.BranchId},
		ComponentId: t.ComponentId,
		ConfigId:    t.ConfigId,
	}, nil
}

func (t *VariablesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *VariablesForRelation) IsDefinedInApi() bool {
	return false
}

func (t *VariablesForRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchKey:   BranchKey{BranchId: variables.BranchId},
		ComponentId: t.ComponentId,
		ConfigId:    t.ConfigId,
	}
	otherSideRelation := &VariablesFromRelation{
		VariablesId: variables.ConfigId,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	variables, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return variables, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.String())
	}
	if variables.ComponentId != VariablesComponentId {
		return variables, fmt.Errorf(`relation "%s" must be defined on config of the "%s" component`, t.Type(), VariablesComponentId)
	}
	return variables, nil
}

func (t *VariablesFromRelation) Type() RelationType {
	return VariablesFromRelType
}

func (t *VariablesFromRelation) String() string {
	return `variables from`
}

func (t *VariablesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.VariablesId)
}

func (t *VariablesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *VariablesFromRelation) IsDefinedInApi() bool {
	return true
}

func (t *VariablesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ Objects) (Key, Relation, error) {
	config, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchKey:   BranchKey{BranchId: config.BranchId},
		ComponentId: VariablesComponentId,
		ConfigId:    t.VariablesId,
	}
	otherSideRelation := &VariablesForRelation{
		ComponentId: config.ComponentId,
		ConfigId:    config.ConfigId,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.String())
	}
	return config, nil
}

func (t *VariablesValuesForRelation) Type() RelationType {
	return VariablesValuesForRelType
}

func (t *VariablesValuesForRelation) String() string {
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

func (t *VariablesValuesForRelation) IsDefinedInApi() bool {
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
		return nil, nil, fmt.Errorf(`%s not found, referenced from %s, by relation "%s"`, variablesConfigKey.String(), relationDefinedOn.String(), t.Type())
	}
	variablesConfig := variablesConfigRaw.(*Config)
	variablesForRaw, err := variablesConfig.Relations.GetOneByType(VariablesForRelType)
	if err != nil {
		return nil, nil, errors.PrefixError(fmt.Sprintf(`invalid %s`, variablesConfig.String()), err)
	}
	if variablesForRaw == nil {
		errs := errors.NewMultiError()
		errs.Append(fmt.Errorf(`missing relation "%s" in %s`, VariablesForRelType, variablesConfig.String()))
		errs.Append(fmt.Errorf(`  - referenced from %s`, relationDefinedOn.String()))
		errs.Append(fmt.Errorf(`  - by relation "%s"`, t.Type()))
		return nil, nil, errs
	}
	variablesForRelation := variablesForRaw.(*VariablesForRelation)
	otherSide := ConfigKey{
		BranchKey:   BranchKey{BranchId: variablesConfig.BranchId},
		ComponentId: variablesForRelation.ComponentId,
		ConfigId:    variablesForRelation.ConfigId,
	}
	otherSideRelation := &VariablesValuesFromRelation{
		VariablesValuesId: valuesRowKey.ConfigRowId,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesValuesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigRowKey, error) {
	values, ok := relationDefinedOn.(ConfigRowKey)
	if !ok {
		return values, fmt.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.String())
	}
	if values.ComponentId != VariablesComponentId {
		return values, fmt.Errorf(`relation "%s" must be defined on config of the "%s" component`, t.Type(), VariablesComponentId)
	}
	return values, nil
}

func (t *VariablesValuesFromRelation) Type() RelationType {
	return VariablesValuesFromRelType
}

func (t *VariablesValuesFromRelation) String() string {
	return `variables values from`
}

func (t *VariablesValuesFromRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.VariablesValuesId)
}

func (t *VariablesValuesFromRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *VariablesValuesFromRelation) IsDefinedInManifest() bool {
	return false
}

func (t *VariablesValuesFromRelation) IsDefinedInApi() bool {
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
		return nil, nil, errors.PrefixError(fmt.Sprintf(`invalid %s`, config.String()), err)
	} else if variablesFromRel == nil {
		errs := errors.NewMultiError()
		errs.Append(fmt.Errorf(`missing relation "%s" in %s`, VariablesFromRelType, config.String()))
		errs.Append(fmt.Errorf(`  - referenced from %s`, relationDefinedOn.String()))
		errs.Append(fmt.Errorf(`  - by relation "%s"`, t.Type()))
		return nil, nil, errs
	}

	otherSideKey := ConfigRowKey{
		ConfigKey: ConfigKey{
			BranchKey:   BranchKey{BranchId: config.BranchId},
			ComponentId: VariablesComponentId,
			ConfigId:    variablesFromRel.(*VariablesFromRelation).VariablesId,
		},
		ConfigRowId: t.VariablesValuesId,
	}
	otherSideRelation := &VariablesValuesForRelation{}
	return otherSideKey, otherSideRelation, nil
}

func (t *VariablesValuesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, fmt.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.String())
	}
	return config, nil
}
