package model

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// VariablesForRelation - variables for target configuration.
type VariablesForRelation struct {
	ComponentId string `json:"componentId" validate:"required"`
	ConfigId    string `json:"configId" validate:"required"`
}

// VariablesFromRelation - variables from source configuration.
type VariablesFromRelation struct {
	VariablesId string `json:"variablesId" validate:"required"`
}

// VariablesValuesForRelation - variables default values for target configuration.
type VariablesValuesForRelation struct{}

// VariablesValuesFromRelation - variables default values from source config row.
type VariablesValuesFromRelation struct {
	VariablesValuesId string `json:"variablesValuesId" validate:"required" `
}

func (t *VariablesForRelation) Type() RelationType {
	return VariablesForRelType
}

func (t *VariablesForRelation) Desc() string {
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
		BranchId:    variables.BranchId,
		ComponentId: t.ComponentId,
		Id:          t.ConfigId,
	}, nil
}

func (t *VariablesForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *VariablesForRelation) IsDefinedInApi() bool {
	return false
}

func (t *VariablesForRelation) NewOtherSideRelation(relationDefinedOn Object, _ *StateObjects) (Key, Relation, error) {
	variables, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchId:    variables.BranchId,
		ComponentId: t.ComponentId,
		Id:          t.ConfigId,
	}
	otherSideRelation := &VariablesFromRelation{
		VariablesId: variables.Id,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	variables, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return variables, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if variables.ComponentId != VariablesComponentId {
		return variables, fmt.Errorf(`relation "%s" must be defined on config of the "%s" component`, t.Type(), VariablesComponentId)
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

func (t *VariablesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ *StateObjects) (Key, Relation, error) {
	config, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	otherSide := ConfigKey{
		BranchId:    config.BranchId,
		ComponentId: VariablesComponentId,
		Id:          t.VariablesId,
	}
	otherSideRelation := &VariablesForRelation{
		ComponentId: config.ComponentId,
		ConfigId:    config.Id,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
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

func (t *VariablesValuesForRelation) IsDefinedInApi() bool {
	return false
}

func (t *VariablesValuesForRelation) NewOtherSideRelation(relationDefinedOn Object, allObjects *StateObjects) (Key, Relation, error) {
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
		return nil, nil, fmt.Errorf(`%s not found, referenced from %s, by relation "%s"`, variablesConfigKey.Desc(), relationDefinedOn.Desc(), t.Type())
	}
	variablesConfig := variablesConfigRaw.(*Config)
	variablesForRaw, err := variablesConfig.Relations.GetOneByType(VariablesForRelType)
	if err != nil {
		return nil, nil, utils.PrefixError(fmt.Sprintf(`invalid %s`, variablesConfig.Desc()), err)
	}
	if variablesForRaw == nil {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`missing relation "%s" in %s`, VariablesForRelType, variablesConfig.Desc()))
		errors.Append(fmt.Errorf(`  - referenced from %s`, relationDefinedOn.Desc()))
		errors.Append(fmt.Errorf(`  - by relation "%s"`, t.Type()))
		return nil, nil, errors
	}
	variablesForRelation := variablesForRaw.(*VariablesForRelation)
	otherSide := ConfigKey{
		BranchId:    variablesConfig.BranchId,
		ComponentId: variablesForRelation.ComponentId,
		Id:          variablesForRelation.ConfigId,
	}
	otherSideRelation := &VariablesValuesFromRelation{
		VariablesValuesId: valuesRowKey.Id,
	}
	return otherSide, otherSideRelation, nil
}

func (t *VariablesValuesForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigRowKey, error) {
	values, ok := relationDefinedOn.(ConfigRowKey)
	if !ok {
		return values, fmt.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	if values.ComponentId != VariablesComponentId {
		return values, fmt.Errorf(`relation "%s" must be defined on config of the "%s" component`, t.Type(), VariablesComponentId)
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

func (t *VariablesValuesFromRelation) NewOtherSideRelation(relationDefinedOn Object, _ *StateObjects) (Key, Relation, error) {
	_, err := t.checkDefinedOn(relationDefinedOn.Key())
	if err != nil {
		return nil, nil, err
	}
	config := relationDefinedOn.(*Config)
	variablesFromRel, err := config.Relations.GetOneByType(VariablesFromRelType)
	if err != nil {
		return nil, nil, utils.PrefixError(fmt.Sprintf(`invalid %s`, config.Desc()), err)
	} else if variablesFromRel == nil {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`missing relation "%s" in %s`, VariablesFromRelType, config.Desc()))
		errors.Append(fmt.Errorf(`  - referenced from %s`, relationDefinedOn.Desc()))
		errors.Append(fmt.Errorf(`  - by relation "%s"`, t.Type()))
		return nil, nil, errors
	}

	otherSideKey := ConfigRowKey{
		BranchId:    config.BranchId,
		ComponentId: VariablesComponentId,
		ConfigId:    variablesFromRel.(*VariablesFromRelation).VariablesId,
		Id:          t.VariablesValuesId,
	}
	otherSideRelation := &VariablesValuesForRelation{}
	return otherSideKey, otherSideRelation, nil
}

func (t *VariablesValuesFromRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, fmt.Errorf(`relation "%s" must be defined on config row, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	return config, nil
}
