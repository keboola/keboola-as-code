package model

import (
	"fmt"
)

// Schedule - https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
type Schedule struct {
	Id       string   `json:"id" validate:"required"`
	ConfigId ConfigId `json:"configurationId" validate:"required"`
}

// SchedulerForRelation - scheduler for a configuration.
type SchedulerForRelation struct {
	ComponentId ComponentId `json:"componentId" validate:"required"`
	ConfigId    ConfigId    `json:"configId" validate:"required"`
}

func (t *SchedulerForRelation) Type() RelationType {
	return SchedulerForRelType
}

func (t *SchedulerForRelation) String() string {
	return `scheduler for`
}

func (t *SchedulerForRelation) Key() string {
	return fmt.Sprintf(`%s_%s_%s`, t.Type(), t.ComponentId, t.ConfigId)
}

func (t *SchedulerForRelation) ParentKey(relationDefinedOn Key) (Key, error) {
	config, err := t.checkDefinedOn(relationDefinedOn)
	if err != nil {
		return nil, err
	}
	return ConfigKey{
		BranchId:    config.BranchId,
		ComponentId: t.ComponentId,
		Id:          t.ConfigId,
	}, nil
}

func (t *SchedulerForRelation) OtherSideKey(_ Key) Key {
	return nil
}

func (t *SchedulerForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *SchedulerForRelation) IsDefinedInApi() bool {
	return true
}

func (t *SchedulerForRelation) NewOtherSideRelation(_ Object, _ Objects) (Key, Relation, error) {
	return nil, nil, nil
}

func (t *SchedulerForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, fmt.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.String())
	}
	return config, nil
}
