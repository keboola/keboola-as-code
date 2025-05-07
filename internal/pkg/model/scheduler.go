package model

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SchedulerForRelation - scheduler for a configuration.
type SchedulerForRelation struct {
	ComponentID keboola.ComponentID `json:"componentId" validate:"required"`
	ConfigID    keboola.ConfigID    `json:"configId" validate:"required"`
}

func (t *SchedulerForRelation) Type() RelationType {
	return SchedulerForRelType
}

func (t *SchedulerForRelation) Desc() string {
	return `scheduler for`
}

func (t *SchedulerForRelation) Key() string {
	return fmt.Sprintf(`%s_%s_%s`, t.Type(), t.ComponentID, t.ConfigID)
}

func (t *SchedulerForRelation) ParentKey(relationDefinedOn Key) (Key, error) {
	config, err := t.checkDefinedOn(relationDefinedOn)
	if err != nil {
		return nil, err
	}
	return ConfigKey{
		BranchID:    config.BranchID,
		ComponentID: t.ComponentID,
		ID:          t.ConfigID,
	}, nil
}

func (t *SchedulerForRelation) OtherSideKey(_ Key) Key {
	return nil
}

func (t *SchedulerForRelation) IsDefinedInManifest() bool {
	return true
}

func (t *SchedulerForRelation) IsDefinedInAPI() bool {
	return true
}

func (t *SchedulerForRelation) NewOtherSideRelation(_ Object, _ Objects) (Key, Relation, error) {
	return nil, nil, nil
}

func (t *SchedulerForRelation) checkDefinedOn(relationDefinedOn Key) (ConfigKey, error) {
	config, ok := relationDefinedOn.(ConfigKey)
	if !ok {
		return config, errors.Errorf(`relation "%s" must be defined on config, found %s`, t.Type(), relationDefinedOn.Desc())
	}
	return config, nil
}
