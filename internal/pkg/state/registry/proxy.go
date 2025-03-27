package registry

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Proxy struct {
	registry  *Registry
	stateType model.StateType
}

func NewProxy(registry *Registry, stateType model.StateType) *Proxy {
	return &Proxy{registry: registry, stateType: stateType}
}

func (f *Proxy) All() []model.Object {
	var out []model.Object
	for _, object := range f.registry.All() {
		if object.HasState(f.stateType) {
			out = append(out, object.GetState(f.stateType))
		}
	}
	return out
}

func (f *Proxy) Branches() (branches []*model.Branch) {
	for _, branch := range f.registry.Branches() {
		if branch.HasState(f.stateType) {
			branches = append(branches, branch.GetState(f.stateType).(*model.Branch))
		}
	}
	return branches
}

func (f *Proxy) Get(key model.Key) (model.Object, bool) {
	objectState, found := f.registry.Get(key)
	if !found || !objectState.HasState(f.stateType) {
		return nil, false
	}
	return objectState.GetState(f.stateType), true
}

func (f *Proxy) MustGet(key model.Key) model.Object {
	objectState, found := f.registry.Get(key)
	if !found || !objectState.HasState(f.stateType) {
		panic(errors.Errorf(`%s not found`, key.Desc()))
	}
	return objectState.GetState(f.stateType)
}

func (f *Proxy) ConfigsFrom(branch model.BranchKey) (configs []*model.Config) {
	for _, config := range f.registry.ConfigsFrom(branch) {
		if config.HasState(f.stateType) {
			configs = append(configs, config.GetState(f.stateType).(*model.Config))
		}
	}
	return configs
}

func (f *Proxy) ConfigsWithRowsFrom(branch model.BranchKey) (configs []*model.ConfigWithRows) {
	configByIDMap := make(map[model.ConfigKey]*model.ConfigWithRows)
	for _, object := range f.registry.All() {
		if v, ok := object.(*model.ConfigState); ok {
			if v.BranchID != branch.ID || !v.HasState(f.stateType) {
				continue
			}
			config := v.GetState(f.stateType).(*model.Config)
			configWithRows := &model.ConfigWithRows{Config: config}
			configByIDMap[configWithRows.ConfigKey] = configWithRows
			configs = append(configs, configWithRows)
		} else if v, ok := object.(*model.ConfigRowState); ok {
			if v.BranchID != branch.ID || !v.HasState(f.stateType) {
				continue
			}
			if configForRow, found := configByIDMap[v.ConfigKey()]; found {
				row := v.GetState(f.stateType).(*model.ConfigRow)
				configForRow.Rows = append(configForRow.Rows, row)
			}
		}
	}
	return configs
}

func (f *Proxy) ConfigRowsFrom(config model.ConfigKey) (rows []*model.ConfigRow) {
	var out []*model.ConfigRow
	for _, row := range f.registry.ConfigRowsFrom(config) {
		if row.HasState(f.stateType) {
			out = append(out, row.GetState(f.stateType).(*model.ConfigRow))
		}
	}
	return out
}
