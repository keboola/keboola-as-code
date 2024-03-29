package registry

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Proxy struct {
	registry  *Registry
	stateType StateType
}

func NewProxy(registry *Registry, stateType StateType) *Proxy {
	return &Proxy{registry: registry, stateType: stateType}
}

func (f *Proxy) All() []Object {
	var out []Object
	for _, object := range f.registry.All() {
		if object.HasState(f.stateType) {
			out = append(out, object.GetState(f.stateType))
		}
	}
	return out
}

func (f *Proxy) Branches() (branches []*Branch) {
	for _, branch := range f.registry.Branches() {
		if branch.HasState(f.stateType) {
			branches = append(branches, branch.GetState(f.stateType).(*Branch))
		}
	}
	return branches
}

func (f *Proxy) Get(key Key) (Object, bool) {
	objectState, found := f.registry.Get(key)
	if !found || !objectState.HasState(f.stateType) {
		return nil, false
	}
	return objectState.GetState(f.stateType), true
}

func (f *Proxy) MustGet(key Key) Object {
	objectState, found := f.registry.Get(key)
	if !found || !objectState.HasState(f.stateType) {
		panic(errors.Errorf(`%s not found`, key.Desc()))
	}
	return objectState.GetState(f.stateType)
}

func (f *Proxy) ConfigsFrom(branch BranchKey) (configs []*Config) {
	for _, config := range f.registry.ConfigsFrom(branch) {
		if config.HasState(f.stateType) {
			configs = append(configs, config.GetState(f.stateType).(*Config))
		}
	}
	return configs
}

func (f *Proxy) ConfigsWithRowsFrom(branch BranchKey) (configs []*ConfigWithRows) {
	configByIDMap := make(map[ConfigKey]*ConfigWithRows)
	for _, object := range f.registry.All() {
		if v, ok := object.(*ConfigState); ok {
			if v.BranchID != branch.ID || !v.HasState(f.stateType) {
				continue
			}
			config := v.GetState(f.stateType).(*Config)
			configWithRows := &ConfigWithRows{Config: config}
			configByIDMap[configWithRows.ConfigKey] = configWithRows
			configs = append(configs, configWithRows)
		} else if v, ok := object.(*ConfigRowState); ok {
			if v.BranchID != branch.ID || !v.HasState(f.stateType) {
				continue
			}
			if configForRow, found := configByIDMap[v.ConfigKey()]; found {
				row := v.GetState(f.stateType).(*ConfigRow)
				configForRow.Rows = append(configForRow.Rows, row)
			}
		}
	}
	return configs
}

func (f *Proxy) ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow) {
	var out []*ConfigRow
	for _, row := range f.registry.ConfigRowsFrom(config) {
		if row.HasState(f.stateType) {
			out = append(out, row.GetState(f.stateType).(*ConfigRow))
		}
	}
	return out
}
