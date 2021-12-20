package registry

import (
	"fmt"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
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
		panic(fmt.Errorf(`%s not found`, key.Desc()))
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

func (f *Proxy) ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow) {
	var out []*ConfigRow
	for _, row := range f.registry.ConfigRowsFrom(config) {
		if row.HasState(f.stateType) {
			out = append(out, row.GetState(f.stateType).(*ConfigRow))
		}
	}
	return out
}
