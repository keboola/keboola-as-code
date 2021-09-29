package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

const DeprecatedFlag = `deprecated`

// Component https://keboola.docs.apiary.io/#reference/components-and-configurations/get-development-branch-components/get-development-branch-components
type Component struct {
	ComponentKey
	Type      string          `json:"type" validate:"required"`
	Name      string          `json:"name" validate:"required"`
	Flags     []string        `json:"flags,omitempty"`
	Schema    json.RawMessage `json:"configurationSchema,omitempty"`
	SchemaRow json.RawMessage `json:"configurationRowSchema,omitempty"`
}

type ComponentWithConfigs struct {
	BranchId int `json:"branchId" validate:"required"`
	*Component
	Configs []*ConfigWithRows `json:"configurations" validate:"required"`
}

func (c *Component) IsTransformation() bool {
	return c.Type == TransformationType
}

func (c *Component) IsSharedCode() bool {
	return c.Id == ShareCodeComponentId
}

func (c *Component) IsDeprecated() bool {
	for _, flag := range c.Flags {
		if flag == DeprecatedFlag {
			return true
		}
	}
	return false
}

// remoteComponentsProvider - interface for Storage API.
type remoteComponentsProvider interface {
	GetComponent(componentId string) (*Component, error)
}

type ComponentsMap struct {
	mutex          *sync.Mutex
	remoteProvider remoteComponentsProvider
	components     map[string]*Component
}

func NewComponentsMap(remoteProvider remoteComponentsProvider) *ComponentsMap {
	return &ComponentsMap{
		mutex:          &sync.Mutex{},
		remoteProvider: remoteProvider,
		components:     make(map[string]*Component),
	}
}

func (c *ComponentsMap) AllLoaded() []*Component {
	var components []*Component
	for _, c := range c.components {
		components = append(components, c)
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].Id < components[j].Id
	})
	return components
}

func (c *ComponentsMap) Get(key ComponentKey) (*Component, error) {
	// Load component from cache if present
	if component, found := c.doGet(key); found {
		return component, nil
	}

	// Remote provider can be nil in tests, prevent panic
	if c.remoteProvider == nil {
		return nil, fmt.Errorf(`cannot load component "%s": remote provider is not set`, key.Id)
	}

	// Or by API
	if component, err := c.remoteProvider.GetComponent(key.Id); err == nil {
		return component, nil
	} else {
		return nil, err
	}
}

func (c *ComponentsMap) Set(component *Component) {
	if component == nil {
		panic(fmt.Errorf("component is not set"))
	}
	c.doSet(component)
}

func (c *ComponentsMap) doGet(key ComponentKey) (*Component, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	component, found := c.components[key.String()]
	return component, found
}

func (c *ComponentsMap) doSet(component *Component) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.components[component.String()] = component
}
