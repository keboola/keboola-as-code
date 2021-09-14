package components

import (
	"fmt"
	"sort"
	"sync"

	"keboola-as-code/src/model"
)

type remoteProvider interface {
	GetComponent(componentId string) (*model.Component, error)
}

type Provider struct {
	mutex          *sync.Mutex
	remoteProvider remoteProvider
	components     map[string]*model.Component
}

func NewProvider(remoteProvider remoteProvider) *Provider {
	return &Provider{
		mutex:          &sync.Mutex{},
		remoteProvider: remoteProvider,
		components:     make(map[string]*model.Component),
	}
}

func (c *Provider) AllLoaded() []*model.Component {
	var components []*model.Component
	for _, c := range c.components {
		components = append(components, c)
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].Id < components[j].Id
	})
	return components
}

func (c *Provider) Get(key model.ComponentKey) (*model.Component, error) {
	// Load component from cache if present
	if component, found := c.doGet(key); found {
		return component, nil
	}

	// Or by API
	if component, err := c.remoteProvider.GetComponent(key.Id); err == nil {
		return component, nil
	} else {
		return nil, err
	}
}

func (c *Provider) Set(component *model.Component) {
	if component == nil {
		panic(fmt.Errorf("component is not set"))
	}
	c.doSet(component)
}

func (c *Provider) doGet(key model.ComponentKey) (*model.Component, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	component, found := c.components[key.String()]
	return component, found
}

func (c *Provider) doSet(component *model.Component) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.components[component.String()] = component
}
