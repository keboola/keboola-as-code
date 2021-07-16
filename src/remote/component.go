package remote

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"sort"
	"sync"
)

func (a *StorageApi) GetComponent(componentId string) (*model.Component, error) {
	response := a.GetComponentRequest(componentId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Component), nil
	}
	return nil, response.Err()
}

// GetComponentRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/get-component/get-component
func (a *StorageApi) GetComponentRequest(componentId string) *client.Request {
	component := &model.Component{}
	component.Id = componentId
	return a.
		NewRequest(resty.MethodGet, "components/{componentId}").
		SetPathParam("componentId", componentId).
		SetResult(component).
		OnSuccess(func(response *client.Response) {
			a.Components().Set(component)
		})
}

type ComponentsCache struct {
	mutex      *sync.Mutex
	api        *StorageApi
	components map[string]*model.Component
}

func NewComponentCache(api *StorageApi) *ComponentsCache {
	return &ComponentsCache{
		mutex:      &sync.Mutex{},
		api:        api,
		components: make(map[string]*model.Component),
	}
}

func (c *ComponentsCache) AllLoaded() []*model.Component {
	var components []*model.Component
	for _, c := range c.components {
		components = append(components, c)
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].Id < components[j].Id
	})
	return components
}

func (c *ComponentsCache) Get(key model.ComponentKey) (*model.Component, error) {
	// Load component from cache if present
	if component, found := c.doGet(key); found {
		return component, nil
	}

	// Or by API
	if component, err := c.api.GetComponent(key.Id); err == nil {
		return component, nil
	} else {
		return nil, err
	}
}

func (c *ComponentsCache) Set(component *model.Component) {
	if component == nil {
		panic(fmt.Errorf("component is not set"))
	}
	c.doSet(component)
}

func (c *ComponentsCache) doGet(key model.ComponentKey) (*model.Component, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	component, found := c.components[key.String()]
	return component, found
}

func (c *ComponentsCache) doSet(component *model.Component) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.components[component.String()] = component
}
