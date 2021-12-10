package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const (
	VariablesComponentId     = ComponentId(`keboola.variables`)
	SchedulerComponentId     = ComponentId("keboola.scheduler")
	DeprecatedFlag           = `deprecated`
	ExcludeFromNewListFlag   = `excludeFromNewList`
	ComponentTypeCodePattern = `code-pattern`
	ComponentTypeProcessor   = `processor`
)

// Component https://keboola.docs.apiary.io/#reference/components-and-configurations/get-development-branch-components/get-development-branch-components
type Component struct {
	ComponentKey
	Type           string                 `json:"type" validate:"required"`
	Name           string                 `json:"name" validate:"required"`
	Flags          []string               `json:"flags,omitempty"`
	Schema         json.RawMessage        `json:"configurationSchema,omitempty"`
	SchemaRow      json.RawMessage        `json:"configurationRowSchema,omitempty"`
	EmptyConfig    *orderedmap.OrderedMap `json:"emptyConfiguration,omitempty"`
	EmptyConfigRow *orderedmap.OrderedMap `json:"emptyConfigurationRow,omitempty"`
	Data           ComponentData          `json:"data"`
}

type ComponentData struct {
	DefaultBucket      bool   `json:"default_bucket"`       //nolint: tagliatelle
	DefaultBucketStage string `json:"default_bucket_stage"` //nolint: tagliatelle
}

type ComponentWithConfigs struct {
	BranchId BranchId `json:"branchId" validate:"required"`
	*Component
	Configs []*ConfigWithRows `json:"configurations" validate:"required"`
}

func (c *Component) IsTransformation() bool {
	return c.Type == TransformationType
}

func (c *Component) IsSharedCode() bool {
	return c.Id == SharedCodeComponentId
}

func (c *Component) IsVariables() bool {
	return c.Id == VariablesComponentId
}

func (c *Component) IsCodePattern() bool {
	return c.Type == ComponentTypeCodePattern
}

func (c *Component) IsProcessor() bool {
	return c.Type == ComponentTypeProcessor
}

func (c *Component) IsScheduler() bool {
	return c.Id == SchedulerComponentId
}

func (c *Component) IsOrchestrator() bool {
	return c.Id == OrchestratorComponentId
}

func (c *Component) IsDeprecated() bool {
	for _, flag := range c.Flags {
		if flag == DeprecatedFlag {
			return true
		}
	}
	return false
}

func (c *Component) IsExcludedFromNewList() bool {
	for _, flag := range c.Flags {
		if flag == ExcludeFromNewListFlag {
			return true
		}
	}
	return false
}

// RemoteComponentsProvider - interface for Storage API.
type RemoteComponentsProvider interface {
	GetComponent(componentId ComponentId) (*Component, error)
}

type ComponentsMap struct {
	mutex                       *sync.Mutex
	remoteProvider              RemoteComponentsProvider
	components                  map[string]*Component
	defaultBucketsByComponentId map[ComponentId]string
	defaultBucketsByPrefix      map[string]ComponentId
}

func NewComponentsMap(remoteProvider RemoteComponentsProvider) *ComponentsMap {
	return &ComponentsMap{
		mutex:                       &sync.Mutex{},
		remoteProvider:              remoteProvider,
		components:                  make(map[string]*Component),
		defaultBucketsByComponentId: make(map[ComponentId]string),
		defaultBucketsByPrefix:      make(map[string]ComponentId),
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
		c.doSet(component)
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
	if component.Data.DefaultBucket && component.Data.DefaultBucketStage != "" {
		c.addDefaultBucketPrefix(component)
	}
}

func (c *ComponentsMap) addDefaultBucketPrefix(component *Component) {
	r := regexpcache.MustCompile(`(?i)[^a-zA-Z0-9-]`)
	bucketPrefix := fmt.Sprintf(`%s.c-%s-`, component.Data.DefaultBucketStage, r.ReplaceAllString(component.Id.String(), `-`))
	c.defaultBucketsByComponentId[component.Id] = bucketPrefix
	c.defaultBucketsByPrefix[bucketPrefix] = component.Id
}

func (c *ComponentsMap) GetDefaultBucketByTableId(tableId string) (ComponentId, ConfigId, bool) {
	bucketId := tableId[0:strings.LastIndex(tableId, ".")]
	if !strings.Contains(bucketId, "-") {
		return "", "", false
	}
	bucketPrefix := bucketId[0 : strings.LastIndex(tableId, "-")+1]
	configId := ConfigId(bucketId[strings.LastIndex(tableId, "-")+1:])

	componentId, found := c.defaultBucketsByPrefix[bucketPrefix]
	if !found {
		return "", "", false
	}
	return componentId, configId, len(componentId) > 0 && len(configId) > 0
}

func (c *ComponentsMap) GetDefaultBucketByComponentId(componentId ComponentId, configId ConfigId) (string, bool) {
	defaultBucketPrefix, found := c.defaultBucketsByComponentId[componentId]
	if !found {
		return "", false
	}
	return fmt.Sprintf("%s%s", defaultBucketPrefix, configId), true
}
