package model

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const ComponentsUpdateTimeout = 20 * time.Second

type ComponentsProvider struct {
	updateLock       *sync.RWMutex
	logger           log.Logger
	storageApiClient client.Sender
	value            *ComponentsMap
}

func NewComponentsProvider(index *storageapi.IndexComponents, logger log.Logger, storageApiClient client.Sender) *ComponentsProvider {
	return &ComponentsProvider{
		updateLock:       &sync.RWMutex{},
		logger:           logger,
		storageApiClient: storageApiClient,
		value:            NewComponentsMap(index.Components),
	}
}

func (p *ComponentsProvider) Components() *ComponentsMap {
	p.updateLock.RLock()
	defer p.updateLock.RUnlock()
	return p.value
}

func (p *ComponentsProvider) Update(ctx context.Context) {
	go func() {
		startTime := time.Now()
		p.logger.Infof("components update started")
		ctx, cancel := context.WithTimeout(ctx, ComponentsUpdateTimeout)
		defer cancel()
		if index, err := p.index(ctx); err == nil {
			p.updateLock.Lock()
			defer p.updateLock.Unlock()
			p.value = NewComponentsMap(index.Components)
			p.logger.Infof("components update finished | %s", time.Since(startTime))
		} else {
			p.logger.Errorf("components update failed: %w", err)
		}
	}()
}

func (p *ComponentsProvider) index(ctx context.Context) (*storageapi.IndexComponents, error) {
	return storageapi.IndexComponentsRequest().Send(ctx, p.storageApiClient)
}

type componentsMap = storageapi.ComponentsMap
type ComponentsMap struct {
	componentsMap
	components                  storageapi.Components
	defaultBucketsByComponentId map[storageapi.ComponentID]string
	defaultBucketsByPrefix      map[string]storageapi.ComponentID
	usedLock                    *sync.Mutex
	used                        map[storageapi.ComponentID]bool
}

func NewComponentsMap(components storageapi.Components) *ComponentsMap {
	v := &ComponentsMap{
		componentsMap:               components.ToMap(),
		components:                  components,
		defaultBucketsByComponentId: make(map[storageapi.ComponentID]string),
		defaultBucketsByPrefix:      make(map[string]storageapi.ComponentID),
		used:                        make(map[storageapi.ComponentID]bool),
		usedLock:                    &sync.Mutex{},
	}

	// Init aux maps
	for _, component := range components {
		if component.Data.DefaultBucket && component.Data.DefaultBucketStage != "" {
			v.addDefaultBucketPrefix(component)
		}
	}

	return v
}

func (m ComponentsMap) NewComponentList() storageapi.Components {
	return m.components.NewComponentList()
}

func (m ComponentsMap) Get(id storageapi.ComponentID) (*storageapi.Component, bool) {
	v, ok := m.componentsMap.Get(id)
	if ok {
		m.usedLock.Lock()
		m.used[id] = true
		m.usedLock.Unlock()
	}
	return v, ok
}

func (m ComponentsMap) GetOrErr(id storageapi.ComponentID) (*storageapi.Component, error) {
	v, ok := m.Get(id)
	if !ok {
		return nil, fmt.Errorf(`component "%s" not found`, id)
	}
	return v, nil
}

func (m ComponentsMap) Used() storageapi.Components {
	out := make(storageapi.Components, 0)
	for id := range m.used {
		component, _ := m.Get(id)
		out = append(out, component)
	}
	storageapi.SortComponents(out)
	return out
}

func (m ComponentsMap) GetDefaultBucketByTableId(tableId string) (storageapi.ComponentID, storageapi.ConfigID, bool) {
	dotIndex := strings.LastIndex(tableId, ".")
	if dotIndex < 1 {
		return "", "", false
	}

	bucketId := tableId[0:dotIndex]
	if !strings.Contains(bucketId, "-") {
		return "", "", false
	}

	bucketPrefix := bucketId[0 : strings.LastIndex(bucketId, "-")+1]
	configId := storageapi.ConfigID(bucketId[strings.LastIndex(bucketId, "-")+1:])

	componentId, found := m.defaultBucketsByPrefix[bucketPrefix]
	if !found {
		return "", "", false
	}

	return componentId, configId, len(componentId) > 0 && len(configId) > 0
}

func (m ComponentsMap) GetDefaultBucketByComponentId(componentId storageapi.ComponentID, configId storageapi.ConfigID) (string, bool) {
	defaultBucketPrefix, found := m.defaultBucketsByComponentId[componentId]
	if !found {
		return "", false
	}
	return fmt.Sprintf("%s%s", defaultBucketPrefix, configId), true
}

func (m ComponentsMap) addDefaultBucketPrefix(component *storageapi.Component) {
	r := regexpcache.MustCompile(`(?i)[^a-zA-Z0-9-]`)
	bucketPrefix := fmt.Sprintf(`%s.c-%s-`, component.Data.DefaultBucketStage, r.ReplaceAllString(component.ID.String(), `-`))
	m.defaultBucketsByComponentId[component.ID] = bucketPrefix
	m.defaultBucketsByPrefix[bucketPrefix] = component.ID
}
