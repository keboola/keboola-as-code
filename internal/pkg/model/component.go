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

func NewComponentsProvider(ctx context.Context, logger log.Logger, storageApiClient client.Sender) (*ComponentsProvider, error) {
	p := &ComponentsProvider{updateLock: &sync.RWMutex{}, logger: logger, storageApiClient: storageApiClient}
	if err := p.doUpdate(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// RLock acquire read lock, before getting Components().
// Update() is blocked until the read is finished.
func (p *ComponentsProvider) RLock() {
	p.updateLock.RLock()
}

// RUnlock release read lock.
func (p *ComponentsProvider) RUnlock() {
	p.updateLock.RUnlock()
}

func (p *ComponentsProvider) Components() *ComponentsMap {
	return p.value
}

func (p *ComponentsProvider) Update(ctx context.Context) {
	go func() {
		// Error is logged
		_ = p.doUpdate(ctx)
	}()
}

func (p *ComponentsProvider) doUpdate(ctx context.Context) error {
	startTime := time.Now()
	p.logger.Infof(`components update: started`)
	p.updateLock.Lock()

	defer p.updateLock.Unlock()
	p.logger.Infof(`components update: acquired lock`)
	lockTime := time.Now()

	ctx, cancel := context.WithTimeout(ctx, ComponentsUpdateTimeout)
	defer cancel()
	if index, err := storageapi.IndexComponentsRequest().Send(ctx, p.storageApiClient); err == nil {
		p.value = NewComponentsMap(index.Components)
		p.logger.Infof("components updated: finished | %s / %s", time.Since(startTime), time.Since(lockTime))
		return nil
	} else {
		p.logger.Errorf("components update: error: %w", err)
		return err
	}
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
