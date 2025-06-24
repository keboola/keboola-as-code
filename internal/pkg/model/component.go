package model

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/sasha-s/go-deadlock"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const ComponentsUpdateTimeout = 20 * time.Second

type ComponentsProvider struct {
	updateLock       *deadlock.RWMutex
	logger           log.Logger
	keboolaPublicAPI *keboola.PublicAPI
	value            *ComponentsMap
}

func NewComponentsProvider(index *keboola.IndexComponents, logger log.Logger, keboolaPublicAPI *keboola.PublicAPI) *ComponentsProvider {
	return &ComponentsProvider{
		updateLock:       &deadlock.RWMutex{},
		logger:           logger,
		keboolaPublicAPI: keboolaPublicAPI,
		value:            NewComponentsMap(index.Components),
	}
}

func (p *ComponentsProvider) Components() *ComponentsMap {
	p.updateLock.RLock()
	defer p.updateLock.RUnlock()
	return p.value
}

func (p *ComponentsProvider) UpdateAsync(ctx context.Context) {
	go func() {
		if err := p.Update(ctx); err != nil {
			p.logger.Errorf(ctx, "components update failed: %s", err)
		}
	}()
}

func (p *ComponentsProvider) Update(ctx context.Context) error {
	startTime := time.Now()
	p.logger.Infof(ctx, "components update started")
	ctx, cancel := context.WithTimeoutCause(ctx, ComponentsUpdateTimeout, errors.New("components update timeout"))
	defer cancel()

	// Get index
	index, err := p.index(ctx)
	if err != nil {
		return err
	}

	// Update value
	p.updateLock.Lock()
	defer p.updateLock.Unlock()
	p.value = NewComponentsMap(index.Components)
	p.logger.WithDuration(time.Since(startTime)).Infof(ctx, "components update finished")
	return nil
}

func (p *ComponentsProvider) index(ctx context.Context) (*keboola.IndexComponents, error) {
	return p.keboolaPublicAPI.IndexComponentsRequest().Send(ctx)
}

type (
	componentsMap = keboola.ComponentsMap
	ComponentsMap struct {
		componentsMap
		components                  keboola.Components
		defaultBucketsByComponentID map[keboola.ComponentID]string
		defaultBucketsByPrefix      map[string]keboola.ComponentID
		usedLock                    *deadlock.Mutex
		used                        map[keboola.ComponentID]bool
	}
)

func NewComponentsMap(components keboola.Components) *ComponentsMap {
	v := &ComponentsMap{
		componentsMap:               components.ToMap(),
		components:                  components,
		defaultBucketsByComponentID: make(map[keboola.ComponentID]string),
		defaultBucketsByPrefix:      make(map[string]keboola.ComponentID),
		used:                        make(map[keboola.ComponentID]bool),
		usedLock:                    &deadlock.Mutex{},
	}

	// Init aux maps
	for _, component := range components {
		if component.Data.DefaultBucket && component.Data.DefaultBucketStage != "" {
			v.addDefaultBucketPrefix(component)
		}
	}

	return v
}

func (m ComponentsMap) NewComponentList() keboola.Components {
	return m.components.NewComponentList()
}

func (m ComponentsMap) All() keboola.Components {
	return m.components
}

func (m ComponentsMap) Get(id keboola.ComponentID) (*keboola.Component, bool) {
	v, ok := m.componentsMap.Get(id)
	if ok {
		m.usedLock.Lock()
		m.used[id] = true
		m.usedLock.Unlock()
	}
	return v, ok
}

func (m ComponentsMap) Has(id keboola.ComponentID) bool {
	_, ok := m.Get(id)
	return ok
}

func (m ComponentsMap) GetOrErr(id keboola.ComponentID) (*keboola.Component, error) {
	v, ok := m.Get(id)
	if !ok {
		return nil, errors.Errorf(`component "%s" not found`, id)
	}
	return v, nil
}

func (m ComponentsMap) Used() keboola.Components {
	out := make(keboola.Components, 0)
	for id := range m.used {
		component, _ := m.Get(id)
		out = append(out, component)
	}
	keboola.SortComponents(out)
	return out
}

func (m ComponentsMap) GetDefaultBucketByTableID(tableID string) (keboola.ComponentID, keboola.ConfigID, bool) {
	dotIndex := strings.LastIndex(tableID, ".")
	if dotIndex < 1 {
		return "", "", false
	}

	bucketID := tableID[0:dotIndex]
	if !strings.Contains(bucketID, "-") {
		return "", "", false
	}

	bucketPrefix := bucketID[0 : strings.LastIndex(bucketID, "-")+1]
	configID := keboola.ConfigID(bucketID[strings.LastIndex(bucketID, "-")+1:])

	componentID, found := m.defaultBucketsByPrefix[bucketPrefix]
	if !found {
		return "", "", false
	}

	return componentID, configID, len(componentID) > 0 && len(configID) > 0
}

func (m ComponentsMap) GetDefaultBucketByComponentID(componentID keboola.ComponentID, configID keboola.ConfigID) (string, bool) {
	defaultBucketPrefix, found := m.defaultBucketsByComponentID[componentID]
	if !found {
		return "", false
	}
	return fmt.Sprintf("%s%s", defaultBucketPrefix, configID), true
}

func (m ComponentsMap) addDefaultBucketPrefix(component *keboola.Component) {
	r := regexpcache.MustCompile(`(?i)[^a-zA-Z0-9-]`)
	bucketPrefix := fmt.Sprintf(`%s.c-%s-`, component.Data.DefaultBucketStage, r.ReplaceAllString(component.ID.String(), `-`))
	m.defaultBucketsByComponentID[component.ID] = bucketPrefix
	m.defaultBucketsByPrefix[bucketPrefix] = component.ID
}
