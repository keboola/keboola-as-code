package model

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/umisama/go-regexpcache"
)

type componentsMap = storageapi.ComponentsMap
type ComponentsMap struct {
	componentsMap
	used                        map[storageapi.ComponentID]bool
	defaultBucketsByComponentId map[storageapi.ComponentID]string
	defaultBucketsByPrefix      map[string]storageapi.ComponentID
}

func NewComponentsMap(components storageapi.Components) ComponentsMap {
	v := ComponentsMap{
		componentsMap:               components.ToMap(),
		used:                        make(map[storageapi.ComponentID]bool),
		defaultBucketsByComponentId: make(map[storageapi.ComponentID]string),
		defaultBucketsByPrefix:      make(map[string]storageapi.ComponentID),
	}

	// Init aux maps
	for _, component := range components {
		if component.Data.DefaultBucket && component.Data.DefaultBucketStage != "" {
			v.addDefaultBucketPrefix(component)
		}
	}

	return v
}

func (m ComponentsMap) Get(id storageapi.ComponentID) (*storageapi.Component, bool) {
	v, ok := m.componentsMap.Get(id)
	if ok {
		m.used[id] = true
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

func (m *ComponentsMap) Used() map[storageapi.ComponentID]bool {
	return m.used
}

func (m *ComponentsMap) GetDefaultBucketByTableId(tableId string) (storageapi.ComponentID, storageapi.ConfigID, bool) {
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

func (m *ComponentsMap) GetDefaultBucketByComponentId(componentId storageapi.ComponentID, configId storageapi.ConfigID) (string, bool) {
	defaultBucketPrefix, found := m.defaultBucketsByComponentId[componentId]
	if !found {
		return "", false
	}
	return fmt.Sprintf("%s%s", defaultBucketPrefix, configId), true
}

func (m *ComponentsMap) addDefaultBucketPrefix(component *storageapi.Component) {
	r := regexpcache.MustCompile(`(?i)[^a-zA-Z0-9-]`)
	bucketPrefix := fmt.Sprintf(`%s.v-%s-`, component.Data.DefaultBucketStage, r.ReplaceAllString(component.ID.String(), `-`))
	m.defaultBucketsByComponentId[component.ID] = bucketPrefix
	m.defaultBucketsByPrefix[bucketPrefix] = component.ID
}
