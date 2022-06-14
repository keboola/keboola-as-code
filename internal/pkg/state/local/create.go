package local

import (
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *Manager) createObject(key model.Key, name string) (model.Object, error) {
	switch k := key.(type) {
	case model.ConfigKey:
		component, err := m.state.Components().Get(k.ComponentKey())
		if err != nil {
			return nil, err
		}
		content, err := generateContent(component.Schema, component.EmptyConfig)
		if err != nil {
			return nil, err
		}
		return &model.Config{
			ConfigKey: k,
			Name:      name,
			Content:   content,
		}, nil
	case model.ConfigRowKey:
		component, err := m.state.Components().Get(k.ComponentKey())
		if err != nil {
			return nil, err
		}
		content, err := generateContent(component.SchemaRow, component.EmptyConfigRow)
		if err != nil {
			return nil, err
		}
		return &model.ConfigRow{
			ConfigRowKey: k,
			Name:         name,
			Content:      content,
		}, nil
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}

func generateContent(schemaDef []byte, defaultConfig *orderedmap.OrderedMap) (*orderedmap.OrderedMap, error) {
	// Use default configuration if defined in the component's metadata
	if len(defaultConfig.Keys()) > 0 {
		return defaultConfig, nil
	}

	// Otherwise, generate configuration from the JSON schema
	return schema.GenerateDocument(schemaDef)
}
