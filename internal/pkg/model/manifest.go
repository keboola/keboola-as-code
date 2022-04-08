package model

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type RecordPath interface {
	Path() AbsPath
	SetPath(path AbsPath)
}

// ObjectManifest - manifest record for a object.
type ObjectManifest interface {
	Key
	RecordPath
	Key() Key // unique key for map -> for fast access
	NewEmptyObject() Object
}

type ObjectManifestWithRelations interface {
	ObjectManifest
	GetRelations() Relations
	SetRelations(relations Relations)
	AddRelation(relation Relation)
}

type BranchManifest struct {
	BranchKey
	AbsPath
}

type ConfigManifest struct {
	ConfigKey
	AbsPath
	Relations Relations              `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables definition
	Metadata  *orderedmap.OrderedMap `json:"metadata,omitempty"`
}

type ConfigRowManifest struct {
	ConfigRowKey
	AbsPath
	Relations Relations `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables values definition
}

type ConfigManifestWithRows struct {
	ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

func (m BranchManifest) String() string {
	if m.AbsPath.IsSet() {
		return fmt.Sprintf(`%s "%s"`, m.Kind().Name, m.AbsPath.String())
	}
	return m.Key().String()
}

func (m ConfigManifest) String() string {
	if m.AbsPath.IsSet() {
		return fmt.Sprintf(`%s "%s"`, m.Kind().Name, m.AbsPath.String())
	}
	return m.Key().String()
}

func (m ConfigRowManifest) String() string {
	if m.AbsPath.IsSet() {
		return fmt.Sprintf(`%s "%s"`, m.Kind().Name, m.AbsPath.String())
	}
	return m.Key().String()
}

func (m BranchManifest) NewEmptyObject() Object {
	return &Branch{BranchKey: m.BranchKey}
}

func (m ConfigManifest) NewEmptyObject() Object {
	return &Config{ConfigKey: m.ConfigKey}
}

func (m ConfigRowManifest) NewEmptyObject() Object {
	return &ConfigRow{ConfigRowKey: m.ConfigRowKey}
}

// ParentKey - config parent can be modified via Relations, for example variables config is embedded in another config.
func (m ConfigManifest) ParentKey() (Key, error) {
	if parentKey, err := m.Relations.ParentKey(m.Key()); err != nil {
		return nil, err
	} else if parentKey != nil {
		return parentKey, nil
	}

	// No parent defined via "Relations" -> parent is branch
	return m.ConfigKey.ParentKey()
}

func (m *ConfigManifest) GetRelations() Relations {
	return m.Relations
}

func (m *ConfigRowManifest) GetRelations() Relations {
	return m.Relations
}

func (m *ConfigManifest) SetRelations(relations Relations) {
	m.Relations = relations
}

func (m *ConfigRowManifest) SetRelations(relations Relations) {
	m.Relations = relations
}

func (m *ConfigManifest) AddRelation(relation Relation) {
	m.Relations.Add(relation)
}

func (m *ConfigManifest) MetadataMap() map[string]string {
	metadata := make(map[string]string)
	if m.Metadata != nil {
		for _, key := range m.Metadata.Keys() {
			val, _ := m.Metadata.Get(key)
			metadata[key] = val.(string)
		}
	}
	return metadata
}

func (m *ConfigRowManifest) AddRelation(relation Relation) {
	m.Relations.Add(relation)
}
