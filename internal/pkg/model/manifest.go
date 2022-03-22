package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type RecordPath interface {
	GetAbsPath() AbsPath
	// Path gets path relative to the top dir, it is parent path + relative path.
	String() string
	// GetRelativePath - for example path of the object inside parent object/path.
	GetRelativePath() string
	// SetRelativePath - for example path of the object inside parent object/path.
	SetRelativePath(string)
	// GetParentPath - for example path of the parent object.
	GetParentPath() string
	// SetParentPath - for example path of the parent object.
	SetParentPath(string)
	// IsParentPathSet returns true if the parent path is set/resolved.
	IsParentPathSet() bool
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
	AbsPath `json:"path"`
}

type ConfigManifest struct {
	ConfigKey
	AbsPath   `json:"path"`
	Relations Relations              `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables definition
	Metadata  *orderedmap.OrderedMap `json:"metadata,omitempty"`
}

type ConfigRowManifest struct {
	ConfigRowKey
	AbsPath   `json:"path"`
	Relations Relations `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables values definition
}

type ConfigManifestWithRows struct {
	ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

func (b BranchManifest) NewEmptyObject() Object {
	return &Branch{BranchKey: b.BranchKey}
}

func (c ConfigManifest) NewEmptyObject() Object {
	return &Config{ConfigKey: c.ConfigKey}
}

func (r ConfigRowManifest) NewEmptyObject() Object {
	return &ConfigRow{ConfigRowKey: r.ConfigRowKey}
}

// ParentKey - config parent can be modified via Relations, for example variables config is embedded in another config.
func (c ConfigManifest) ParentKey() (Key, error) {
	if parentKey, err := c.Relations.ParentKey(c.Key()); err != nil {
		return nil, err
	} else if parentKey != nil {
		return parentKey, nil
	}

	// No parent defined via "Relations" -> parent is branch
	return c.ConfigKey.ParentKey()
}

func (c *ConfigManifest) GetRelations() Relations {
	return c.Relations
}

func (r *ConfigRowManifest) GetRelations() Relations {
	return r.Relations
}

func (c *ConfigManifest) SetRelations(relations Relations) {
	c.Relations = relations
}

func (r *ConfigRowManifest) SetRelations(relations Relations) {
	r.Relations = relations
}

func (c *ConfigManifest) AddRelation(relation Relation) {
	c.Relations.Add(relation)
}

func (c *ConfigManifest) MetadataMap() map[string]string {
	metadata := make(map[string]string)
	if c.Metadata != nil {
		for _, key := range c.Metadata.Keys() {
			val, _ := c.Metadata.Get(key)
			metadata[key] = val.(string)
		}
	}
	return metadata
}

func (r *ConfigRowManifest) AddRelation(relation Relation) {
	r.Relations.Add(relation)
}
