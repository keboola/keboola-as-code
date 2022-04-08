package model

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

var ConfigKind = Kind{Name: "config", Abbr: "C", ToMany: true}

type ConfigId string

type ConfigKey struct {
	BranchKey   `validate:"dive"`
	ComponentId ComponentId `json:"componentId" validate:"required"`
	ConfigId    ConfigId    `json:"id" validate:"required"`
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name              string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" descriptionFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	Relations         Relations              `json:"-" validate:"dive" diff:"true"`
	Metadata          map[string]string      `json:"-" validate:"dive" diff:"true"`
}

type ConfigWithRows struct {
	*Config
	Rows []*ConfigRow `json:"rows"`
}

func (k Kind) IsConfig() bool {
	return k == ConfigKind
}

func (v ConfigId) String() string {
	return string(v)
}

func (k ConfigKey) Level() ObjectLevel {
	return 20
}

func (k ConfigKey) Kind() Kind {
	return ConfigKind
}

func (k ConfigKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k ConfigKey) LogicPath() string {
	if k.BranchId == 0 {
		// Config in a template
		return fmt.Sprintf(`component:%s/config:%s`, k.ComponentId, k.ConfigId)
	}
	return fmt.Sprintf(`branch:%d/component:%s/config:%s`, k.BranchId, k.ComponentId, k.ConfigId)
}

func (k ConfigKey) Key() Key {
	return k
}

func (k ConfigKey) ParentKey() (Key, error) {
	if k.BranchId == 0 {
		// Configs in template are not related to any branch
		return nil, nil
	}
	return k.BranchKey, nil
}

func (k ConfigKey) ObjectId() string {
	return k.ComponentId.String() + ":" + k.ConfigId.String()
}

func (k ConfigKey) ComponentKey() ComponentKey {
	return ComponentKey{Id: k.ComponentId}
}

func (k ConfigKey) NewObject() Object {
	return &Config{ConfigKey: k}
}

func (k ConfigKey) NewObjectManifest() ObjectManifest {
	return &ConfigManifest{ConfigKey: k}
}

func (c *Config) ObjectName() string {
	return c.Name
}

func (c *Config) GetComponentId() ComponentId {
	return c.ComponentId
}

func (c *Config) GetContent() *orderedmap.OrderedMap {
	return c.Content
}

func (c *Config) GetRelations() Relations {
	return c.Relations
}

func (c *Config) SetRelations(relations Relations) {
	c.Relations = relations
}

func (c *Config) AddRelation(relation Relation) {
	c.Relations.Add(relation)
}

func (c *Config) MetadataOrderedMap() *orderedmap.OrderedMap {
	ordered := orderedmap.New()
	for key, val := range c.Metadata {
		ordered.Set(key, val)
	}
	ordered.SortKeys(sort.Strings)
	return ordered
}

func (c *Config) ToApiValues() (map[string]string, error) {
	configJson, err := json.EncodeString(c.Content, false)
	if err != nil {
		return nil, errors.PrefixError(`cannot JSON encode config configuration`, err)
	}

	return map[string]string{
		"name":              c.Name,
		"description":       c.Description,
		"changeDescription": c.ChangeDescription,
		"configuration":     configJson,
	}, nil
}

// ParentKey - config parent can be modified via Relations, for example variables config is embedded in another config.
func (c *Config) ParentKey() (Key, error) {
	if parentKey, err := c.Relations.ParentKey(c.Key()); err != nil {
		return nil, err
	} else if parentKey != nil {
		return parentKey, nil
	}

	// No parent defined via "Relations" -> parent is branch
	return c.ConfigKey.ParentKey()
}

func (c *ConfigWithRows) SortRows() {
	sort.SliceStable(c.Rows, func(i, j int) bool {
		return c.Rows[i].Name < c.Rows[j].Name
	})
}
