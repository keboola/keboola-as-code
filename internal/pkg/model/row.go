package model

import (
	"fmt"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const (
	ConfigRowKind = "config row"
	RowAbbr       = "R"
)

type RowId string

type ConfigRowKey struct {
	BranchId    BranchId    `json:"-" validate:"required_in_project"`
	ComponentId ComponentId `json:"-" validate:"required"`
	ConfigId    ConfigId    `json:"-" validate:"required"`
	Id          RowId       `json:"id" validate:"required" `
}

// ConfigRow https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type ConfigRow struct {
	ConfigRowKey
	Name              string                 `json:"name" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" descriptionFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	SharedCode        *SharedCodeRow         `json:"-" validate:"omitempty,dive" diff:"true"`
	Relations         Relations              `json:"-" validate:"dive" diff:"true"`
}

func (k Kind) IsConfigRow() bool {
	return k.Name == ConfigRowKind
}

func (v RowId) String() string {
	return string(v)
}

func (k ConfigRowKey) Level() ObjectLevel {
	return 3
}

func (k ConfigRowKey) Kind() Kind {
	return Kind{Name: ConfigRowKind, Abbr: RowAbbr}
}

func (k ConfigRowKey) String() string {
	if k.BranchId == 0 {
		// Row in a template
		return fmt.Sprintf(`%s "component:%s/config:%s"`, k.Kind().Name, k.ComponentId, k.Id)
	}
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/row:%s"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.Id)
}

func (k ConfigRowKey) Key() Key {
	return k
}

func (k ConfigRowKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (k ConfigRowKey) ComponentKey() ComponentKey {
	return ComponentKey{Id: k.ComponentId}
}

func (k *ConfigRowKey) GetComponentId() ComponentId {
	return k.ComponentId
}

func (k ConfigRowKey) BranchKey() BranchKey {
	return k.ConfigKey().BranchKey()
}

func (k ConfigRowKey) ConfigKey() ConfigKey {
	return ConfigKey{BranchId: k.BranchId, ComponentId: k.ComponentId, Id: k.ConfigId}
}

func (k ConfigRowKey) ObjectId() string {
	return k.Id.String()
}

func (k ConfigRowKey) NewObject() Object {
	return &ConfigRow{ConfigRowKey: k}
}

func (k ConfigRowKey) NewObjectManifest() ObjectManifest {
	return &ConfigRowManifest{ConfigRowKey: k}
}

func (r ConfigRow) ObjectName() string {
	return r.Name
}

func (r *ConfigRow) GetContent() *orderedmap.OrderedMap {
	return r.Content
}

func (r *ConfigRow) GetRelations() Relations {
	return r.Relations
}

func (r *ConfigRow) SetRelations(relations Relations) {
	r.Relations = relations
}

func (r *ConfigRow) AddRelation(relation Relation) {
	r.Relations.Add(relation)
}

func (r *ConfigRow) ToApiValues() (map[string]string, error) {
	configJson, err := json.EncodeString(r.Content, false)
	if err != nil {
		return nil, utils.PrefixError(`cannot JSON encode config configuration`, err)
	}

	return map[string]string{
		"name":              r.Name,
		"description":       r.Description,
		"changeDescription": r.ChangeDescription,
		"isDisabled":        strconv.FormatBool(r.IsDisabled),
		"configuration":     configJson,
	}, nil
}
