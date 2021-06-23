package model

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"keboola-as-code/src/json"
	"sort"
	"strconv"
)

const (
	MetaFileTag   = "metaFile:true"
	ConfigFileTag = "configFile:true"
)

// Token https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
type Token struct {
	Id       string     `json:"id"`
	Token    string     `json:"token"`
	IsMaster bool       `json:"isMasterToken"`
	Owner    TokenOwner `json:"owner"`
}

func (t *Token) ProjectId() int {
	return t.Owner.Id
}

func (t *Token) ProjectName() string {
	return t.Owner.Name
}

type TokenOwner struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type BranchKey struct {
	Id int `json:"id" validate:"required"`
}

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	BranchKey
	Name        string `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description string `json:"description" diff:"true" metaFile:"true"`
	IsDefault   bool   `json:"isDefault" diff:"true" metaFile:"true"`
}

type ComponentKey struct {
	Id string `json:"id" validate:"required"`
}

// Component https://keboola.docs.apiary.io/#reference/components-and-configurations/get-development-branch-components/get-development-branch-components
type Component struct {
	ComponentKey
	Type      string                 `json:"type" validate:"required"`
	Name      string                 `json:"name" validate:"required"`
	Schema    map[string]interface{} `json:"configurationSchema,omitempty"`
	SchemaRow map[string]interface{} `json:"configurationRowSchema,omitempty"`
}

type ComponentWithConfigs struct {
	BranchId int `json:"branchId" validate:"required"`
	*Component
	Configs []*ConfigWithRows `json:"configurations" validate:"required"`
}

type ConfigKey struct {
	BranchId    int    `json:"branchId" validate:"required"`
	ComponentId string `json:"componentId" validate:"required"`
	Id          string `json:"id" validate:"required"`
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name              string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" metaFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
}

type ConfigWithRows struct {
	*Config
	Rows []*ConfigRow `json:"rows"`
}

func (c *ConfigWithRows) SortRows() {
	sort.SliceStable(c.Rows, func(i, j int) bool {
		return c.Rows[i].Name < c.Rows[j].Name
	})
}

type ConfigRowKey struct {
	BranchId    int    `json:"-" validate:"required"`
	ComponentId string `json:"-" validate:"required"`
	ConfigId    string `json:"-" validate:"required"`
	Id          string `json:"id" validate:"required" `
}

// ConfigRow https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type ConfigRow struct {
	ConfigRowKey
	Name              string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" metaFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
}

// Job - Storage API job
type Job struct {
	Id      int                    `json:"id" validate:"required"`
	Status  string                 `json:"status" validate:"required"`
	Url     string                 `json:"url" validate:"required"`
	Results map[string]interface{} `json:"results"`
}

// Event https://keboola.docs.apiary.io/#reference/events/events/create-event
type Event struct {
	Id string `json:"id"`
}

func (k *BranchKey) String() string {
	return fmt.Sprintf("01_%d", k.Id)
}

func (k *ComponentKey) String() string {
	return fmt.Sprintf("02_%s", k.Id)
}

func (k *ConfigKey) String() string {
	return fmt.Sprintf("03_%d_%s_%s", k.BranchId, k.ComponentId, k.Id)
}

func (k *ConfigRowKey) String() string {
	return fmt.Sprintf("04_%d_%s_%s_%s", k.BranchId, k.ComponentId, k.ConfigId, k.Id)
}

func (k *ConfigKey) BranchKey() BranchKey {
	return BranchKey{Id: k.BranchId}
}

func (k *ConfigRowKey) ConfigKey() ConfigKey {
	return ConfigKey{BranchId: k.BranchId, ComponentId: k.ComponentId, Id: k.ConfigId}
}

func (r *ConfigRow) ToApiValues() (map[string]string, error) {
	configJson, err := json.Encode(r.Content, false)
	if err != nil {
		return nil, fmt.Errorf(`cannot JSON encode config configuration: %s`, err)
	}

	return map[string]string{
		"name":              r.Name,
		"description":       r.Description,
		"changeDescription": r.ChangeDescription,
		"isDisabled":        strconv.FormatBool(r.IsDisabled),
		"configuration":     string(configJson),
	}, nil
}

func (c *Config) ToApiValues() (map[string]string, error) {
	configJson, err := json.Encode(c.Content, false)
	if err != nil {
		return nil, fmt.Errorf(`cannot JSON encode config configuration: %s`, err)
	}

	return map[string]string{
		"name":              c.Name,
		"description":       c.Description,
		"changeDescription": c.ChangeDescription,
		"configuration":     string(configJson),
	}, nil
}
