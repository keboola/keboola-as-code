package model

import (
	"sort"
	"strconv"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	MetaFileTag                 = "metaFile:true"
	ConfigFileTag               = "configFile:true"
	DescriptionFileTag          = "descriptionFile:true"
	TransformationType          = "transformation"
	SharedCodeComponentId       = "keboola.shared-code"
	ShareCodeTargetComponentKey = `componentId`
	ShareCodeContentKey         = `code_content`
)

type Object interface {
	Key
	Key() Key
	ObjectName() string
}

type ObjectWithContent interface {
	Object
	GetComponentId() string
	GetContent() *orderedmap.OrderedMap
}

type ObjectsProvider interface {
	Naming() Naming
	Components() *ComponentsMap
	All() []ObjectState
	Branches() (branches []*BranchState)
	Configs() (configs []*ConfigState)
	ConfigRows() (rows []*ConfigRowState)
}

// Kind - type of the object, branch, config ...
type Kind struct {
	Name string
	Abbr string
}

// Ticket https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id
type Ticket struct {
	Id string `json:"id"`
}

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

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	BranchKey
	Name        string `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description string `json:"description" diff:"true" descriptionFile:"true"`
	IsDefault   bool   `json:"isDefault" diff:"true" metaFile:"true"`
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name              string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" descriptionFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	Blocks            []*Block               `json:"-"` // loaded transformation's blocks, filled in only for the LOCAL state
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

// ConfigRow https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type ConfigRow struct {
	ConfigRowKey
	Name              string                 `json:"name" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" descriptionFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
}

// Job - Storage API job.
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

// Block - transformation block.
type Block struct {
	BlockKey
	Paths `json:"-"`
	Name  string  `json:"name" validate:"required" metaFile:"true"`
	Codes []*Code `json:"codes" validate:"omitempty,dive"`
}

// Code - transformation code.
type Code struct {
	CodeKey
	Paths        `json:"-"`
	CodeFileName string   `json:"-"` // eg. "code.sql", "code.py", ...
	Name         string   `json:"name" validate:"required" metaFile:"true"`
	Scripts      []string `json:"script"` // scripts, eg. SQL statements
}

func (b *Branch) ObjectName() string {
	return b.Name
}

func (c *Config) ObjectName() string {
	return c.Name
}

func (r *ConfigRow) ObjectName() string {
	return r.Name
}

func (c *Config) GetComponentId() string {
	return c.ComponentId
}

func (r *ConfigRow) GetComponentId() string {
	return r.ComponentId
}

func (c *Config) GetContent() *orderedmap.OrderedMap {
	return c.Content
}

func (r *ConfigRow) GetContent() *orderedmap.OrderedMap {
	return r.Content
}

func (c *Config) ToApiValues() (map[string]string, error) {
	configJson, err := json.EncodeString(c.Content, false)
	if err != nil {
		return nil, utils.PrefixError(`cannot JSON encode config configuration`, err)
	}

	return map[string]string{
		"name":              c.Name,
		"description":       c.Description,
		"changeDescription": c.ChangeDescription,
		"configuration":     configJson,
	}, nil
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

func (k Kind) String() string {
	return k.Name
}

func (k Kind) IsBranch() bool {
	return k.Name == BranchKind
}

func (k Kind) IsComponent() bool {
	return k.Name == ComponentKind
}

func (k Kind) IsConfig() bool {
	return k.Name == ConfigKind
}

func (k Kind) IsConfigRow() bool {
	return k.Name == ConfigRowKind
}

func (k Kind) IsBlock() bool {
	return k.Name == BlockKind
}

func (k Kind) IsCode() bool {
	return k.Name == CodeKind
}
