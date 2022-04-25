package model

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const (
	MetaFileFieldsTag                 = "metaFile:true"        // marks meta fields in object struct
	ConfigFileFieldTag                = "configFile:true"      // marks config field in object struct
	DescriptionFileFieldTag           = "descriptionFile:true" // marks description field in object struct
	TransformationType                = "transformation"
	SharedCodeComponentId             = ComponentId("keboola.shared-code")
	OrchestratorComponentId           = ComponentId("keboola.orchestrator")
	ShareCodeTargetComponentKey       = `componentId`
	SharedCodeContentKey              = `code_content`
	VariablesIdContentKey             = `variables_id`
	VariablesValuesIdContentKey       = `variables_values_id`
	SharedCodeVariablesIdContentKey   = `variables_id`
	SharedCodeIdContentKey            = `shared_code_id`
	SharedCodeRowsIdContentKey        = `shared_code_row_ids`
	SharedCodePathContentKey          = `shared_code_path`
	SchedulerTargetKey                = `target`
	SchedulerTargetComponentIdKey     = `componentId`
	SchedulerTargetConfigurationIdKey = `configurationId`
	OrchestratorPhasesContentKey      = `phases`
	OrchestratorTasksContentKey       = `tasks`
	TemplatesInstancesMetaKey         = `KBC.KAC.templates.instances`
)

type Object interface {
	Key
	Key() Key
	ObjectName() string
}

type ObjectWithContent interface {
	Object
	GetComponentId() ComponentId
	GetContent() *orderedmap.OrderedMap
}

type ObjectWithRelations interface {
	Object
	GetRelations() Relations
	SetRelations(relations Relations)
	AddRelation(relation Relation)
}

type ObjectStates interface {
	ObjectsInState(stateType StateType) Objects
	RemoteObjects() Objects
	LocalObjects() Objects
	All() []ObjectState
	Components() *ComponentsMap
	Branches() (branches []*BranchState)
	Configs() []*ConfigState
	ConfigsFrom(branch BranchKey) (configs []*ConfigState)
	ConfigRows() []*ConfigRowState
	ConfigRowsFrom(config ConfigKey) (rows []*ConfigRowState)
	Get(key Key) (ObjectState, bool)
	GetOrNil(key Key) ObjectState
	MustGet(key Key) ObjectState
	CreateFrom(manifest ObjectManifest) (ObjectState, error)
	GetOrCreateFrom(manifest ObjectManifest) (ObjectState, error)
	Set(objectState ObjectState) error
	TrackedPaths() []string
	ReloadPathsState() error
	IsFile(path string) bool
	IsDir(path string) bool
}

type Objects interface {
	Get(key Key) (Object, bool)
	All() []Object
	Branches() (branches []*Branch)
	ConfigsFrom(branch BranchKey) (configs []*Config)
	ConfigsWithRowsFrom(branch BranchKey) (configs []*ConfigWithRows)
	ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow)
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

type BranchMetadata map[string]string

type TemplateUsageRecord struct {
	InstanceId string `json:"instanceId"`
	TemplateId string `json:"templateId"`
	Version    string `json:"version"`
}
type TemplateUsageRecords []TemplateUsageRecord

func (m BranchMetadata) AddTemplateUsage(instanceId string, templateId string, version string) error {
	r := TemplateUsageRecord{
		InstanceId: instanceId,
		TemplateId: templateId,
		Version:    version,
	}
	instances, err := m.TemplatesUsages()
	if err != nil {
		return err
	}
	instances = append(instances, r)
	encoded, err := json.EncodeString(instances, false)
	if err != nil {
		return fmt.Errorf(`metadata "%s" are not in valid format: %w`, TemplatesInstancesMetaKey, err)
	}
	m[TemplatesInstancesMetaKey] = encoded
	return nil
}

func (m BranchMetadata) TemplatesUsages() (TemplateUsageRecords, error) {
	instances := &TemplateUsageRecords{}
	instancesEncoded, found := m[TemplatesInstancesMetaKey]
	if !found {
		return *instances, nil
	}
	err := json.DecodeString(instancesEncoded, instances)
	if err != nil {
		return nil, fmt.Errorf(`metadata "%s" are not in valid format: %w`, TemplatesInstancesMetaKey, err)
	}
	return *instances, nil
}

func (m BranchMetadata) ToOrderedMap() *orderedmap.OrderedMap {
	res := orderedmap.New()
	for k, v := range m {
		res.Set(k, v)
	}
	return res
}

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	BranchKey
	Name        string         `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description string         `json:"description" diff:"true" descriptionFile:"true"`
	IsDefault   bool           `json:"isDefault" diff:"true" metaFile:"true"`
	Metadata    BranchMetadata `json:"-" validate:"dive" diff:"true"`
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name              string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description       string                 `json:"description" diff:"true" descriptionFile:"true"`
	ChangeDescription string                 `json:"changeDescription"`
	Content           *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	Transformation    *Transformation        `json:"-" validate:"omitempty,dive" diff:"true"`
	SharedCode        *SharedCodeConfig      `json:"-" validate:"omitempty,dive" diff:"true"`
	Orchestration     *Orchestration         `json:"-" validate:"omitempty,dive" diff:"true"`
	Relations         Relations              `json:"-" validate:"dive" diff:"true"`
	Metadata          map[string]string      `json:"-" validate:"dive" diff:"true"`
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
	SharedCode        *SharedCodeRow         `json:"-" validate:"omitempty,dive" diff:"true"`
	Relations         Relations              `json:"-" validate:"dive" diff:"true"`
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

// Schedule - https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
type Schedule struct {
	Id       string   `json:"id" validate:"required"`
	ConfigId ConfigId `json:"configurationId" validate:"required"`
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

func (c *Config) GetComponentId() ComponentId {
	return c.ComponentId
}

func (r *ConfigRow) GetComponentId() ComponentId {
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

func (k Kind) String() string {
	return k.Name
}

func (k Kind) IsEmpty() bool {
	return k.Name == "" && k.Abbr == ""
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

func (c *Config) GetRelations() Relations {
	return c.Relations
}

func (r *ConfigRow) GetRelations() Relations {
	return r.Relations
}

func (c *Config) SetRelations(relations Relations) {
	c.Relations = relations
}

func (r *ConfigRow) SetRelations(relations Relations) {
	r.Relations = relations
}

func (c *Config) AddRelation(relation Relation) {
	c.Relations.Add(relation)
}

func (r *ConfigRow) AddRelation(relation Relation) {
	r.Relations.Add(relation)
}

func (c *Config) MetadataOrderedMap() *orderedmap.OrderedMap {
	ordered := orderedmap.New()
	for key, val := range c.Metadata {
		ordered.Set(key, val)
	}
	ordered.SortKeys(sort.Strings)
	return ordered
}

func (b *Branch) MetadataOrderedMap() *orderedmap.OrderedMap {
	ordered := orderedmap.New()
	for key, val := range b.Metadata {
		ordered.Set(key, val)
	}
	ordered.SortKeys(sort.Strings)
	return ordered
}
