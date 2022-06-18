package model

import (
	"fmt"
	"sort"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
)

const (
	MetaFileFieldsTag                 = "metaFile:true"        // marks meta fields in object struct
	ConfigFileFieldTag                = "configFile:true"      // marks config field in object struct
	DescriptionFileFieldTag           = "descriptionFile:true" // marks description field in object struct
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
	ProjectDescriptionMetaKey         = `KBC.projectDescription`
	templatesInstancesMetaKey         = `KBC.KAC.templates.instances`
	configIdMetadataKey               = "KBC.KAC.templates.configId"
	rowsIdsMetadataKey                = "KBC.KAC.templates.rowsIds"
	repositoryMetadataKey             = "KBC.KAC.templates.repository"
	templateIdMetadataKey             = "KBC.KAC.templates.templateId"
	instanceIdMetadataKey             = "KBC.KAC.templates.instanceId" // attach config to a template instance
	configInputsUsageMetadataKey      = "KBC.KAC.templates.configInputs"
	rowsInputsUsageMetadataKey        = "KBC.KAC.templates.rowsInputs"
)

type Object interface {
	Key
	Key() Key
	ObjectName() string
	SetObjectId(any)
}

type ToApiObject interface {
	ToApiObject(changeDescription string, changedFields ChangedFields) (apiObject storageapi.Object, apiChangedFields []string)
}

type ToApiObjectKey interface {
	ToApiObjectKey() any
}

type ToApiMetadata interface {
	ToApiObjectKey
	ToApiMetadata() storageapi.Metadata
}

type ObjectWithContent interface {
	Object
	GetComponentId() storageapi.ComponentID
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
	Components() ComponentsMap
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

type BranchMetadata map[string]string

type TemplateInstance struct {
	InstanceId     string              `json:"instanceId"`
	InstanceName   string              `json:"instanceName"`
	TemplateId     string              `json:"templateId"`
	RepositoryName string              `json:"repositoryName"`
	Version        string              `json:"version"`
	Created        ChangedByRecord     `json:"created"`
	Updated        ChangedByRecord     `json:"updated"`
	MainConfig     *TemplateMainConfig `json:"mainConfig,omitempty"`
}
type TemplatesInstances []TemplateInstance

type ChangedByRecord struct {
	Date    time.Time `json:"date"`
	TokenId string    `json:"tokenId"`
}

type TemplateMainConfig struct {
	ConfigId    storageapi.ConfigID    `json:"configId"`
	ComponentId storageapi.ComponentID `json:"componentId"`
}

// NewBranch creates branch model from API values.
func NewBranch(apiValue *storageapi.Branch) *Branch {
	out := &Branch{}
	out.Id = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDefault = apiValue.IsDefault
	return out
}

// NewConfig creates config model from API values.
func NewConfig(apiValue *storageapi.Config) *Config {
	out := &Config{}
	out.BranchId = apiValue.BranchID
	out.ComponentId = apiValue.ComponentID
	out.Id = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDisabled = apiValue.IsDisabled
	out.Content = apiValue.Content
	return out
}

// NewConfigRow creates config row model from API values.
func NewConfigRow(apiValue *storageapi.ConfigRow) *ConfigRow {
	out := &ConfigRow{}
	out.BranchId = apiValue.BranchID
	out.ComponentId = apiValue.ComponentID
	out.ConfigId = apiValue.ConfigID
	out.Id = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDisabled = apiValue.IsDisabled
	out.Content = apiValue.Content
	return out
}

// ToApiObject ...
func (b *Branch) ToApiObject(_ string, changedFields ChangedFields) (storageapi.Object, []string) {
	out := &storageapi.Branch{}
	out.ID = b.Id
	out.Name = b.Name
	out.Description = b.Description
	out.IsDefault = b.IsDefault
	return out, changedFields.Slice()
}

// ToApiObject ...
func (c *Config) ToApiObject(changeDescription string, changedFields ChangedFields) (storageapi.Object, []string) {
	out := &storageapi.Config{}
	out.ChangeDescription = changeDescription
	out.BranchID = c.BranchId
	out.ComponentID = c.ComponentId
	out.ID = c.Id
	out.Name = c.Name
	out.Description = c.Description
	out.IsDisabled = c.IsDisabled
	out.Content = c.Content
	return out, append(changedFields.Slice(), "changeDescription")
}

// ToApiObject ...
func (r *ConfigRow) ToApiObject(changeDescription string, changedFields ChangedFields) (storageapi.Object, []string) {
	out := &storageapi.ConfigRow{}
	out.ChangeDescription = changeDescription
	out.BranchID = r.BranchId
	out.ComponentID = r.ComponentId
	out.ID = r.Id
	out.Name = r.Name
	out.Description = r.Description
	out.IsDisabled = r.IsDisabled
	out.Content = r.Content
	return out, append(changedFields.Slice(), "changeDescription")
}

// ToApiObjectKey ...
func (b *Branch) ToApiObjectKey() any {
	return storageapi.BranchKey{ID: b.Id}
}

// ToApiObjectKey ...
func (c *Config) ToApiObjectKey() any {
	return storageapi.ConfigKey{BranchID: c.BranchId, ComponentID: c.ComponentId, ID: c.Id}
}

// ToApiObjectKey ...
func (r *ConfigRow) ToApiObjectKey() any {
	return storageapi.ConfigRowKey{BranchID: r.BranchId, ComponentID: r.ComponentId, ConfigID: r.ConfigId, ID: r.Id}
}

// ToApiMetadata ...
func (b *Branch) ToApiMetadata() storageapi.Metadata {
	return storageapi.Metadata(b.Metadata)
}

// ToApiMetadata ...
func (c *Config) ToApiMetadata() storageapi.Metadata {
	return storageapi.Metadata(c.Metadata)
}

func (m BranchMetadata) saveTemplateUsages(instances TemplatesInstances) error {
	sort.SliceStable(instances, func(i, j int) bool {
		return instances[i].InstanceId < instances[j].InstanceId
	})
	encoded, err := json.EncodeString(instances, false)
	if err != nil {
		return fmt.Errorf(`metadata "%s" are not in valid format: %w`, templatesInstancesMetaKey, err)
	}
	m[templatesInstancesMetaKey] = encoded
	return nil
}

func (m BranchMetadata) UpsertTemplateInstanceFrom(now time.Time, tokenId string, d TemplateInstance) error {
	return m.UpsertTemplateInstance(now, d.InstanceId, d.InstanceName, d.TemplateId, d.RepositoryName, d.Version, tokenId, d.MainConfig)
}

// UpsertTemplateInstance (update or insert) on use or upgrade operation.
func (m BranchMetadata) UpsertTemplateInstance(now time.Time, instanceId, instanceName, templateId, repositoryName, version, tokenId string, mainConfig *TemplateMainConfig) error {
	now = now.Truncate(time.Second).UTC()
	instance := TemplateInstance{
		InstanceId:     instanceId,
		TemplateId:     templateId,
		RepositoryName: repositoryName,
		Version:        version,
		Created:        ChangedByRecord{Date: now, TokenId: tokenId},
		Updated:        ChangedByRecord{Date: now, TokenId: tokenId},
	}

	// Load instance, if exists
	instancesOld, _ := m.TemplatesInstances() // on error -> empty slice
	instances := make(TemplatesInstances, 0)
	for _, item := range instancesOld {
		if item.InstanceId != instanceId {
			// Pass through other instances
			instances = append(instances, item)
		} else {
			// Skip existing instance, get value
			instance = item
		}
	}

	// Set/update version, updated date, main config
	instance.Version = version
	instance.InstanceName = instanceName
	instance.Updated = ChangedByRecord{Date: now, TokenId: tokenId}
	instance.MainConfig = mainConfig

	// Store instance
	instances = append(instances, instance)
	return m.saveTemplateUsages(instances)
}

func (m BranchMetadata) DeleteTemplateUsage(instanceId string) error {
	instances, err := m.TemplatesInstances()
	if err != nil {
		return err
	}

	for i, u := range instances {
		if u.InstanceId == instanceId {
			instances = append(instances[:i], instances[i+1:]...)
			return m.saveTemplateUsages(instances)
		}
	}

	return fmt.Errorf(`instance "%s" not found`, instanceId)
}

func (m BranchMetadata) TemplatesInstances() (TemplatesInstances, error) {
	instances := &TemplatesInstances{}
	instancesEncoded, found := m[templatesInstancesMetaKey]
	if !found {
		return *instances, nil
	}
	err := json.DecodeString(instancesEncoded, instances)
	if err != nil {
		return nil, fmt.Errorf(`metadata "%s" are not in valid format: %w`, templatesInstancesMetaKey, err)
	}
	return *instances, nil
}

func (m BranchMetadata) TemplateInstance(instance string) (*TemplateInstance, bool, error) {
	usages, err := m.TemplatesInstances()
	if err != nil {
		return &TemplateInstance{}, false, err
	}
	for _, usage := range usages {
		if usage.InstanceId == instance {
			return &usage, true, nil
		}
	}
	return &TemplateInstance{}, false, nil
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

// ConfigMetadata stores config template metadata to config metadata.
type ConfigMetadata map[string]string

type ConfigInputUsage struct {
	Input   string `json:"input"`
	JsonKey string `json:"key"`
}

type RowInputUsage struct {
	RowId   storageapi.RowID `json:"rowId"`
	Input   string           `json:"input"`
	JsonKey string           `json:"key"`
}

func (m ConfigMetadata) SetConfigTemplateId(templateObjectId storageapi.ConfigID) {
	m[configIdMetadataKey] = json.MustEncodeString(ConfigIdMetadata{
		IdInTemplate: templateObjectId,
	}, false)
}

func (m ConfigMetadata) ConfigTemplateId() *ConfigIdMetadata {
	out := ConfigIdMetadata{}
	_ = json.DecodeString(m[configIdMetadataKey], &out) // ignore empty string or other errors
	if len(out.IdInTemplate) == 0 {
		return nil
	}
	return &out
}

func (m ConfigMetadata) InputsUsage() []ConfigInputUsage {
	var out []ConfigInputUsage
	_ = json.DecodeString(m[configInputsUsageMetadataKey], &out) // ignore empty string or other errors
	return out
}

func (m ConfigMetadata) AddInputUsage(inputName string, jsonKey orderedmap.Path) {
	m[configInputsUsageMetadataKey] = json.MustEncodeString(append(m.InputsUsage(), ConfigInputUsage{
		Input:   inputName,
		JsonKey: jsonKey.String(),
	}), false)
}

func (m ConfigMetadata) AddRowTemplateId(projectObjectId, templateObjectId storageapi.RowID) {
	items := append(m.RowsTemplateIds(), RowIdMetadata{
		IdInProject:  projectObjectId,
		IdInTemplate: templateObjectId,
	})
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].IdInTemplate < items[j].IdInTemplate
	})
	m[rowsIdsMetadataKey] = json.MustEncodeString(items, false)
}

func (m ConfigMetadata) RowsTemplateIds() (out []RowIdMetadata) {
	_ = json.DecodeString(m[rowsIdsMetadataKey], &out) // ignore empty string or other errors
	return out
}

func (m ConfigMetadata) RowsInputsUsage() []RowInputUsage {
	var out []RowInputUsage
	_ = json.DecodeString(m[rowsInputsUsageMetadataKey], &out) // ignore empty string or other errors
	return out
}

func (m ConfigMetadata) AddRowInputUsage(rowId storageapi.RowID, inputName string, jsonKey orderedmap.Path) {
	values := append(m.RowsInputsUsage(), RowInputUsage{
		RowId:   rowId,
		Input:   inputName,
		JsonKey: jsonKey.String(),
	})
	sort.SliceStable(values, func(i, j int) bool {
		return values[i].Input < values[j].Input
	})
	m[rowsInputsUsageMetadataKey] = json.MustEncodeString(values, false)
}

func (m ConfigMetadata) SetTemplateInstance(repo string, tmpl string, inst string) {
	m[repositoryMetadataKey] = repo
	m[templateIdMetadataKey] = tmpl
	m[instanceIdMetadataKey] = inst
}

func (m ConfigMetadata) Repository() string {
	return m[repositoryMetadataKey]
}

func (m ConfigMetadata) TemplateId() string {
	return m[templateIdMetadataKey]
}

func (m ConfigMetadata) InstanceId() string {
	return m[instanceIdMetadataKey]
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name           string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description    string                 `json:"description" diff:"true" descriptionFile:"true"`
	IsDisabled     bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content        *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	Transformation *Transformation        `json:"-" validate:"omitempty,dive" diff:"true"`
	SharedCode     *SharedCodeConfig      `json:"-" validate:"omitempty,dive" diff:"true"`
	Orchestration  *Orchestration         `json:"-" validate:"omitempty,dive" diff:"true"`
	Relations      Relations              `json:"-" validate:"dive" diff:"true"`
	Metadata       ConfigMetadata         `json:"-" validate:"dive" diff:"true"`
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
	Name        string                 `json:"name" diff:"true" metaFile:"true"`
	Description string                 `json:"description" diff:"true" descriptionFile:"true"`
	IsDisabled  bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content     *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	SharedCode  *SharedCodeRow         `json:"-" validate:"omitempty,dive" diff:"true"`
	Relations   Relations              `json:"-" validate:"dive" diff:"true"`
}

// Job - Storage API job.
type Job struct {
	Id      int                    `json:"id" validate:"required"`
	Status  string                 `json:"status" validate:"required"`
	Url     string                 `json:"url" validate:"required"`
	Results map[string]interface{} `json:"results"`
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

func (b *Branch) SetObjectId(id any) {
	b.Id = id.(storageapi.BranchID)
}

func (c *Config) SetObjectId(id any) {
	c.Id = id.(storageapi.ConfigID)
}

func (r *ConfigRow) SetObjectId(id any) {
	r.Id = id.(storageapi.RowID)
}

func (c *Config) GetComponentId() storageapi.ComponentID {
	return c.ComponentId
}

func (r *ConfigRow) GetComponentId() storageapi.ComponentID {
	return r.ComponentId
}

func (c *Config) GetContent() *orderedmap.OrderedMap {
	return c.Content
}

func (r *ConfigRow) GetContent() *orderedmap.OrderedMap {
	return r.Content
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
