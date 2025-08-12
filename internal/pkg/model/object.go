package model

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	MetaFileFieldsTag                 = "metaFile:true"        // marks meta fields in object struct
	ConfigFileFieldTag                = "configFile:true"      // marks config field in object struct
	DescriptionFileFieldTag           = "descriptionFile:true" // marks description field in object struct
	ShareCodeTargetComponentKey       = `componentId`
	SharedCodeContentKey              = `code_content`
	VariablesIDContentKey             = `variables_id`
	VariablesValuesIDContentKey       = `variables_values_id`
	SharedCodeVariablesIDContentKey   = `variables_id`
	SharedCodeIDContentKey            = `shared_code_id`
	SharedCodeRowsIDContentKey        = `shared_code_row_ids`
	SharedCodePathContentKey          = `shared_code_path`
	SchedulerTargetKey                = `target`
	SchedulerTargetComponentIDKey     = `componentId`
	SchedulerTargetConfigurationIDKey = `configurationId`
	OrchestratorPhasesContentKey      = `phases`
	OrchestratorTasksContentKey       = `tasks`
	ProjectDescriptionMetaKey         = `KBC.projectDescription`
	templatesInstancesMetaKey         = `KBC.KAC.templates.instances`
	configIDMetadataKey               = "KBC.KAC.templates.configId"
	rowsIdsMetadataKey                = "KBC.KAC.templates.rowsIds"
	repositoryMetadataKey             = "KBC.KAC.templates.repository"
	templateIDMetadataKey             = "KBC.KAC.templates.templateId"
	instanceIDMetadataKey             = "KBC.KAC.templates.instanceId" // attach config to a template instance
	configInputsUsageMetadataKey      = "KBC.KAC.templates.configInputs"
	rowsInputsUsageMetadataKey        = "KBC.KAC.templates.rowsInputs"
)

type Object interface {
	Key
	Key() Key
	ObjectName() string
	SetObjectID(objectID any)
}

type ToAPIObject interface {
	ToAPIObject(changeDescription string, changedFields ChangedFields) (apiObject keboola.Object, apiChangedFields []string)
}

type ToAPIObjectKey interface {
	ToAPIObjectKey() any
}

type ToAPIMetadata interface {
	ToAPIObjectKey
	ToAPIMetadata() keboola.Metadata
}

type ObjectWithContent interface {
	Object
	GetComponentID() keboola.ComponentID
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
	ReloadPathsState(ctx context.Context) error
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
	InstanceID     string              `json:"instanceId"`
	InstanceName   string              `json:"instanceName"`
	TemplateID     string              `json:"templateId"`
	RepositoryName string              `json:"repositoryName"`
	Version        string              `json:"version"`
	Created        ChangedByRecord     `json:"created"`
	Updated        ChangedByRecord     `json:"updated"`
	MainConfig     *TemplateMainConfig `json:"mainConfig,omitempty"`
}
type TemplatesInstances []TemplateInstance

type ChangedByRecord struct {
	Date    time.Time `json:"date"`
	TokenID string    `json:"tokenId"`
}

type TemplateMainConfig struct {
	ConfigID    keboola.ConfigID    `json:"configId"`
	ComponentID keboola.ComponentID `json:"componentId"`
}

// NewBranch creates branch model from API values.
func NewBranch(apiValue *keboola.Branch) *Branch {
	out := &Branch{}
	out.ID = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDefault = apiValue.IsDefault
	return out
}

// NewConfig creates config model from API values.
func NewConfig(apiValue *keboola.Config) *Config {
	out := &Config{}
	out.BranchID = apiValue.BranchID
	out.ComponentID = apiValue.ComponentID
	out.ID = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDisabled = apiValue.IsDisabled
	out.Content = apiValue.Content
	return out
}

// NewConfigWithRows creates config model from API values.
func NewConfigWithRows(apiValue *keboola.ConfigWithRows) *ConfigWithRows {
	out := &ConfigWithRows{Config: &Config{}}
	out.BranchID = apiValue.BranchID
	out.ComponentID = apiValue.ComponentID
	out.ID = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDisabled = apiValue.IsDisabled
	out.Content = apiValue.Content
	for _, apiRow := range apiValue.Rows {
		out.Rows = append(out.Rows, NewConfigRow(apiRow))
	}
	return out
}

// NewConfigRow creates config row model from API values.
func NewConfigRow(apiValue *keboola.ConfigRow) *ConfigRow {
	out := &ConfigRow{}
	out.BranchID = apiValue.BranchID
	out.ComponentID = apiValue.ComponentID
	out.ConfigID = apiValue.ConfigID
	out.ID = apiValue.ID
	out.Name = apiValue.Name
	out.Description = apiValue.Description
	out.IsDisabled = apiValue.IsDisabled
	out.Content = apiValue.Content
	return out
}

// ToAPIObject ...
func (b *Branch) ToAPIObject(_ string, changedFields ChangedFields) (keboola.Object, []string) {
	out := &keboola.Branch{}
	out.ID = b.ID
	out.Name = b.Name
	out.Description = b.Description
	out.IsDefault = b.IsDefault
	return out, changedFields.Slice()
}

// ToAPIObject ...
func (c *Config) ToAPIObject(changeDescription string, changedFields ChangedFields) (keboola.Object, []string) {
	out := &keboola.Config{}
	out.ChangeDescription = changeDescription
	out.BranchID = c.BranchID
	out.ComponentID = c.ComponentID
	out.ID = c.ID
	out.Name = c.Name
	out.Description = c.Description
	out.IsDisabled = c.IsDisabled
	out.Content = c.Content
	return out, append(changedFields.Slice(), "changeDescription")
}

// ToAPIObject ...
func (r *ConfigRow) ToAPIObject(changeDescription string, changedFields ChangedFields) (keboola.Object, []string) {
	out := &keboola.ConfigRow{}
	out.ChangeDescription = changeDescription
	out.BranchID = r.BranchID
	out.ComponentID = r.ComponentID
	out.ConfigID = r.ConfigID
	out.ID = r.ID
	out.Name = r.Name
	out.Description = r.Description
	out.IsDisabled = r.IsDisabled
	out.Content = r.Content
	return out, append(changedFields.Slice(), "changeDescription")
}

// ToAPIObjectKey ...
func (b *Branch) ToAPIObjectKey() any {
	return keboola.BranchKey{ID: b.ID}
}

// ToAPIObjectKey ...
func (c *Config) ToAPIObjectKey() any {
	return keboola.ConfigKey{BranchID: c.BranchID, ComponentID: c.ComponentID, ID: c.ID}
}

// ToAPIObjectKey ...
func (r *ConfigRow) ToAPIObjectKey() any {
	return keboola.ConfigRowKey{BranchID: r.BranchID, ComponentID: r.ComponentID, ConfigID: r.ConfigID, ID: r.ID}
}

// ToAPIMetadata ...
func (b *Branch) ToAPIMetadata() keboola.Metadata {
	return keboola.Metadata(b.Metadata)
}

// ToAPIMetadata ...
func (c *Config) ToAPIMetadata() keboola.Metadata {
	return keboola.Metadata(c.Metadata)
}

func (m BranchMetadata) saveTemplateUsages(instances TemplatesInstances) error {
	sort.SliceStable(instances, func(i, j int) bool {
		if v := strings.Compare(instances[i].RepositoryName, instances[j].RepositoryName); v != 0 {
			return v == -1
		}
		if v := strings.Compare(instances[i].InstanceName, instances[j].InstanceName); v != 0 {
			return v == -1
		}
		return instances[i].InstanceID < instances[j].InstanceID
	})
	encoded, err := json.EncodeString(instances, false)
	if err != nil {
		return errors.Errorf(`metadata "%s" are not in valid format: %w`, templatesInstancesMetaKey, err)
	}
	m[templatesInstancesMetaKey] = encoded
	return nil
}

func (m BranchMetadata) UpsertTemplateInstanceFrom(now time.Time, tokenID string, d TemplateInstance) error {
	return m.UpsertTemplateInstance(now, d.InstanceID, d.InstanceName, d.TemplateID, d.RepositoryName, d.Version, tokenID, d.MainConfig)
}

// UpsertTemplateInstance (update or insert) on use or upgrade operation.
func (m BranchMetadata) UpsertTemplateInstance(now time.Time, instanceID, instanceName, templateID, repositoryName, version, tokenID string, mainConfig *TemplateMainConfig) error {
	now = now.Truncate(time.Second).UTC()
	instance := TemplateInstance{
		InstanceID:     instanceID,
		TemplateID:     templateID,
		RepositoryName: repositoryName,
		Version:        version,
		Created:        ChangedByRecord{Date: now, TokenID: tokenID},
		Updated:        ChangedByRecord{Date: now, TokenID: tokenID},
	}

	// Load instance, if exists
	instancesOld, _ := m.TemplatesInstances() // on error -> empty slice
	instances := make(TemplatesInstances, 0)
	for _, item := range instancesOld {
		if item.InstanceID != instanceID {
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
	instance.Updated = ChangedByRecord{Date: now, TokenID: tokenID}
	instance.MainConfig = mainConfig

	// Store instance
	instances = append(instances, instance)
	return m.saveTemplateUsages(instances)
}

func (m BranchMetadata) DeleteTemplateUsage(instanceID string) error {
	instances, err := m.TemplatesInstances()
	if err != nil {
		return err
	}

	for i, u := range instances {
		if u.InstanceID == instanceID {
			instances = slices.Delete(instances, i, i+1)
			return m.saveTemplateUsages(instances)
		}
	}

	return errors.Errorf(`instance "%s" not found`, instanceID)
}

func (m BranchMetadata) TemplatesInstances() (TemplatesInstances, error) {
	instances := &TemplatesInstances{}
	instancesEncoded, found := m[templatesInstancesMetaKey]
	if !found {
		return *instances, nil
	}
	err := json.DecodeString(instancesEncoded, instances)
	if err != nil {
		return nil, errors.Errorf(`metadata "%s" are not in valid format: %w`, templatesInstancesMetaKey, err)
	}
	return *instances, nil
}

func (m BranchMetadata) TemplateInstance(instance string) (*TemplateInstance, bool, error) {
	usages, err := m.TemplatesInstances()
	if err != nil {
		return &TemplateInstance{}, false, err
	}
	for _, usage := range usages {
		if usage.InstanceID == instance {
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
	Input      string   `json:"input"`
	JSONKey    string   `json:"key"`
	ObjectKeys []string `json:"objectKeys,omitempty"` // list of object keys generated from the input (empty = all)
}

type RowInputUsage struct {
	RowID      keboola.RowID `json:"rowId"`
	Input      string        `json:"input"`
	JSONKey    string        `json:"key"`
	ObjectKeys []string      `json:"objectKeys,omitempty"` // list of object keys generated from the input (empty = all)
}

func (m ConfigMetadata) SetConfigTemplateID(templateObjectID keboola.ConfigID) {
	m[configIDMetadataKey] = json.MustEncodeString(ConfigIDMetadata{
		IDInTemplate: templateObjectID,
	}, false)
}

func (m ConfigMetadata) ConfigTemplateID() *ConfigIDMetadata {
	out := ConfigIDMetadata{}
	_ = json.DecodeString(m[configIDMetadataKey], &out) // ignore empty string or other errors
	if len(out.IDInTemplate) == 0 {
		return nil
	}
	return &out
}

func (m ConfigMetadata) InputsUsage() []ConfigInputUsage {
	var out []ConfigInputUsage
	_ = json.DecodeString(m[configInputsUsageMetadataKey], &out) // ignore empty string or other errors
	return out
}

func (m ConfigMetadata) AddInputUsage(inputName string, jsonKey orderedmap.Path, objectKeys []string) {
	sort.Strings(objectKeys)
	m[configInputsUsageMetadataKey] = json.MustEncodeString(append(m.InputsUsage(), ConfigInputUsage{
		Input:      inputName,
		JSONKey:    jsonKey.String(),
		ObjectKeys: objectKeys,
	}), false)
}

func (m ConfigMetadata) AddRowTemplateID(projectObjectID, templateObjectID keboola.RowID) {
	items := append(m.RowsTemplateIds(), RowIDMetadata{
		IDInProject:  projectObjectID,
		IDInTemplate: templateObjectID,
	})
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].IDInTemplate < items[j].IDInTemplate
	})
	m[rowsIdsMetadataKey] = json.MustEncodeString(items, false)
}

func (m ConfigMetadata) RowsTemplateIds() (out []RowIDMetadata) {
	_ = json.DecodeString(m[rowsIdsMetadataKey], &out) // ignore empty string or other errors
	return out
}

func (m ConfigMetadata) RowsInputsUsage() []RowInputUsage {
	var out []RowInputUsage
	_ = json.DecodeString(m[rowsInputsUsageMetadataKey], &out) // ignore empty string or other errors
	return out
}

func (m ConfigMetadata) AddRowInputUsage(rowID keboola.RowID, inputName string, jsonKey orderedmap.Path, objectKeys []string) {
	sort.Strings(objectKeys)
	values := append(m.RowsInputsUsage(), RowInputUsage{
		RowID:      rowID,
		Input:      inputName,
		JSONKey:    jsonKey.String(),
		ObjectKeys: objectKeys,
	})
	sort.SliceStable(values, func(i, j int) bool {
		return values[i].Input < values[j].Input
	})
	m[rowsInputsUsageMetadataKey] = json.MustEncodeString(values, false)
}

func (m ConfigMetadata) SetTemplateInstance(repo string, tmpl string, inst string) {
	m[repositoryMetadataKey] = repo
	m[templateIDMetadataKey] = tmpl
	if inst != "" {
		m[instanceIDMetadataKey] = inst
	}
}

func (m ConfigMetadata) Repository() string {
	return m[repositoryMetadataKey]
}

func (m ConfigMetadata) TemplateID() string {
	return m[templateIDMetadataKey]
}

func (m ConfigMetadata) InstanceID() string {
	return m[instanceIDMetadataKey]
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	ConfigKey
	Name           string                 `json:"name" validate:"required" diff:"true" metaFile:"true"`
	Description    string                 `json:"description" diff:"true" descriptionFile:"true"`
	IsDisabled     bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content        *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	Relations      Relations              `json:"-" validate:"dive" diff:"true"`
	Transformation *Transformation        `json:"-" diff:"true"`
	SharedCode     *SharedCodeConfig      `json:"-" diff:"true"`
	Orchestration  *Orchestration         `json:"-" diff:"true"`
	Metadata       ConfigMetadata         `json:"-" diff:"true"`
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

// ToAPIObject converts ConfigWithRows to API object including rows
func (c *ConfigWithRows) ToAPIObject(changeDescription string, changedFields ChangedFields) (keboola.Object, []string) {
	out := &keboola.ConfigWithRows{Config: &keboola.Config{}}
	out.ChangeDescription = changeDescription
	out.BranchID = c.BranchID
	out.ComponentID = c.ComponentID
	out.ID = c.ID
	out.Name = c.Name
	out.Description = c.Description
	out.IsDisabled = c.IsDisabled
	out.Content = c.Content

	// Include rows in the API object
	for _, row := range c.Rows {
		apiRow := &keboola.ConfigRow{}
		apiRow.ChangeDescription = changeDescription
		apiRow.BranchID = row.BranchID
		apiRow.ComponentID = row.ComponentID
		apiRow.ConfigID = row.ConfigID
		apiRow.ID = row.ID
		apiRow.Name = row.Name
		apiRow.Description = row.Description
		apiRow.IsDisabled = row.IsDisabled
		apiRow.Content = row.Content
		out.Rows = append(out.Rows, apiRow)
	}

	return out, append(changedFields.Slice(), "changeDescription")
}

// ConfigRow https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type ConfigRow struct {
	ConfigRowKey
	Name        string                 `json:"name" diff:"true" metaFile:"true"`
	Description string                 `json:"description" diff:"true" descriptionFile:"true"`
	IsDisabled  bool                   `json:"isDisabled" diff:"true" metaFile:"true"`
	Content     *orderedmap.OrderedMap `json:"configuration" validate:"required" diff:"true" configFile:"true"`
	Relations   Relations              `json:"-" validate:"dive" diff:"true"`
	SharedCode  *SharedCodeRow         `json:"-" diff:"true"`
}

// Job - Storage API job.
type Job struct {
	ID      int            `json:"id" validate:"required"`
	Status  string         `json:"status" validate:"required"`
	URL     string         `json:"url" validate:"required"`
	Results map[string]any `json:"results"`
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

func (b *Branch) SetObjectID(id any) {
	b.ID = id.(keboola.BranchID)
}

func (c *Config) SetObjectID(id any) {
	c.ID = id.(keboola.ConfigID)
}

func (c *ConfigWithRows) SetObjectID(id any) {
	c.ID = id.(keboola.ConfigID)
	for _, row := range c.Rows {
		row.ConfigID = c.ID
	}
}

func (r *ConfigRow) SetObjectID(id any) {
	r.ID = id.(keboola.RowID)
}

func (c *Config) GetComponentID() keboola.ComponentID {
	return c.ComponentID
}

func (r *ConfigRow) GetComponentID() keboola.ComponentID {
	return r.ComponentID
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
