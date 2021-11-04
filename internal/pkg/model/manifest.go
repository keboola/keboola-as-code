package model

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const (
	SortById   = "id"
	SortByPath = "path"
)

type RecordPaths interface {
	Path() string          // parent path + object path -> path relative to the project dir
	GetObjectPath() string // path relative to the parent object
	IsParentPathSet() bool
	GetParentPath() string // parent path relative to the project dir
}

// Record - manifest record.
type Record interface {
	Key
	RecordPaths
	Key() Key                   // unique key for map -> for fast access
	SortKey(sort string) string // unique key for sorting
	SetObjectPath(string)       // set path relative to the parent object
	IsParentPathSet() bool      // is parent path resolved?
	SetParentPath(string)       // set parent path
	GetRelatedPaths() []string  // files related to the record, relative to the project dir, e.g. main/meta.json
	AddRelatedPath(path string)
	State() *RecordState
	NewEmptyObject() Object
	NewObjectState() ObjectState
}

type ObjectManifestWithRelations interface {
	Record
	GetRelations() Relations
	SetRelations(relations Relations)
	AddRelation(relation Relation)
}

type RecordState struct {
	Invalid       bool // object files are not valid, eg. missing file, invalid JSON, ...
	NotFound      bool // object directory is not present in the filesystem
	Persisted     bool // record will be part of the manifest when saved
	Deleted       bool // record has been deleted in this command run
	ParentChanged bool // record parent has been changed, files must be moved to new destination
}

type PathInProject struct {
	ObjectPath    string `json:"path" validate:"required"`
	parentPath    string
	parentPathSet bool
}

type Paths struct {
	PathInProject
	RelatedPaths []string `json:"-"` // not serialized, slice is generated when the object is loaded
}

type Project struct {
	Id      int    `json:"id" validate:"required"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type BranchManifest struct {
	RecordState `json:"-"`
	BranchKey
	Paths
}

type ConfigManifest struct {
	RecordState `json:"-"`
	ConfigKey
	Paths
	Relations Relations `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables definition
}

type ConfigRowManifest struct {
	RecordState `json:"-"`
	ConfigRowKey
	Paths
	Relations Relations `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables values definition
}

type ConfigManifestWithRows struct {
	*ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

func NewPathInProject(parentPath, objectPath string) PathInProject {
	return PathInProject{parentPath: parentPath, parentPathSet: true, ObjectPath: objectPath}
}

func (p *PathInProject) GetObjectPath() string {
	return p.ObjectPath
}

func (p *PathInProject) SetObjectPath(path string) {
	p.ObjectPath = path
}

func (p *PathInProject) GetParentPath() string {
	return p.parentPath
}

func (p *PathInProject) IsParentPathSet() bool {
	return p.parentPathSet
}

func (p *PathInProject) SetParentPath(parentPath string) {
	p.parentPathSet = true
	p.parentPath = parentPath
}

func (p PathInProject) Path() string {
	return filesystem.Join(p.parentPath, p.ObjectPath)
}

func (p *Paths) GetRelatedPaths() []string {
	dir := p.Path()
	out := make([]string, 0)
	for _, path := range p.RelatedPaths {
		// Prefix by dir -> path will be relative to the project dir
		out = append(out, filesystem.Join(dir, path))
	}
	return out
}

func (p *Paths) AddRelatedPath(path string) {
	dir := p.Path()
	if !filesystem.IsFrom(path, dir) {
		panic(fmt.Errorf(`path "%s" is not from the dir "%s"`, path, dir))
	}

	relPath, err := filesystem.Rel(dir, path)
	if err != nil {
		panic(err)
	}

	p.RelatedPaths = append(p.RelatedPaths, relPath)
}

func (p *Paths) AbsolutePath(projectDir string) string {
	return filesystem.Join(projectDir, p.Path())
}

func (s *RecordState) State() *RecordState {
	return s
}

func (s *RecordState) IsNotFound() bool {
	return s.NotFound
}

func (s *RecordState) SetNotFound() {
	s.NotFound = true
}

func (s *RecordState) IsInvalid() bool {
	return s.Invalid
}

func (s *RecordState) SetInvalid() {
	s.Invalid = true
}

func (s *RecordState) IsPersisted() bool {
	return s.Persisted
}

func (s *RecordState) SetPersisted() {
	s.Invalid = false
	s.Persisted = true
}

func (s *RecordState) IsDeleted() bool {
	return s.Deleted
}

func (s *RecordState) SetDeleted() {
	s.Deleted = true
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

func (b *BranchManifest) NewObjectState() ObjectState {
	return &BranchState{BranchManifest: b}
}

func (c *ConfigManifest) NewObjectState() ObjectState {
	return &ConfigState{ConfigManifest: c}
}

func (r *ConfigRowManifest) NewObjectState() ObjectState {
	return &ConfigRowState{ConfigRowManifest: r}
}

func (b BranchManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_branch_%s", b.Level(), b.Path())
	} else {
		return b.BranchKey.String()
	}
}

func (c ConfigManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_config_%s", c.Level(), c.Path())
	} else {
		return c.ConfigKey.String()
	}
}

func (r ConfigRowManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_row_%s", r.Level(), r.Path())
	} else {
		return r.ConfigRowKey.String()
	}
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

func (r *ConfigRowManifest) AddRelation(relation Relation) {
	r.Relations.Add(relation)
}
