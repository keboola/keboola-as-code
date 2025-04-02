package model

import (
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	SortByID   = "id"
	SortByPath = "path"
)

type RecordPaths interface {
	GetAbsPath() AbsPath
	// Path gets path relative to the top dir, it is parent path + relative path.
	Path() string
	// GetRelativePath - for example path of the object inside parent object/path.
	GetRelativePath() string
	// SetRelativePath - for example path of the object inside parent object/path.
	SetRelativePath(path string)
	// GetParentPath - for example path of the parent object.
	GetParentPath() string
	// SetParentPath - for example path of the parent object.
	SetParentPath(path string)
	// IsParentPathSet returns true if the parent path is set/resolved.
	IsParentPathSet() bool
}

type RelatedPaths interface {
	// GetRelatedPaths returns files related to the record, relative to the project dir, e.g. main/meta.json
	GetRelatedPaths() []string
	ClearRelatedPaths()
	AddRelatedPath(path string)
	// AddRelatedPathInRoot allows tracking of project-related files from under the default branch until we have direct project representation
	AddRelatedPathInRoot(path string)
	RenameRelatedPaths(oldPath, newPath string)
}

// ObjectManifest - manifest record for a object.
type ObjectManifest interface {
	Key
	RecordPaths
	RelatedPaths
	Key() Key                   // unique key for map -> for fast access
	SortKey(sort string) string // unique key for sorting
	State() *RecordState
	NewEmptyObject() Object
	NewObjectState() ObjectState
}

type ObjectManifestWithRelations interface {
	ObjectManifest
	GetRelations() Relations
	SetRelations(relations Relations)
	AddRelation(relation Relation)
}

type RecordState struct {
	Invalid   bool // object files are not valid, eg. missing file, invalid JSON, ...
	NotFound  bool // object directory is not present in the filesystem
	Persisted bool // record will be part of the manifest when saved
	Deleted   bool // record has been deleted in this command run
}

type BranchManifest struct {
	RecordState `json:"-"`
	BranchKey
	Paths
	Metadata *orderedmap.OrderedMap `json:"metadata,omitempty"`
}

type ConfigManifest struct {
	RecordState `json:"-"`
	ConfigKey
	Paths
	Relations Relations              `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables definition
	Metadata  *orderedmap.OrderedMap `json:"metadata,omitempty"`
}

type ConfigRowManifest struct {
	RecordState `json:"-"`
	ConfigRowKey
	Paths
	Relations Relations `json:"relations,omitempty" validate:"dive"` // relations with other objects, for example variables values definition
}

type ConfigManifestWithRows struct {
	ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

func (p *Paths) ClearRelatedPaths() {
	p.RelatedPaths = make([]string, 0)
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
		panic(errors.Errorf(`path "%s" is not from the dir "%s"`, path, dir))
	}

	relPath, err := filesystem.Rel(dir, path)
	if err != nil {
		panic(err)
	}

	p.RelatedPaths = append(p.RelatedPaths, relPath)
}

func (p *Paths) AddRelatedPathInRoot(path string) {
	p.RelatedPaths = append(p.RelatedPaths, fmt.Sprintf("..%c%s", filesystem.PathSeparator, path))
}

func (p *Paths) RenameRelatedPaths(oldPath, newPath string) {
	dir := p.Path()
	if !filesystem.IsFrom(oldPath, dir) {
		panic(errors.Errorf(`old "%s" is not from the dir "%s" (%s)`, oldPath, dir, newPath))
	}
	if !filesystem.IsFrom(newPath, dir) {
		panic(errors.Errorf(`new "%s" is not from the dir "%s"`, oldPath, dir))
	}
	oldRel, err := filesystem.Rel(dir, oldPath)
	if err != nil {
		panic(err)
	}
	newRel, err := filesystem.Rel(dir, newPath)
	if err != nil {
		panic(err)
	}

	// Rename all related paths that match old -> new
	for i, path := range p.RelatedPaths {
		if path == oldRel {
			p.RelatedPaths[i] = newRel
		} else if filesystem.IsFrom(path, oldRel) {
			pathRel, err := filesystem.Rel(oldRel, path)
			if err != nil {
				panic(err)
			}
			p.RelatedPaths[i] = filesystem.Join(newRel, pathRel)
		}
	}
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
		return b.String()
	}
}

func (c ConfigManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_config_%s", c.Level(), c.Path())
	} else {
		return c.String()
	}
}

func (r ConfigRowManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_row_%s", r.Level(), r.Path())
	} else {
		return r.String()
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

func (b *BranchManifest) MetadataMap() map[string]string {
	metadata := make(map[string]string)
	if b.Metadata != nil {
		for _, key := range b.Metadata.Keys() {
			val, _ := b.Metadata.Get(key)
			metadata[key] = val.(string)
		}
	}
	return metadata
}

func (r *ConfigRowManifest) AddRelation(relation Relation) {
	r.Relations.Add(relation)
}
