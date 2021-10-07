package model

import (
	"fmt"
	"os"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const (
	SortById   = "id"
	SortByPath = "path"
)

type RecordPaths interface {
	Path() string          // parent path + object path -> path relative to the project dir
	GetObjectPath() string // path relative to the parent object
	GetParentPath() string // parent path relative to the project dir
}

// Record - manifest record.
type Record interface {
	Key
	RecordPaths
	Key() Key                   // unique key for map -> for fast access
	SortKey(sort string) string // unique key for sorting
	SetObjectPath(string)       // set path relative to the parent object
	SetParentPath(string)       // set parent path
	GetRelatedPaths() []string  // files related to the record, relative to the project dir, e.g. main/meta.json
	AddRelatedPath(path string)
	State() *RecordState
}

type RecordState struct {
	Invalid   bool // if true, object files are not valid, eg. missing file, invalid JSON, ...
	NotFound  bool // if true, object directory is not present in the filesystem
	Persisted bool // if true, record will be part of the manifest when saved
	Deleted   bool // if true, record has been deleted in this command run
}

type PathInProject struct {
	ObjectPath string `json:"path" validate:"required"`
	ParentPath string `json:"-"` // not serialized, records are stored hierarchically
}

type Paths struct {
	PathInProject
	RelatedPaths []string `json:"-"` // no serialized, is generated when the object is loaded
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
}

type ConfigRowManifest struct {
	RecordState `json:"-"`
	ConfigRowKey
	Paths
}

type ConfigManifestWithRows struct {
	*ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

func (p *PathInProject) GetObjectPath() string {
	return p.ObjectPath
}

func (p *PathInProject) SetObjectPath(path string) {
	p.ObjectPath = path
}

func (p *PathInProject) GetParentPath() string {
	return p.ParentPath
}

func (p *PathInProject) SetParentPath(parentPath string) {
	p.ParentPath = parentPath
}

func (p PathInProject) Path() string {
	return filesystem.Join(
		strings.ReplaceAll(p.ParentPath, "/", string(os.PathSeparator)),
		strings.ReplaceAll(p.ObjectPath, "/", string(os.PathSeparator)),
	)
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
	prefix := dir + string(os.PathSeparator)
	if !strings.HasPrefix(path, prefix) {
		panic(fmt.Errorf(`path "%s" is not from the dir "%s"`, path, dir))
	}

	p.RelatedPaths = append(p.RelatedPaths, strings.TrimPrefix(path, prefix))
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
