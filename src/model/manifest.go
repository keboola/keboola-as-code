package model

import (
	"fmt"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cast"
)

const (
	SortById       = "id"
	SortByPath     = "path"
	AllBranchesDef = "__all__"
	MainBranchDef  = "__main__"
)

type AllowedBranch string
type AllowedBranches []AllowedBranch

func (v AllowedBranches) String() string {
	if len(v) == 0 {
		return `[]`
	}

	items := make([]string, 0)
	for _, item := range v {
		items = append(items, string(item))
	}
	return `"` + strings.Join(items, `", "`) + `"`
}

func (v AllowedBranches) IsBranchAllowed(branch *Branch) bool {
	for _, definition := range v {
		if definition.IsBranchAllowed(branch) {
			return true
		}
	}
	return false
}

func (v AllowedBranch) IsBranchAllowed(branch *Branch) bool {
	pattern := string(v)

	// All branches
	if pattern == AllBranchesDef {
		return true
	}

	// Main branch
	if pattern == MainBranchDef && branch.IsDefault {
		return true
	}

	// Defined by ID
	if cast.ToInt(pattern) == branch.Id {
		return true
	}

	// Defined by name blob
	if match, _ := filepath.Match(string(v), branch.Name); match {
		return true
	}

	// Defined by name blob - normalized name
	if match, _ := filepath.Match(string(v), utils.NormalizeName(branch.Name)); match {
		return true
	}

	return false
}

// Record - manifest record
type Record interface {
	Kind() Kind                 // eg. branch, config, config row -> used in logs
	Level() int                 // hierarchical level, "1" for branch, "2" for config, ...
	Key() Key                   // unique key for map -> for fast access
	SortKey(sort string) string // unique key for sorting
	RelativePath() string       // path to the object directory
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

type Paths struct {
	Path         string   `json:"path" validate:"required"`
	ParentPath   string   `json:"-"` // not serialized, records are stored hierarchically
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

func (p *Paths) RelativePath() string {
	return filepath.Join(
		strings.ReplaceAll(p.ParentPath, "/", string(os.PathSeparator)),
		strings.ReplaceAll(p.Path, "/", string(os.PathSeparator)),
	)
}

func (p *Paths) GetRelatedPaths() []string {
	dir := p.RelativePath()
	out := make([]string, 0)
	for _, path := range p.RelatedPaths {
		// Prefix by dir -> path will be relative to the project dir
		out = append(out, filepath.Join(dir, path))
	}
	return out
}

func (p *Paths) AddRelatedPath(path string) {
	dir := p.RelativePath()
	if !strings.HasPrefix(path, dir) {
		panic(fmt.Errorf(`path "%s" is not from the dir "%s"`, path, dir))
	}
	p.RelatedPaths = append(p.RelatedPaths, utils.RelPath(dir, path))
}

func (p *Paths) AbsolutePath(projectDir string) string {
	return filepath.Join(projectDir, p.RelativePath())
}

func (b BranchManifest) Kind() Kind {
	return Kind{Name: "branch", Abbr: "B"}
}

func (c ConfigManifest) Kind() Kind {
	return Kind{Name: "config", Abbr: "C"}
}

func (r ConfigRowManifest) Kind() Kind {
	return Kind{Name: "config row", Abbr: "R"}
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

func (b *BranchManifest) ResolveParentPath() {
	b.ParentPath = ""
}

func (c *ConfigManifest) ResolveParentPath(branchManifest *BranchManifest) {
	c.ParentPath = filepath.Join(branchManifest.ParentPath, branchManifest.Path)
}

func (r *ConfigRowManifest) ResolveParentPath(configManifest *ConfigManifest) {
	r.ParentPath = filepath.Join(configManifest.ParentPath, configManifest.Path)
}

func (b BranchManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_branch_%s", b.Level(), b.RelativePath())
	} else {
		return b.BranchKey.String()
	}

}

func (c ConfigManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_config_%s", c.Level(), c.RelativePath())
	} else {
		return c.ConfigKey.String()
	}

}

func (r ConfigRowManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_row_%s", r.Level(), r.RelativePath())
	} else {
		return r.ConfigRowKey.String()
	}

}
