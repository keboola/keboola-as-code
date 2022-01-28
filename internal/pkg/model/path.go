package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type AbsPath struct {
	RelativePath  string `json:"path" validate:"required"`
	parentPath    string
	parentPathSet bool
}

type Paths struct {
	AbsPath
	RelatedPaths []string `json:"-"` // not serialized, slice is generated when the object is loaded
}

func NewAbsPath(parentPath, objectPath string) AbsPath {
	return AbsPath{parentPath: parentPath, parentPathSet: true, RelativePath: objectPath}
}

func (p AbsPath) DeepCopy(_ deepcopy.TranslateFunc, _ deepcopy.Steps, _ deepcopy.VisitedMap) AbsPath {
	return p
}

func (p AbsPath) GetAbsPath() AbsPath {
	return p
}

func (p *AbsPath) GetRelativePath() string {
	return p.RelativePath
}

func (p *AbsPath) SetRelativePath(path string) {
	p.RelativePath = path
}

func (p *AbsPath) GetParentPath() string {
	return p.parentPath
}

func (p *AbsPath) IsParentPathSet() bool {
	return p.parentPathSet
}

func (p *AbsPath) SetParentPath(parentPath string) {
	p.parentPathSet = true
	p.parentPath = parentPath
}

func (p AbsPath) Path() string {
	return filesystem.Join(p.parentPath, p.RelativePath)
}
