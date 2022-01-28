package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type AbsPath struct {
	ObjectPath    string `json:"path" validate:"required"`
	parentPath    string
	parentPathSet bool
}

type Paths struct {
	AbsPath
	RelatedPaths []string `json:"-"` // not serialized, slice is generated when the object is loaded
}

func NewAbsPath(parentPath, objectPath string) AbsPath {
	return AbsPath{parentPath: parentPath, parentPathSet: true, ObjectPath: objectPath}
}

func (p AbsPath) DeepCopy(_ deepcopy.TranslateFunc, _ deepcopy.Steps, _ deepcopy.VisitedMap) AbsPath {
	return p
}

func (p AbsPath) GetPathInProject() AbsPath {
	return p
}

func (p *AbsPath) GetObjectPath() string {
	return p.ObjectPath
}

func (p *AbsPath) SetObjectPath(path string) {
	p.ObjectPath = path
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
	return filesystem.Join(p.parentPath, p.ObjectPath)
}
