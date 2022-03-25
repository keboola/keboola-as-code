package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type Path string

type AbsPath struct {
	RelPath       Path `json:"path" validate:"required"`
	parentPath    Path
	parentPathSet bool
}

func NewAbsPath(parentPath, objectPath string) AbsPath {
	return AbsPath{parentPath: Path(parentPath), parentPathSet: true, RelPath: Path(objectPath)}
}

func (p AbsPath) DeepCopy(_ deepcopy.TranslateFunc, _ deepcopy.Steps, _ deepcopy.VisitedMap) AbsPath {
	return p
}

func (p AbsPath) Path() AbsPath {
	return p
}

func (p *AbsPath) SetPath(path AbsPath) {
	*p = path
}

func (p AbsPath) RelativePath() string {
	return string(p.RelPath)
}

func (p AbsPath) WithRelativePath(path string) AbsPath {
	p.RelPath = Path(path)
	return p
}

func (p AbsPath) ParentPath() string {
	return string(p.parentPath)
}

func (p AbsPath) WithParentPath(parentPath string) AbsPath {
	p.parentPathSet = true
	p.parentPath = Path(parentPath)
	return p
}

func (p AbsPath) IsSet() bool {
	return p.parentPathSet && p.RelPath != ""
}

func (p AbsPath) String() string {
	return filesystem.Join(string(p.parentPath), string(p.RelPath))
}
