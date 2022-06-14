package model

import (
	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type Path string

type AbsPath struct {
	RelativePath  Path `json:"path" validate:"required"`
	parentPath    Path
	parentPathSet bool
}

type Paths struct {
	AbsPath
	RelatedPaths []string `json:"-"` // not serialized, slice is generated when the object is loaded
}

func NewAbsPath(parentPath, objectPath string) AbsPath {
	return AbsPath{parentPath: Path(parentPath), parentPathSet: true, RelativePath: Path(objectPath)}
}

func (p AbsPath) HandleDeepCopy(_ deepcopy.TranslateFn, _ deepcopy.Path, _ deepcopy.VisitedPtrMap) (AbsPath, deepcopy.CloneFn) {
	return p, nil
}

func (p AbsPath) GetAbsPath() AbsPath {
	return p
}

func (p *AbsPath) GetRelativePath() string {
	return string(p.RelativePath)
}

func (p *AbsPath) SetRelativePath(path string) {
	p.RelativePath = Path(path)
}

func (p *AbsPath) GetParentPath() string {
	return string(p.parentPath)
}

func (p *AbsPath) IsParentPathSet() bool {
	return p.parentPathSet
}

func (p *AbsPath) SetParentPath(parentPath string) {
	p.parentPathSet = true
	p.parentPath = Path(parentPath)
}

func (p AbsPath) Path() string {
	return filesystem.Join(string(p.parentPath), string(p.RelativePath))
}
