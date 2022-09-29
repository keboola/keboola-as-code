package dbt

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const ProjectFile = "dbt_project.yml"
const SourcesPath = "models/_sources"

type Project struct {
	fs filesystem.Fs
}

func LoadProject(fs filesystem.Fs) (*Project, error) {
	if !fs.Exists(ProjectFile) {
		return nil, fmt.Errorf(`missing file "%s" in the "%s"`, ProjectFile, fs.BasePath())
	}

	// TODO

	return &Project{fs: fs}, nil
}
