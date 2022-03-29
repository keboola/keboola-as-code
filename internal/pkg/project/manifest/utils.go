package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type projectInfo struct {
	Project Project `json:"project"`
}

func ProjectInfo(fs filesystem.Fs, manifestPath string) (project Project, err error) {
	// Exists?
	if !fs.IsFile(manifestPath) {
		return project, fmt.Errorf("manifest \"%s\" not found", manifestPath)
	}

	// Load project info
	var info projectInfo
	file := filesystem.NewFileDef(manifestPath).SetDescription("manifest")
	if _, err := fs.FileLoader().ReadJsonFileTo(file, &info); err != nil {
		return project, InvalidManifestError{err}
	}
	project = info.Project

	// Validate project ID
	if project.Id == 0 {
		return project, InvalidManifestError{fmt.Errorf(`missing "project.id" key in "%s"`, manifestPath)}
	}

	// Validate Storage API host
	if project.ApiHost == "" {
		return project, InvalidManifestError{fmt.Errorf(`missing "project.apiHost" key in "%s"`, manifestPath)}
	}

	return project, nil
}
