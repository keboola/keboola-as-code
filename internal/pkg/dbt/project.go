package dbt

import (
	"context"
	"errors"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Project struct {
	fs          filesystem.Fs
	projectFile ProjectFile
}

type ProjectFile struct {
	Profile string `yaml:"profile" validate:"required"`
}

func LoadProject(ctx context.Context, fs filesystem.Fs) (*Project, error) {
	out := &Project{fs: fs}

	// Load project file
	fileDef := filesystem.NewFileDef(ProjectFilePath).SetDescription("dbt project")
	if _, err := fs.FileLoader().ReadYamlFileTo(fileDef, &out.projectFile); errors.Is(err, filesystem.ErrNotExist) {
		return nil, fmt.Errorf(`missing  "%s" in the "%s"`, ProjectFilePath, fs.BasePath())
	} else if err != nil {
		return nil, err
	}

	// Validate project file
	if err := validator.New().Validate(ctx, out.projectFile); err != nil {
		return nil, fmt.Errorf(`dbt project file "%s" is not valid: %w`, ProjectFilePath, err)
	}

	return out, nil
}

func (p *Project) Fs() filesystem.Fs {
	return p.fs
}

func (p *Project) Profile() string {
	return p.projectFile.Profile
}
