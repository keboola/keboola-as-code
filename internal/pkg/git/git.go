package git

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Repository interface {
	String() string
	Definition() model.TemplateRepository
	CommitHash() string
	Fs() (filesystem.Fs, RepositoryFsUnlockFn)
}
