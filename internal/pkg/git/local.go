package git

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const CommitHashNotSet = "-"

type LocalRepository struct {
	ref model.TemplateRepository
	fs  filesystem.Fs
}

func (v LocalRepository) String() string {
	return fmt.Sprintf("dir:%s", v.ref.URL)
}

func (v LocalRepository) Definition() model.TemplateRepository {
	return v.ref
}

func (v LocalRepository) CommitHash() string {
	return CommitHashNotSet // the state of the repository does not change
}

func (v LocalRepository) Fs() (filesystem.Fs, RepositoryFsUnlockFn) {
	// No unlock function, FS is not updated/modified
	return v.fs, func() {}
}

func (v LocalRepository) Free() <-chan struct{} {
	// No operation
	done := make(chan struct{})
	close(done)
	return done
}

func NewLocalRepository(ref model.TemplateRepository, fs filesystem.Fs) LocalRepository {
	return LocalRepository{ref: ref, fs: fs}
}
