package repository_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestNewManager(t *testing.T) {
	t.Parallel()

	m, err := repository.NewManager(log.NewDebugLogger())
	assert.NoError(t, err)

	defaultRepo, err := m.Repository(repository.DefaultRepository())
	assert.NoError(t, err)
	_ = defaultRepo.CallWithFs(func(fs filesystem.Fs) error {
		assert.True(t, fs.Exists("/.keboola/repository.json"))
		return nil
	})
}

func TestAddRepository_AlreadyExists(t *testing.T) {
	t.Parallel()

	m, err := repository.NewManager(log.NewDebugLogger())
	assert.NoError(t, err)

	err = m.AddRepository(repository.DefaultRepository())
	assert.Errorf(t, err, "repository already exists")
}
