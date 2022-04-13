package repository_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestNewManager(t *testing.T) {
	t.Parallel()

	m, err := repository.NewManager(log.NewDebugLogger())
	assert.NoError(t, err)

	defaultRepo, err := m.Repository(repository.DefaultRepository())
	assert.NoError(t, err)
	assert.True(t, defaultRepo.Fs().Exists("/.keboola/repository.json"))
}

func TestAddRepository_AlreadyExists(t *testing.T) {
	t.Parallel()

	m, err := repository.NewManager(log.NewDebugLogger())
	assert.NoError(t, err)

	err = m.AddRepository(repository.DefaultRepository())
	assert.Errorf(t, err, "repository already exists")
}
