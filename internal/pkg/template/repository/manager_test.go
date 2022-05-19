package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestNewManager(t *testing.T) {
	t.Parallel()

	m := repository.NewManager(context.Background(), log.NewDebugLogger())
	err := m.AddRepository(repository.DefaultRepository())
	assert.NoError(t, err)

	defaultRepo, err := m.Repository(repository.DefaultRepository())
	assert.NoError(t, err)
	assert.True(t, defaultRepo.Fs().Exists("/.keboola/repository.json"))
}

func TestAddRepository_AlreadyExists(t *testing.T) {
	t.Parallel()

	m := repository.NewManager(context.Background(), log.NewDebugLogger())
	err := m.AddRepository(repository.DefaultRepository())
	assert.NoError(t, err)

	err = m.AddRepository(repository.DefaultRepository())
	assert.NoError(t, err)
}
