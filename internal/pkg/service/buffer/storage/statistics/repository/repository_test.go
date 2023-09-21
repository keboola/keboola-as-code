package repository_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRepository_ObjectPrefix(t *testing.T) {
	t.Parallel()
	repo := repository.New(dependencies.NewMocked(t, dependencies.WithEnabledEtcdClient()))

	expected := "storage/stats/staging/123/my-receiver/my-export/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/"
	actual := repo.ObjectPrefix(storage.LevelStaging, test.NewSliceKey())
	assert.Equal(t, expected, actual)
}
