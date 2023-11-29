package repository_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestRepository_ObjectPrefix(t *testing.T) {
	t.Parallel()
	repo := repository.New(dependencies.NewMocked(t, dependencies.WithEnabledEtcdClient()))

	expected := "storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/"
	actual := repo.ObjectPrefix(storage.LevelStaging, test.NewSliceKey())
	assert.Equal(t, expected, actual)
}
