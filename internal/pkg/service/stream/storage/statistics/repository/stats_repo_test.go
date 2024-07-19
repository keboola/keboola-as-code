package repository_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestRepository_ObjectPrefix(t *testing.T) {
	t.Parallel()

	d, _ := dependencies.NewMockedStorageScope(t)
	repo := d.StatisticsRepository()

	expected := "storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/"
	actual := repo.ObjectPrefix(model.LevelStaging, test.NewSliceKey())
	assert.Equal(t, expected, actual)
}
