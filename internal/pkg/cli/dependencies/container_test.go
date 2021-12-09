package dependencies

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestDifferentProjectIdInManifestAndToken(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())

	logger := zap.NewNop().Sugar()
	opts := options.New()
	d := NewContainer(context.Background(), nil, testhelper.NewMemoryFs(), nil, logger, opts)
	d.hostFromManifest = true
	d.options.Set(options.StorageApiTokenOpt, project.Token())
	d.projectManifest = &manifest.Manifest{
		Content: &manifest.Content{
			Project: model.Project{
				Id:      12345,
				ApiHost: project.StorageApiHost(),
			},
		},
	}

	_, err := d.StorageApi()
	expected := fmt.Sprintf(`given token is from the project "%d", but in manifest is defined project "12345"`, project.Id())
	assert.Error(t, err)
	assert.Equal(t, expected, err.Error())
}
