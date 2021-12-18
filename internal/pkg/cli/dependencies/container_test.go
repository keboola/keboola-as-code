package dependencies

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestDifferentProjectIdInManifestAndToken(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())

	logger := log.NewNopLogger()
	opts := options.New()
	d := NewContainer(context.Background(), nil, testfs.NewMemoryFs(), nil, logger, opts)
	d.hostFromManifest = true
	d.options.Set(options.StorageApiTokenOpt, project.Token())
	d.projectManifest = manifest.New(12345, project.StorageApiHost())

	_, err := d.StorageApi()
	expected := fmt.Sprintf(`given token is from the project "%d", but in manifest is defined project "12345"`, project.Id())
	assert.Error(t, err)
	assert.Equal(t, expected, err.Error())
}
