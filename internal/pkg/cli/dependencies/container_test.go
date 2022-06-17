package dependencies

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestDifferentProjectIdInManifestAndToken(t *testing.T) {
	t.Parallel()
	testProject := testproject.GetTestProject(t, env.Empty())

	d := NewContainer(context.Background(), env.Empty(), testfs.NewMemoryFs(), dialog.New(nopPrompt.New()), log.NewNopLogger(), options.New())
	c := d.(*container)
	c.project = project.NewWithManifest(testfs.NewMemoryFs(), manifest.New(12345, testProject.StorageAPIHost()), d)
	c.SetStorageApiHost(testProject.StorageAPIHost())
	c.SetStorageApiToken(testProject.StorageAPIToken())

	_, err := d.StorageApiClient()
	expected := fmt.Sprintf(`given token is from the project "%d", but in manifest is defined project "12345"`, testProject.ID())
	assert.Error(t, err)
	assert.Equal(t, expected, err.Error())
}
