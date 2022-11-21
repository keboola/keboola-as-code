package dependencies

import (
	"context"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestDifferentProjectIdInManifestAndToken(t *testing.T) {
	t.Parallel()

	// Create manifest
	fs := aferofs.NewMemoryFs()
	manifestContent := `{"version": 2, "project": {"id": 789, "apiHost": "mocked.transport.http"}}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(projectManifest.Path(), manifestContent)))

	// Set token
	opts := options.New()
	opts.Set(options.StorageApiTokenOpt, "my-secret")

	// Create http client
	logger := log.NewNopLogger()
	httpClient, httpTransport := client.NewMockedClient()

	// Mock API index
	httpTransport.RegisterResponder(
		http.MethodGet,
		"https://mocked.transport.http/v2/storage/",
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Index: storageapi.Index{
				Services: storageapi.Services{
					{ID: "encryption", URL: "https://encryption.mocked.transport.http"},
					{ID: "scheduler", URL: "https://scheduler.mocked.transport.http"},
					{ID: "queue", URL: "https://queue.mocked.transport.http"},
					{ID: "sandboxes", URL: "https://sandboxes.mocked.transport.http"},
				},
			},
			Components: testapi.MockedComponents(),
		}),
	)

	// Mocked token verification
	httpTransport.RegisterResponder(
		http.MethodGet,
		"https://mocked.transport.http/v2/storage/tokens/verify",
		httpmock.NewJsonResponderOrPanic(200, storageapi.Token{IsMaster: true, Owner: storageapi.TokenOwner{ID: 12345}}),
	)

	// Assert
	baseDeps := newBaseDeps(context.Background(), env.Empty(), logger, httpClient, fs, dialog.New(nopPrompt.New()), opts)
	publicDeps, err := newPublicDeps(baseDeps)
	assert.NoError(t, err)
	_, err = newProjectDeps(context.Background(), publicDeps)
	expected := `given token is from the remote "12345", but in manifest is defined remote "789"`
	assert.Error(t, err)
	assert.Equal(t, expected, err.Error())
}
