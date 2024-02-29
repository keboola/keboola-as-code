package dependencies

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/flag"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestDifferentProjectIdInManifestAndToken(t *testing.T) {
	t.Parallel()

	// Create manifest
	fs := aferofs.NewMemoryFs()
	manifestContent := `{"version": 2, "project": {"id": 789, "apiHost": "mocked.transport.http"}}`
	assert.NoError(t, fs.WriteFile(context.Background(), filesystem.NewRawFile(projectManifest.Path(), manifestContent)))

	// Set token
	opts := options.New()
	opts.Set(options.StorageAPITokenOpt, "my-secret")

	// Create http client
	logger := log.NewNopLogger()
	httpClient, httpTransport := client.NewMockedClient()

	// Mock API index
	httpTransport.RegisterResponder(resty.MethodGet, `=~storage/?exclude=components`,
		httpmock.NewStringResponder(200, `{
			"services": [],
			"features": []
		}`),
	)
	httpTransport.RegisterResponder(
		http.MethodGet,
		"https://mocked.transport.http/v2/storage/",
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{
				Services: keboola.Services{
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
		httpmock.NewJsonResponderOrPanic(200, keboola.Token{IsMaster: true, Owner: keboola.TokenOwner{ID: 12345}}),
	)

	// Assert
	ctx := context.Background()
	proc := servicectx.NewForTest(t)
	baseScp := newBaseScope(ctx, logger, os.Stdout, os.Stderr, proc, httpClient, fs, dialog.New(nopPrompt.New(), opts), opts, flag.DefaultGlobalFlags(), env.Empty())
	localScp, err := newLocalCommandScope(ctx, baseScp)
	assert.NoError(t, err)
	_, err = newRemoteCommandScope(ctx, localScp)
	expected := `given token is from the project "12345", but in manifest is defined project "789"`
	assert.Error(t, err)
	assert.Equal(t, expected, err.Error())
}
