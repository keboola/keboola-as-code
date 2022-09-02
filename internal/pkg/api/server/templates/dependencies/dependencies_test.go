package dependencies

import (
	"context"
	"fmt"
	"io"
	stdLog "log"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// TestForPublicRequest_Components_Cached tests that the value of the component does not change during the entire request.
func TestForPublicRequest_Components_Cached(t *testing.T) {
	t.Parallel()

	// Mocked components
	components1 := storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: "foo1.bar1"}, Type: "other", Name: "Foo1 Bar1"},
	}
	components2 := storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: "foo2.bar2"}, Type: "other", Name: "Foo2 Bar2"},
	}
	assert.NotEqual(t, components1, components2)

	// Create mocked dependencies for server with "components1"
	nopApiLogger := log.NewApiLogger(stdLog.New(io.Discard, "", 0), "", false)
	mockedDeps := dependencies.NewMockedDeps(dependencies.WithMockedComponents(components1))
	serverDeps := &forServer{Base: mockedDeps, Public: mockedDeps, serverCtx: context.Background(), logger: nopApiLogger}

	// Request 1 gets "components1"
	req1Deps := NewDepsForPublicRequest(serverDeps, context.Background(), "req1")
	assert.Equal(t, components1, req1Deps.Components().All())
	assert.Equal(t, components1, req1Deps.Components().All())

	// Components are updated to "components2"
	mockedDeps.MockedHttpTransport().RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/", mockedDeps.StorageApiHost()),
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Components: components2,
		}).Once(),
	)
	assert.NoError(t, mockedDeps.ComponentsProvider().Update(context.Background()))

	// Request 1 still gets "components1"
	assert.Equal(t, components1, req1Deps.Components().All())
	assert.Equal(t, components1, req1Deps.Components().All())

	// But request2 gets "components2"
	req2Deps := NewDepsForPublicRequest(serverDeps, context.Background(), "req2")
	assert.Equal(t, components2, req2Deps.Components().All())
	assert.Equal(t, components2, req2Deps.Components().All())
}
