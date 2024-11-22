package dependencies

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
)

// TestPublicRequestScope_Components_Cached tests that the value of the component does not change during the entire request.
func TestPublicRequestScope_Components_Cached(t *testing.T) {
	t.Parallel()

	// Mocked components
	components1 := keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: "foo1.bar1"}, Type: "other", Name: "Foo1 Bar1"},
	}
	components2 := keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: "foo2.bar2"}, Type: "other", Name: "Foo2 Bar2"},
	}
	assert.NotEqual(t, components1, components2)

	// Mocked API scope
	ctx := context.Background()
	apiScp, mock := NewMockedAPIScope(t, ctx, config.New(), dependencies.WithMockedComponents(components1))

	// Request 1 gets "components1"
	req1Scp := NewPublicRequestScope(apiScp, httptest.NewRequest(http.MethodGet, "/req1", nil))
	assert.Equal(t, components1, req1Scp.Components().All())
	assert.Equal(t, components1, req1Scp.Components().All())

	// Components are updated to "components2"
	mock.MockedHTTPTransport().RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/", mock.StorageAPIHost()),
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Components: components2,
		}).Once(),
	)
	assert.NoError(t, mock.ComponentsProvider().Update(context.Background()))

	// Request 1 still gets "components1"
	assert.Equal(t, components1, req1Scp.Components().All())
	assert.Equal(t, components1, req1Scp.Components().All())

	// But request2 gets "components2"
	req2Scp := NewPublicRequestScope(apiScp, httptest.NewRequest(http.MethodGet, "/req2", nil))
	assert.Equal(t, components2, req2Scp.Components().All())
	assert.Equal(t, components2, req2Scp.Components().All())
}
