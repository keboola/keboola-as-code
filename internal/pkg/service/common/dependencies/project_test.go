package dependencies

import (
	"net/http"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goaHttp "goa.design/goa/v3/http"
)

func TestNewProjectDeps_MasterTokenRequiredError(t *testing.T) {
	t.Parallel()
	d := NewMocked(t, t.Context())
	assert.False(t, d.UseRealAPIs())
	token := keboola.Token{IsMaster: false}
	_, err := newProjectScope(t.Context(), d, token)
	require.Error(t, err)
	assert.Equal(t, "a master token of a project administrator is required", err.Error())
	assert.Equal(t, http.StatusUnauthorized, err.(goaHttp.Statuser).StatusCode())
}
