package dependencies

import (
	"context"
	"net/http"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	goaHttp "goa.design/goa/v3/http"
)

func TestNewProjectDeps_MasterTokenRequiredError(t *testing.T) {
	t.Parallel()
	d, useRealAPIs := NewMocked(t, context.Background())
	assert.False(t, useRealAPIs)
	token := keboola.Token{IsMaster: false}
	_, err := newProjectScope(context.Background(), d, token)
	assert.Error(t, err)
	assert.Equal(t, "a master token of a project administrator is required", err.Error())
	assert.Equal(t, http.StatusUnauthorized, err.(goaHttp.Statuser).StatusCode())
}
