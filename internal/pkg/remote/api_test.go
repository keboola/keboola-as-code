package remote_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/remote"
)

func TestNewStorageApi(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	a := NewStorageApi("foo.bar.com", context.Background(), logger.Logger, false)
	assert.NotNil(t, a)
	assert.Equal(t, "foo.bar.com", a.Host())
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.HostUrl())
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.RestyClient().HostURL)
}

func TestHostnameNotFound(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	api := NewStorageApi("foo.bar.com", context.Background(), logger.Logger, false)
	token, err := api.GetToken("mytoken")
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `Get "https://foo.bar.com/v2/storage/tokens/verify": dial tcp`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR\tGet "https://foo.bar.com/v2/storage/tokens/verify": dial tcp`, logger.String())
}

func TestInvalidHost(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	api := NewStorageApi("google.com", context.Background(), logger.Logger, false)
	token, err := api.GetToken("mytoken")
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `GET https://google.com/v2/storage/tokens/verify | returned http code 404`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR	GET https://google.com/v2/storage/tokens/verify | returned http code 404`, logger.String())
}
