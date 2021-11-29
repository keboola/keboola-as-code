package remote_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestNewStorageApi(t *testing.T) {
	t.Parallel()
	logger, _ := utils.NewDebugLogger()
	a := NewStorageApi("foo.bar.com", context.Background(), logger, false)
	assert.NotNil(t, a)
	assert.Equal(t, "foo.bar.com", a.Host())
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.HostUrl())
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.RestyClient().HostURL)
}

func TestHostnameNotFound(t *testing.T) {
	t.Parallel()
	logger, logs := utils.NewDebugLogger()
	api := NewStorageApi("foo.bar.com", context.Background(), logger, false)
	token, err := api.GetToken("mytoken")
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `Get "https://foo.bar.com/v2/storage/tokens/verify": dial tcp`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR\tGet "https://foo.bar.com/v2/storage/tokens/verify": dial tcp`, logs.String())
}

func TestInvalidHost(t *testing.T) {
	t.Parallel()
	logger, logs := utils.NewDebugLogger()
	api := NewStorageApi("google.com", context.Background(), logger, false)
	token, err := api.GetToken("mytoken")
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `GET https://google.com/v2/storage/tokens/verify | returned http code 404`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR	GET https://google.com/v2/storage/tokens/verify | returned http code 404`, logs.String())
}
