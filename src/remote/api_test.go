package remote

import (
	"context"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
)

func TestNewStorageApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	a := NewStorageApi("foo.bar.com", context.Background(), logger, false)
	assert.NotNil(t, a)
	assert.Equal(t, "foo.bar.com", a.Host())
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.HostUrl())
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.client.HostUrl())
}

func TestHostnameNotFound(t *testing.T) {
	api, logs := TestStorageApiWithHost(t, "foo.bar.com")
	token, err := api.GetToken("mytoken")
	assert.Nil(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `Get "https://foo.bar.com/v2/storage/tokens/verify": dial tcp: lookup foo.bar.com: No address associated with hostname`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR\tGet "https://foo.bar.com/v2/storage/tokens/verify": dial tcp: lookup foo.bar.com: No address associated with hostname`, logs.String())
}

func TestInvalidHost(t *testing.T) {
	api, logs := TestStorageApiWithHost(t, "google.com")
	token, err := api.GetToken("mytoken")
	assert.Nil(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `GET https://google.com/v2/storage/tokens/verify | returned http code 404`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR	GET https://google.com/v2/storage/tokens/verify | returned http code 404`, logs.String())
}
