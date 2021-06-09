package api

import (
	"context"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/tests"
	"keboola-as-code/src/utils"
	"testing"
	"time"
)

func TestNewStorageApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	a := NewStorageApi("foo.bar.com", context.Background(), logger, false)
	assert.NotNil(t, a)
	assert.Equal(t, "https://foo.bar.com/v2/storage", a.http.resty.HostURL)
}

func TestHostnameNotFond(t *testing.T) {
	api, logs := newStorageApiWithHost(t, "foo.bar.com")
	token, err := api.GetToken("mytoken")
	assert.Nil(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `Get "https://foo.bar.com/v2/storage/tokens/verify": dial tcp: lookup foo.bar.com: No address associated with hostname`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR\tGet "https://foo.bar.com/v2/storage/tokens/verify": dial tcp: lookup foo.bar.com: No address associated with hostname`, logs.String())
}

func TestInvalidHost(t *testing.T) {
	api, logs := newStorageApiWithHost(t, "google.com")
	token, err := api.GetToken("mytoken")
	assert.Nil(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `GET "https://google.com/v2/storage/tokens/verify" returned http code 404`)
	assert.Regexp(t, `DEBUG  HTTP-ERROR	GET "https://google.com/v2/storage/tokens/verify" returned http code 404`, logs.String())
}

func newStorageApi(t *testing.T) (*StorageApi, *utils.Writer) {
	return newStorageApiWithHost(t, tests.TestApiHost())
}

func newStorageApiWithHost(t *testing.T, apiHost string) (*StorageApi, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	api := NewStorageApi(apiHost, context.Background(), logger, false)

	// Set short retry delay in tests
	api.http.resty.RetryWaitTime = 1 * time.Millisecond
	api.http.resty.RetryMaxWaitTime = 1 * time.Millisecond

	return api, logs
}

//func newStorageApiWithToken(t *testing.T) (*StorageApi, *utils.Writer) {
//	apiHost := utils.MustGetEnv("TEST_KBC_STORAGE_API_HOST")
//	apiToken := utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN")
//	logger, logs := utils.NewDebugLogger()
//	api := NewStorageApiFromOptions(apiHost, context.Background(), logger, false)
//	token, err := api.GetToken(apiToken)
//	assert.NoError(t, err)
//	return api.WithToken(token), logs
//}
