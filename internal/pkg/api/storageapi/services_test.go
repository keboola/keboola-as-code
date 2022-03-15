package storageapi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestServicesUrls(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	api := New(context.Background(), logger, "connection.keboola.com", false)
	urls, err := api.ServicesUrlById()
	assert.NoError(t, err)

	assert.Equal(t, urls[`encryption`], ServiceUrl("https://encryption.keboola.com"))
	assert.Equal(t, urls[`scheduler`], ServiceUrl("https://scheduler.keboola.com"))
}
