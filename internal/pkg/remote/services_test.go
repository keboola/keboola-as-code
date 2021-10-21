package remote

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestGetEncryptionApiUrl(t *testing.T) {
	t.Parallel()
	logger, _ := utils.NewDebugLogger()
	api := NewStorageApi("connection.keboola.com", context.Background(), logger, false)
	encryptionApiUrl, _ := api.GetEncryptionApiUrl()
	assert.NotEmpty(t, encryptionApiUrl)
	assert.Equal(t, encryptionApiUrl, "https://encryption.keboola.com")
}

func TestErrorGetEncryptionApiUrl(t *testing.T) {
	t.Parallel()
	logger, _ := utils.NewDebugLogger()
	api := NewStorageApi("connection.foobar.keboola.com", context.Background(), logger, false)
	_, err := api.GetEncryptionApiUrl()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such host")
}

func TestGetSchedulerApiUrl(t *testing.T) {
	t.Parallel()
	logger, _ := utils.NewDebugLogger()
	api := NewStorageApi("connection.keboola.com", context.Background(), logger, false)
	apiUrl, _ := api.GetSchedulerApiUrl()
	assert.NotEmpty(t, apiUrl)
	assert.Equal(t, apiUrl, "https://scheduler.keboola.com")
}
