package encryption

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/utils"
)

func TestNewEncryptionApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	a := NewEncryptionApi("https://encryption.keboola.com", context.Background(), logger, true)
	assert.NotNil(t, a)
	assert.Equal(t, "https://encryption.keboola.com", a.hostUrl)
	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	request := a.CreateEncryptRequest("keboola.ex-generic-v2", 1234, mapToEncrypt).Send()
	assert.True(t, request.HasResult())
	assert.False(t, request.HasError())
	encryptedMap := *request.Result.(*map[string]string)
	assert.NotEmpty(t, encryptedMap)
	assert.True(t, IsEncrypted(encryptedMap["#keyToEncrypt"]))
}

func TestErrorEncryptionApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	a := NewEncryptionApi("https://encryption.keboola.com", context.Background(), logger, true)
	assert.NotNil(t, a)
	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	request := a.CreateEncryptRequest("", 1234, mapToEncrypt).Send()
	assert.False(t, request.HasResult())
	assert.True(t, request.HasError())
	assert.Error(t, request.Err())
	assert.Contains(t, request.Err().Error(), "The componentId parameter is required")
}
