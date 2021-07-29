package encryption

import (
	"context"
	"keboola-as-code/src/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEncryptionApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	a := NewEncryptionApi("connection.keboola.com", context.Background(), logger, true)
	assert.NotNil(t, a)
	assert.Equal(t, "https://encryption.keboola.com", a.apiHostUrl)
	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	encryptedMap, err := a.EncryptMapValues("keboola.ex-generic-v2", "1234", mapToEncrypt)
	assert.Nil(t, err)
	assert.NotNil(t, encryptedMap)
	assert.True(t, isEncrypted(encryptedMap["#keyToEncrypt"]))
}

func TestErrorEncryptionApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	a := NewEncryptionApi("connection.keboola.com", context.Background(), logger, false)
	_, err := a.EncryptMapValues("", "", mapToEncrypt)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "The componentId parameter is required")
}
