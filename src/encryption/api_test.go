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
	encryptedMap, err := a.EncryptMapValues("keboola.ex-generic-v2", "1234", mapToEncrypt)
	assert.Nil(t, err)
	assert.NotNil(t, encryptedMap)
	assert.True(t, isEncrypted(encryptedMap["#keyToEncrypt"]))
}

func TestErrorEncryptionApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}
	a := NewEncryptionApi("https://encryption.keboola.com", context.Background(), logger, false)
	_, err := a.EncryptMapValues("", "", mapToEncrypt)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "The componentId parameter is required")
}
