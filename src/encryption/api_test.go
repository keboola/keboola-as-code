package encryption

import (
	"context"
	"fmt"
	"keboola-as-code/src/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEncryptionApi(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	a := NewEncryptionApi("connection.keboola.com", context.Background(), logger, true)
	assert.NotNil(t, a)
	assert.Equal(t, "encryption.keboola.com", a.apiHost)
	mapToEncrypt := map[string]string{"#keyToEncrypt": "value"}

	encryptedMap, err := a.EncryptMapValues("keboola.ex-generic-v2", "1234", mapToEncrypt)
	fmt.Println(err.Error())
	assert.Nil(t, err)
	assert.NotNil(t, encryptedMap)
	assert.True(t, isValueEncrypted(encryptedMap["#keyToEncrypt"]))
}
