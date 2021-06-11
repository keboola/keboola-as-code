package http

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
)

func TestClientLogger(t *testing.T) {
	logger, out := utils.NewDebugLogger()
	clientLogger := &ClientLogger{logger}
	clientLogger.Debugf("Some debug")
	clientLogger.Warnf("Some warning")
	clientLogger.Errorf("Some error")

	expected := "DEBUG  HTTP\tSome debug\nDEBUG  HTTP-WARN\tSome warning\nDEBUG  HTTP-ERROR\tSome error\n"
	assert.Equal(t, expected, out.String())
}

func TestRemoveSecrets(t *testing.T) {
	assert.Equal(t, "token/verify", removeSecrets("token/verify"))
	assert.Equal(t, "tokens/verify", removeSecrets("tokens/verify"))
	assert.Equal(t, "token, abc", removeSecrets("token, abc"))
	assert.Equal(t, "token: *****", removeSecrets("token: ABC12345-abc"))
	assert.Equal(t, "token: ***** ", removeSecrets("token: ABC12345-abc "))
	assert.Equal(t, "foo1: bar1\ntoken: *****\nfoo2: bar2", removeSecrets("foo1: bar1\ntoken: ABC12345-abc\nfoo2: bar2"))
	assert.Equal(t, "X-StorageApi-Token: *****", removeSecrets("X-StorageApi-Token: ABC12345-abc"))
}
