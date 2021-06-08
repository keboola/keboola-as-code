package api

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
	assert.NoError(t, out.Flush())

	expected := "DEBUG  HTTP\tSome debug\nDEBUG  HTTP-WARN\tSome warning\nDEBUG  HTTP-ERROR\tSome error\n"
	assert.Equal(t, expected, out.Buffer.String())
}
