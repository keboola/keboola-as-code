package api

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSendEvent(t *testing.T) {
	api, _ := TestStorageApiWithToken(t)
	event, err := api.SendEvent("info", "Test event", 123456*time.Millisecond, map[string]interface{}{"foo": "bar"})
	assert.NoError(t, err)
	assert.NotNil(t, event)
}
