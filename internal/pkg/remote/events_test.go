package remote_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/remote"
)

func TestSendEvent(t *testing.T) {
	api, _ := TestStorageApiWithToken(t)
	params := map[string]interface{}{"foo1": "bar1"}
	results := map[string]interface{}{"foo2": "bar2"}
	event, err := api.CreateEvent("info", "Test event", 123456*time.Millisecond, params, results)
	assert.NoError(t, err)
	assert.NotNil(t, event)
}
