package mapper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func TestFormatReceiverURL(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"https://buffer.keboola.local/v1/import/1000/my-receiver/my-secret",
		formatReceiverURL("https://buffer.keboola.local", key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}, "my-secret"),
	)
}
