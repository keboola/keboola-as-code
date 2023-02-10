package mapper

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func formatReceiverURL(bufferAPIHost string, k key.ReceiverKey, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%d/%s/%s", bufferAPIHost, k.ProjectID, k.ReceiverID, secret)
}

func formatTaskURL(bufferAPIHost string, k key.TaskKey) string {
	return fmt.Sprintf("https://%s/v1/receivers/%s/tasks/%s/%s", bufferAPIHost, k.ReceiverID, k.Type, k.TaskID)
}
