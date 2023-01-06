package mapper

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func formatReceiverURL(bufferAPIHost string, k key.ReceiverKey, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%d/%s/%s", bufferAPIHost, k.ProjectID, k.ReceiverID, secret)
}
