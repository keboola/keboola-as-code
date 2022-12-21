package mapper

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func formatReceiverURL(bufferAPIHost string, k key.ReceiverKey, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%s/%s/%s", bufferAPIHost, k.ProjectID.String(), k.ReceiverID, secret)
}
