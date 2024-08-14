package keboolasink

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type FileUploadCredentials struct {
	key.SinkKey
	keboola.TableKey
	keboola.FileUploadCredentials
}
