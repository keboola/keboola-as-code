package download

import (
	"path/filepath"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

type Options struct {
	File              *keboola.FileDownloadCredentials
	Output            string
	AllowSliced       bool
	Columns           []string
	Header            configmap.Value[bool]
	WithOutDecompress configmap.Value[bool]
}

func (o *Options) ToStdout() bool {
	return o.Output == StdoutOutput
}

// FormattedOutput returns formatted file output for logging purposes.
func (o *Options) FormattedOutput() string {
	switch {
	case o.ToStdout():
		return "stdout"
	case o.AllowSliced && o.File.IsSliced:
		return filepath.Join(o.Output, "<slice>") // nolint:forbidigo
	default:
		return o.Output
	}
}
