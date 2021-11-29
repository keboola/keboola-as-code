package dialog

import (
	"errors"
	"net/url"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
)

func (p *Dialogs) AskStorageApiHost(options *options.Options) string {
	host := options.GetString(`storage-api-host`)
	if len(host) == 0 {
		host, _ = p.Ask(&prompt.Question{
			Label:       "API host",
			Description: "Please enter Keboola Storage API host, eg. \"connection.keboola.com\".",
			Validator:   StorageApiHostValidator,
		})
	}

	host = strings.TrimRight(host, "/")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	options.Set(`storage-api-host`, host)
	return host
}

func StorageApiHostValidator(val interface{}) error {
	str := val.(string)
	if len(str) == 0 {
		return errors.New("value is required")
	} else if _, err := url.Parse(str); err != nil {
		return errors.New("invalid host")
	}
	return nil
}
