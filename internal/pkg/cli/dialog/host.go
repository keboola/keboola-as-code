package dialog

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

var ErrMissingStorageApiHost = fmt.Errorf(`missing Storage API host`)

func (p *Dialogs) AskStorageApiHost(options *options.Options) (string, error) {
	host := options.GetString(`storage-api-host`)
	if len(host) == 0 {
		host, _ = p.Ask(&prompt.Question{
			Label:       "API host",
			Description: "Please enter Keboola Storage API host, eg. \"connection.keboola.com\".",
			Validator:   StorageApiHostValidator,
		})
	}

	host = strhelper.NormalizeHost(host)
	if len(host) == 0 {
		return "", ErrMissingStorageApiHost
	}

	options.Set(`storage-api-host`, host)
	return host, nil
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
