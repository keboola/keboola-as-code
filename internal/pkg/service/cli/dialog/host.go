package dialog

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

var ErrMissingStorageApiHost = errors.New("missing Storage API host")

func (p *Dialogs) AskStorageApiHost(d hostAndTokenDependencies) (string, error) {
	opts := d.Options()
	host := opts.GetString("storage-api-host")
	if len(host) == 0 {
		host, _ = p.Ask(&prompt.Question{
			Label:       "API host",
			Description: "Please enter Keboola Storage API host, eg. \"connection.keboola.com\".",
			Validator:   StorageApiHostValidator,
		})
	} else if opts.KeySetBy("storage-api-host") == options.SetByEnv {
		d.Logger().Infof(`Storage API host "%s" set from ENV.`, host)
	}

	host = strhelper.NormalizeHost(host)
	if len(host) == 0 {
		return "", ErrMissingStorageApiHost
	}

	opts.Set(`storage-api-host`, host)
	return host, nil
}

func StorageApiHostValidator(val interface{}) error {
	if str := val.(string); len(str) == 0 {
		return errors.New("value is required")
	} else if _, err := url.Parse(str); err != nil {
		return errors.New("invalid host")
	}
	return nil
}
