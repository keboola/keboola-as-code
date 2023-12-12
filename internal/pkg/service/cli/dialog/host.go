package dialog

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

var ErrMissingStorageAPIHost = errors.New("missing Storage API host")

func (p *Dialogs) AskStorageAPIHost() string {
	host, _ := p.Ask(&prompt.Question{
		Label:       "API host",
		Description: "Please enter Keboola Storage API host, eg. \"connection.keboola.com\".",
		Validator:   StorageAPIHostValidator,
	})
	return host
}

func StorageAPIHostValidator(val any) error {
	if str := val.(string); len(str) == 0 {
		return errors.New("value is required")
	} else if _, err := url.Parse(str); err != nil {
		return errors.New("invalid host")
	}
	return nil
}
