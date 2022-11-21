package dialog

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

var ErrMissingStorageApiToken = errors.New(`missing Storage API token`)

func (p *Dialogs) AskStorageApiToken(d hostAndTokenDependencies) (string, error) {
	opts := d.Options()
	token := opts.GetString(`storage-api-token`)
	if len(token) == 0 {
		token, _ = p.Ask(&prompt.Question{
			Label:       "API token",
			Description: "Please enter Keboola Storage API token. The value will be hidden.",
			Hidden:      true,
			Validator:   prompt.ValueRequired,
		})
	} else if opts.KeySetBy("storage-api-token") == options.SetByEnv {
		d.Logger().Infof(`Storage API token set from ENV.`)
	}

	token = strings.TrimSpace(token)
	if len(token) == 0 {
		return "", ErrMissingStorageApiToken
	}

	opts.Set(`storage-api-token`, token)
	return token, nil
}
