package dialog

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

var ErrMissingStorageAPIToken = errors.New(`missing Storage API token`)

func (p *Dialogs) AskStorageAPIToken(d hostAndTokenDependencies) (string, error) {
	opts := d.Options()
	token := opts.GetString(`storage-api-token`)
	if len(token) == 0 {
		token, _ = p.Ask(&prompt.Question{
			Label:       "API token",
			Description: "Please enter Keboola Storage API token. The value will be hidden.",
			Hidden:      true,
			Validator:   prompt.ValueRequired,
		})
	} else if opts.KeySetBy("storage-api-token") == cliconfig.SetByEnv {
		d.Logger().Infof(`Storage API token set from ENV.`)
	}

	token = strings.TrimSpace(token)
	if len(token) == 0 {
		return "", ErrMissingStorageAPIToken
	}

	opts.Set(`storage-api-token`, token)
	return token, nil
}
