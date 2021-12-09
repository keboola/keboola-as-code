package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
)

var ErrMissingStorageApiToken = fmt.Errorf(`missing Storage API token`)

func (p *Dialogs) AskStorageApiToken(options *options.Options) (string, error) {
	token := options.GetString(`storage-api-token`)
	if len(token) == 0 {
		token, _ = p.Ask(&prompt.Question{
			Label:       "API token",
			Description: "Please enter Keboola Storage API token. The value will be hidden.",
			Hidden:      true,
			Validator:   prompt.ValueRequired,
		})
	}

	token = strings.TrimSpace(token)
	if len(token) == 0 {
		return "", ErrMissingStorageApiToken
	}

	options.Set(`storage-api-token`, token)
	return token, nil
}
