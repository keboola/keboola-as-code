package dialog

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

var ErrMissingStorageAPIToken = errors.New(`missing Storage API token`)

func (p *Dialogs) AskStorageAPIToken() string {
	token, _ := p.Ask(&prompt.Question{
		Label:       "API token",
		Description: "Please enter Keboola Storage API token. The value will be hidden.",
		Hidden:      true,
		Validator:   prompt.ValueRequired,
	})
	return strings.TrimSpace(token)
}
