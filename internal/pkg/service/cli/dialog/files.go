package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskFile(allFiles []*keboola.File) (*keboola.File, error) {
	selectOpts := make([]string, 0)
	for _, w := range allFiles {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%d)`, w.Name, w.ID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   "File",
		Options: selectOpts,
	}); ok {
		return allFiles[index], nil
	}

	return nil, errors.New(`please specify a file`)
}

func (p *Dialogs) AskFileOutput(opts *options.Options) (string, error) {
	output := opts.GetString(`output`)
	if len(output) == 0 {
		output, _ = p.Ask(&prompt.Question{
			Label:       "Output",
			Description: "Enter a path for the file destination or - to write to standard output.",
		})
	}

	output = strings.TrimSpace(output)
	opts.Set(`output`, output)
	return output, nil
}
