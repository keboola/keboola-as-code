package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
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

func (p *Dialogs) AskUploadFile(opts *options.Options, input string, defaultName string) (upload.Options, error) {
	res := upload.Options{}

	name, err := p.askFileName(opts, defaultName)
	if err != nil {
		return res, err
	}
	res.Name = name

	if len(input) > 0 {
		res.Input = input
	} else {
		res.Input = p.askFileInput(opts)
	}

	res.Tags = p.askFileTags(opts)

	return res, nil
}

func (p *Dialogs) askFileInput(opts *options.Options) string {
	input := opts.GetString(`data`)
	if len(input) == 0 {
		input, _ = p.Ask(&prompt.Question{
			Label:       "File",
			Description: "Enter a path for the file input or - to read from standard input.",
		})
	}

	input = strings.TrimSpace(input)
	opts.Set(`input`, input)
	return input
}

func (p *Dialogs) askFileName(opts *options.Options, defaultName string) (string, error) {
	if opts.IsSet("name") {
		return opts.GetString("name"), nil
	} else {
		name, ok := p.Ask(&prompt.Question{
			Label:     "Enter a name for the file",
			Validator: prompt.ValueRequired,
			Default:   defaultName,
		})
		if !ok || len(name) == 0 {
			return "", errors.New("missing name, please specify it")
		}
		return name, nil
	}
}

func (p *Dialogs) askFileTags(opts *options.Options) []string {
	tagsStr := opts.GetString(`tags`)
	if !opts.IsSet(`tags`) {
		tagsStr, _ = p.Ask(&prompt.Question{
			Label:       "Tags",
			Description: "Enter a comma-separated list of tags, or enter to skip.",
		})
	}

	tagsStr = strings.TrimSpace(tagsStr)
	tags := strings.Split(tagsStr, ",")
	opts.Set(`input`, tags)
	return tags
}
