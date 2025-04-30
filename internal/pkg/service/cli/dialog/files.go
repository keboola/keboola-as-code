package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/upload"
)

func (p *Dialogs) AskFile(allFiles []*keboola.File) (*keboola.File, error) {
	selectOpts := make([]string, 0)
	for _, w := range allFiles {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%d)`, w.Name, w.FileID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   "File",
		Options: selectOpts,
	}); ok {
		return allFiles[index], nil
	}

	return nil, errors.New(`please specify a file`)
}

func (p *Dialogs) AskFileOutput(output configmap.Value[string]) (string, error) {
	if len(output.Value) == 0 {
		output.Value, _ = p.Ask(&prompt.Question{
			Label:       "Output",
			Description: "Enter a path for the file destination or - to write to standard output.",
		})
		if len(output.Value) == 0 {
			return "", errors.Errorf("please specify a file")
		}
	}

	output.Value = strings.TrimSpace(output.Value)
	output = configmap.Value[string]{Value: output.Value, SetBy: configmap.SetByDefault}
	return output.Value, nil
}

type AskUpload struct {
	Input       string
	DefaultName string
	FileName    configmap.Value[string]
	Data        configmap.Value[string]
	FileTag     configmap.Value[string]
}

func (p *Dialogs) AskUploadFile(branchKey keboola.BranchKey, d AskUpload) (upload.Options, error) {
	res := upload.Options{BranchKey: branchKey}

	name, err := p.askFileName(d.DefaultName, d.FileName)
	if err != nil {
		return res, err
	}
	res.Name = name

	if len(d.Input) > 0 {
		res.Input = d.Input
	} else {
		res.Input = p.askFileInput(d.Data)
	}

	res.Tags = p.askFileTags(d.FileTag)

	return res, nil
}

func (p *Dialogs) askFileInput(data configmap.Value[string]) string {
	input := data.Value
	if len(input) == 0 {
		input, _ = p.Ask(&prompt.Question{
			Label:       "File",
			Description: "Enter a path for the file input or - to read from standard input.",
		})
	}

	input = strings.TrimSpace(input)
	return input
}

func (p *Dialogs) askFileName(defaultName string, fileName configmap.Value[string]) (string, error) {
	if fileName.IsSet() {
		return fileName.Value, nil
	} else {
		name, ok := p.Ask(&prompt.Question{
			Label:     "Enter a name for the file",
			Validator: prompt.ValueRequired,
			Default:   defaultName,
		})
		if !ok || len(name) == 0 {
			return "", errors.New("missing file name, please specify it")
		}
		return name, nil
	}
}

func (p *Dialogs) askFileTags(fileTag configmap.Value[string]) []string {
	tagsStr := fileTag.Value
	if !fileTag.IsSet() {
		tagsStr, _ = p.Ask(&prompt.Question{
			Label:       "Tags",
			Description: "Enter a comma-separated list of tags for the file, or enter to skip.",
		})
	}

	tagsStr = strings.TrimSpace(tagsStr)
	tags := strings.Split(tagsStr, ",")
	return tags
}
