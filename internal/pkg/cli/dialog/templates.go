package dialog

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (p *Dialogs) SelectTemplateInstance(options *options.Options, branch *model.Branch, label string) (*model.TemplateUsageRecord, error) {
	if options.IsSet(`instance`) {
		usage, found, err := branch.Metadata.TemplateUsage(options.GetString(`instance`))
		if err != nil {
			return nil, err
		}
		if found {
			return usage, nil
		}
		return nil, fmt.Errorf(`template instance "%s" was not found in branch "%s"`, options.GetString(`instance`), branch.Name)
	}

	all, err := branch.Metadata.TemplatesUsages()
	if err != nil {
		return nil, err
	}

	selectOpts := make([]string, 0)
	for _, u := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s %s (%s)`, u.TemplateId, u.Version, u.InstanceId))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   label,
		Options: selectOpts,
	}); ok {
		return &all[index], nil
	}

	return nil, fmt.Errorf(`please specify template instance`)
}
