package dialog

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) selectTemplateInstance(options *options.Options, branch *model.Branch, label string) (*model.TemplateInstance, error) {
	if options.IsSet(`instance`) {
		usage, found, err := branch.Metadata.TemplateInstance(options.GetString(`instance`))
		if err != nil {
			return nil, err
		}
		if found {
			return usage, nil
		}
		return nil, errors.Errorf(`template instance "%s" was not found in branch "%s"`, options.GetString(`instance`), branch.Name)
	}

	all, err := branch.Metadata.TemplatesInstances()
	if err != nil {
		return nil, err
	}

	selectOpts := make([]string, 0)
	for _, u := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s %s (%s)`, u.TemplateID, u.Version, u.InstanceID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   label,
		Options: selectOpts,
	}); ok {
		return &all[index], nil
	}

	return nil, errors.New(`please specify template instance`)
}
