package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (p *Dialogs) SelectBranch(options *options.Options, all []*model.Branch, label string) (*model.Branch, error) {
	if options.IsSet(`branch`) {
		return search.Branch(all, options.GetString(`branch`))
	}

	selectOpts := make([]string, 0)
	for _, b := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, b.ObjectName(), b.ObjectId()))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   label,
		Options: selectOpts,
	}); ok {
		return all[index], nil
	}

	return nil, fmt.Errorf(`please specify branch`)
}

func (p *Dialogs) SelectBranches(options *options.Options, all []*model.Branch, label string) (results []*model.Branch, err error) {
	if options.IsSet(`branches`) {
		errors := utils.NewMultiError()
		for _, item := range strings.Split(options.GetString(`branches`), `,`) {
			item = strings.TrimSpace(item)
			if len(item) == 0 {
				continue
			}

			if b, err := search.Branch(all, item); err == nil {
				results = append(results, b)
			} else {
				errors.Append(err)
				continue
			}
		}
		if len(results) > 0 {
			return results, errors.ErrorOrNil()
		}
		return nil, fmt.Errorf(`please specify at least one branch`)
	}

	selectOpts := orderedmap.New()
	for _, branch := range all {
		msg := fmt.Sprintf(`%s (%d)`, branch.Name, branch.Id)
		selectOpts.Set(msg, branch.Id)
	}
	indexes, _ := p.MultiSelectIndex(&prompt.MultiSelectIndex{
		Label:       label,
		Description: "Please select one or more branches.",
		Options:     selectOpts.Keys(),
		Validator:   prompt.AtLeastOneRequired,
	})
	for _, index := range indexes {
		results = append(results, all[index])
	}
	if len(results) > 0 {
		return results, nil
	}

	return nil, fmt.Errorf(`please specify at least one branch`)
}
