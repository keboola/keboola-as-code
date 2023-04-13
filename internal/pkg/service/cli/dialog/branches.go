package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) SelectBranch(all []*model.Branch, label string) (*model.Branch, error) {
	if p.options.IsSet(`branch`) {
		return search.Branch(all, p.options.GetString(`branch`))
	}

	selectOpts := make([]string, 0)
	for _, b := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, b.ObjectName(), b.ObjectID()))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   label,
		Options: selectOpts,
	}); ok {
		return all[index], nil
	}

	return nil, errors.New(`please specify branch`)
}

func (p *Dialogs) SelectBranches(all []*model.Branch, label string) (results []*model.Branch, err error) {
	if p.options.IsSet(`branches`) {
		errs := errors.NewMultiError()
		for _, item := range strings.Split(p.options.GetString(`branches`), `,`) {
			item = strings.TrimSpace(item)
			if len(item) == 0 {
				continue
			}

			if b, err := search.Branch(all, item); err == nil {
				results = append(results, b)
			} else {
				errs.Append(err)
				continue
			}
		}
		if len(results) > 0 {
			return results, errs.ErrorOrNil()
		}
		return nil, errors.New(`please specify at least one branch`)
	}

	selectOpts := orderedmap.New()
	for _, branch := range all {
		msg := fmt.Sprintf(`%s (%d)`, branch.Name, branch.ID)
		selectOpts.Set(msg, branch.ID)
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

	return nil, errors.New(`please specify at least one branch`)
}
