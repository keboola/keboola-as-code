package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) SelectBranch(all []*model.Branch, label string, branch configmap.Value[string]) (*model.Branch, error) {
	if branch.IsSet() {
		return search.Branch(all, branch.Value)
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

func (p *Dialogs) SelectBranches(all []*model.Branch, label string, branches configmap.Value[string], allowTargetENV configmap.Value[bool]) (results []*model.Branch, err error) {
	if branches.IsSet() {
		errs := errors.NewMultiError()
		for item := range strings.SplitSeq(branches.Value, `,`) {
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

	if allowTargetENV.Value {
		index, _ := p.SelectIndex(&prompt.SelectIndex{
			Label:       label,
			Description: "Please select one branch.",
			Options:     selectOpts.Keys(),
		})
		results = append(results, all[index])
	} else {
		indexes, _ := p.MultiSelectIndex(&prompt.MultiSelectIndex{
			Label:       label,
			Description: "Please select one or more branches.",
			Options:     selectOpts.Keys(),
			Validator:   prompt.AtLeastOneRequired,
		})
		for _, index := range indexes {
			results = append(results, all[index])
		}
	}

	if len(results) > 0 {
		return results, nil
	}

	return nil, errors.New(`please specify at least one branch`)
}
