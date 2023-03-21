package dialog

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskTableID() string {
	v, _ := p.Ask(&prompt.Question{
		Label:       "Table",
		Description: "Enter the destination table ID",
	})
	return v
}

func (p *Dialogs) AskTable(
	d *options.Options,
	allTables []*keboola.Table,
) (*keboola.Table, error) {
	if d.IsSet(`table-id`) {
		tableID, err := keboola.ParseTableID(d.GetString(`table-id`))
		if err != nil {
			return nil, err
		}
		for _, w := range allTables {
			if w.ID == tableID {
				return w, nil
			}
		}
		return nil, errors.Errorf(`table with ID "%s" not found in the project`, tableID)
	}

	selectOpts := make([]string, 0)
	for _, table := range allTables {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, table.DisplayName, table.ID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   "Table",
		Options: selectOpts,
	}); ok {
		return allTables[index], nil
	}

	return nil, errors.New(`please specify a table`)
}
