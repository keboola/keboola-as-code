package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskPrimaryKey(o *options.Options) []string {
	pkStr := o.GetString(`primary-key`)
	if !o.IsSet(`primary-key`) {
		pkStr, _ = p.Ask(&prompt.Question{
			Label:       "Primary key",
			Description: "Enter a comma-separated list of column names for use as the primary key.",
		})
	}

	return strings.Split(strings.TrimSpace(pkStr), ",")
}

func (p *Dialogs) AskTableID() string {
	v, _ := p.Ask(&prompt.Question{
		Label:       "Table",
		Description: "Enter the table ID",
	})
	return v
}

type askTableConfig struct {
	allowCreateNew bool
}

type AskTableOption interface {
	apply(o *askTableConfig)
}

type withAllowCreateNew struct{}

func (opt *withAllowCreateNew) apply(o *askTableConfig) {
	o.allowCreateNew = true
}

func WithAllowCreateNewTable() AskTableOption {
	return &withAllowCreateNew{}
}

// Returns `nil, nil` if the `WithAllowCreateNewTable` option is set, and the user asked to create a new table.
func (p *Dialogs) AskTable(
	d *options.Options,
	allTables []*keboola.Table,
	opts ...AskTableOption,
) (*keboola.Table, error) {
	config := &askTableConfig{}
	for _, opt := range opts {
		opt.apply(config)
	}

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

	var selectOpts []string
	if config.allowCreateNew {
		selectOpts = []string{"Create a new table"}
	}

	for _, table := range allTables {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, table.DisplayName, table.ID))
	}
	index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:      "Table",
		Options:    selectOpts,
		UseDefault: true,
	})
	if !ok {
		return nil, errors.New(`please specify a table`)
	}

	// Special case:
	if index == 0 {
		return nil, nil
	}

	return allTables[index], errors.New(`please specify a table`)
}
