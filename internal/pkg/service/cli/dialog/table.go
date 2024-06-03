package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskPrimaryKey(primaryKey configmap.Value[[]string]) []string {
	pkStr := primaryKey.Value[0]
	if !primaryKey.IsSet() {
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

// AskTable returns `nil, nil` if the `WithAllowCreateNewTable` option is set, and the user asked to create a new table.
func (p *Dialogs) AskTable(allTables []*keboola.Table, id configmap.Value[string], opts ...AskTableOption) (*keboola.Table, error) {
	config := &askTableConfig{}
	for _, opt := range opts {
		opt.apply(config)
	}

	if id.IsSet() {
		tableID, err := keboola.ParseTableID(id.Value)
		if err != nil {
			return nil, err
		}
		for _, w := range allTables {
			if w.TableID == tableID {
				return w, nil
			}
		}
		return nil, errors.Errorf(`table with ID "%s" not found in the project`, tableID)
	}

	selectOpts := make([]string, 0, len(allTables)+1)
	if config.allowCreateNew {
		selectOpts = []string{"Create a new table"}
	}

	for _, table := range allTables {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, table.DisplayName, table.TableID))
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
	if config.allowCreateNew && index == 0 {
		return nil, nil
	}

	return allTables[index], nil
}
